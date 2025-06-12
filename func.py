import os
import shutil
import pandas as pd
from work.dbql_extraction import main, show_table_extract
from work.dbql_extraction import extraction_dbql
from work.dbql_extraction.lineage_branch.working_pipeline import fill_missing_col, config, merge_csvs

# Global db mapping file path
GLOBAL_DB_MAPPING_FILE = "global_db_table_mapping.csv"

def setup_level_directory(level_num, prev_output_csv, prev_input_csv):
    paths = config.get_level_paths(level_num)
    os.makedirs(paths["level_dir"], exist_ok=True)
    os.makedirs(paths["sql_text_files_dir"], exist_ok=True)
    os.makedirs(paths["parsed_dir"], exist_ok=True)
    os.makedirs(paths["merged_dir"], exist_ok=True)
    os.makedirs(paths["processed_outputs"], exist_ok=True)
    
    if level_num == 1:
        # Copy the initial param.csv file
        shutil.copy("../param.csv", paths["input_csv"])
    else:
        # Read previous level's output CSV
        prev_output_df = pd.read_csv(prev_output_csv)
        
        # Read previous level's input param.csv to get already processed db/table combinations
        prev_input_df = pd.read_csv(prev_input_csv)
        
        # Get unique target db/table combinations from previous output
        if not prev_output_df.empty and "target_db" in prev_output_df.columns and "target_table" in prev_output_df.columns:
            target_combinations = prev_output_df[["target_db", "target_table"]].drop_duplicates()
            
            # Filter out combinations that were already in the previous level's input
            if not prev_input_df.empty and "db" in prev_input_df.columns and "table" in prev_input_df.columns:
                # Create a set of previous input combinations for efficient lookup
                prev_input_combinations = set(
                    zip(prev_input_df["db"], prev_input_df["table"])
                )
                
                # Filter target combinations to exclude those already processed
                new_combinations = []
                for _, row in target_combinations.iterrows():
                    if (row["target_db"], row["target_table"]) not in prev_input_combinations:
                        new_combinations.append({
                            "db": row["target_db"],
                            "table": row["target_table"]
                        })
                
                if new_combinations:
                    new_input_df = pd.DataFrame(new_combinations)
                else:
                    # No new combinations found, create empty DataFrame with correct columns
                    new_input_df = pd.DataFrame(columns=["db", "table"])
            else:
                # If no previous input, use all target combinations
                new_input_df = target_combinations.rename(columns={"target_db": "db", "target_table": "table"})
        else:
            # No target columns found, create empty DataFrame
            new_input_df = pd.DataFrame(columns=["db", "table"])
        
        # Save the new param.csv for this level
        new_input_df.to_csv(paths["input_csv"], index=False)
    
    return paths

def run_pipeline(start_from_level=None, start_from_step=None):
    paths_per_level = {}
    
    # Determine starting level
    start_level = start_from_level if start_from_level else config.start_level
    
    for level_num in range(start_level, config.max_levels + 1):
        print(f"\n Starting Round {level_num}")
        
        # Set up directories
        if level_num == 1:
            paths = setup_level_directory(level_num, prev_output_csv=None, prev_input_csv=None)
        else:
            prev_level = level_num - 1
            if prev_level in paths_per_level:
                prev_output_csv = paths_per_level[prev_level]["final_output_csv"]
                prev_input_csv = paths_per_level[prev_level]["input_csv"]
            else:
                # If we are starting from level > 1, we need to reconstruct paths
                prev_paths = config.get_level_paths(prev_level)
                prev_output_csv = prev_paths["final_output_csv"] if "final_output_csv" in prev_paths else os.path.join(prev_paths["processed_outputs"], "final_output.csv")
                prev_input_csv = prev_paths["input_csv"]
            paths = setup_level_directory(level_num, prev_output_csv, prev_input_csv)
        
        # Starting step
        current_step = 1
        start_step = start_from_step if start_from_step and level_num == start_from_level else 1
        
        # Update paths with output files from previous steps if we are resuming
        if start_step > 1:
            update_paths_for_resumption(paths, level_num, start_step)
        
        # Step 1: Extract queries
        if current_step >= start_step:
            try:
                print(f"Executing Step {current_step}: Extracting queries for level {level_num}...")
                data_list = extraction_dbql.read_target_file(paths["input_csv"])
                extracted_csv = extraction_dbql.extract_queries(data_list, paths["level_dir"])
                extraction_dbql.clean_chunk_sql_queries(extracted_csv, paths["sql_text_files_dir"])
                paths["extracted_csv"] = extracted_csv
                save_checkpoint(level_num, current_step)
                print(f"Step {current_step} completed successfully")
            except Exception as e:
                handle_error(e, level_num, current_step)
                return
        current_step += 1
        
        # Step 2: Process files
        if current_step >= start_step:
            try:
                print(f"Executing Step {current_step}: Processing files for level {level_num}...")
                parsed_output_dir = main.process_all_files(paths["sql_text_files_dir"], paths["parsed_dir"])
                paths["parsed_output_dir"] = parsed_output_dir
                save_checkpoint(level_num, current_step)
                print(f"Step {current_step} completed successfully")
            except Exception as e:
                handle_error(e, level_num, current_step)
                return
        current_step += 1
        
        # Step 3: Merge CSV files
        if current_step >= start_step:
            try:
                print(f"Executing Step {current_step}: Merging files for level {level_num}...")
                merged_output = merge_csvs.merge_files(paths.get("parsed_output_dir", paths["parsed_dir"]), paths["merged_dir"])
                paths["merged_output"] = merged_output
                save_checkpoint(level_num, current_step)
                print(f"Step {current_step} completed successfully")
            except Exception as e:
                handle_error(e, level_num, current_step)
                return
        current_step += 1
        
        # Step 4: Map tables to database and extract columns (Modified)
        if current_step >= start_step:
            try:
                print(f"Executing Step {current_step}: Mapping tables and extracting columns for level {level_num}...")
                
                if level_num == 1:
                    # Level 1: Always create/initialize the global DB mapping file
                    df = show_table_extract.map_table_to_db(paths["merged_output"])
                    show_table_ref_path = show_table_extract.extract_columns(df, paths["level_dir"])
                    
                    # Create/update global DB mapping file
                    if "db" in df.columns and "table" in df.columns:
                        db_mapping_df = df[["db", "table"]].drop_duplicates()
                        db_mapping_df.to_csv(GLOBAL_DB_MAPPING_FILE, index=False)
                        print(f"Created global DB mapping file: {GLOBAL_DB_MAPPING_FILE}")
                else:
                    # Subsequent levels: Check global file and append missing mappings
                    merged_df = pd.read_csv(paths["merged_output"])
                    
                    # Read existing global DB mapping
                    if os.path.exists(GLOBAL_DB_MAPPING_FILE):
                        global_mapping_df = pd.read_csv(GLOBAL_DB_MAPPING_FILE)
                        existing_dbs = set(global_mapping_df["db"].unique()) if "db" in global_mapping_df.columns else set()
                    else:
                        global_mapping_df = pd.DataFrame(columns=["db", "table"])
                        existing_dbs = set()
                    
                    # Get unique databases from current level's data
                    current_dbs = set()
                    if "source_db" in merged_df.columns:
                        current_dbs.update(merged_df["source_db"].dropna().unique())
                    if "target_db" in merged_df.columns:
                        current_dbs.update(merged_df["target_db"].dropna().unique())
                    
                    # Find missing databases
                    missing_dbs = current_dbs - existing_dbs
                    
                    if missing_dbs:
                        print(f"Found {len(missing_dbs)} new databases to map: {missing_dbs}")
                        # Extract mappings for missing databases and append to global file
                        df = show_table_extract.map_table_to_db(paths["merged_output"])
                        
                        # Append new mappings to global file
                        if "db" in df.columns and "table" in df.columns:
                            new_mappings = df[["db", "table"]].drop_duplicates()
                            # Filter to only new mappings
                            if not global_mapping_df.empty:
                                existing_combinations = set(zip(global_mapping_df["db"], global_mapping_df["table"]))
                                new_mappings = new_mappings[~new_mappings.apply(lambda x: (x["db"], x["table"]) in existing_combinations, axis=1)]
                            
                            if not new_mappings.empty:
                                # Append to global file
                                updated_global_df = pd.concat([global_mapping_df, new_mappings], ignore_index=True)
                                updated_global_df.to_csv(GLOBAL_DB_MAPPING_FILE, index=False)
                                print(f"Appended {len(new_mappings)} new mappings to global DB mapping file")
                    else:
                        print("All required databases already exist in global mapping file")
                        df = merged_df  # Use merged output as df for next step
                    
                    # Extract columns using the updated global mapping
                    show_table_ref_path = show_table_extract.extract_columns(df, paths["level_dir"])
                
                paths["show_table_ref_path"] = show_table_ref_path
                save_checkpoint(level_num, current_step)
                print(f"Step {current_step} completed successfully")
            except Exception as e:
                handle_error(e, level_num, current_step)
                return
        current_step += 1
        
        # Step 5: Fill missing columns (formerly step 7)
        if current_step >= start_step:
            try:
                print(f"Executing Step {current_step}: Filling missing columns for level {level_num}...")
                # Use merged_output instead of prev_output_csv since we removed steps 4 and 5
                input_csv_for_fill = paths.get("merged_output", paths["merged_output"])
                final_output_csv = fill_missing_col.replace_columns_with_reference(
                    input_csv_for_fill, 
                    paths["show_table_ref_path"], 
                    paths["processed_outputs"]
                )
                paths["final_output_csv"] = final_output_csv
                save_checkpoint(level_num, current_step)
                print(f"Step {current_step} completed successfully")
            except Exception as e:
                handle_error(e, level_num, current_step)
                return
        current_step += 1
        
        paths_per_level[level_num] = paths
        print(f"\nLevel {level_num} completed")
    
    print(f"\nPipeline execution completed")

def update_paths_for_resumption(paths, level_num, start_step):
    level_dir = paths["level_dir"]
    merged_dir = paths["merged_dir"]
    
    # Look for common output files based on the step we are resuming from
    if start_step > 1:  # after extraction
        paths["extracted_csv"] = os.path.join(level_dir, "extracted_queries.csv")
    if start_step > 2:  # after processing
        paths["parsed_output_dir"] = paths["parsed_dir"]
    if start_step > 3:  # after merging
        paths["merged_output"] = os.path.join(merged_dir, "combined_output.csv")
    if start_step > 4:  # after mapping tables
        paths["show_table_ref_path"] = os.path.join(level_dir, "show_table.csv")

def save_checkpoint(level, step):
    checkpoint_file = config.get_level_paths(1).get("checkpoint_file", "pipeline_checkpoint.txt")
    with open(checkpoint_file, 'w') as f:
        f.write(f"level={level}\nstep={step}")

def handle_error(error, level, step):
    print(f"Error: {str(error)}")
    print(f"Pipeline stopped. To resume, run with: resume_from_level={level}, resume_from_step={step}")

# Check for checkpoint file and resume if exists
def main_1():
    checkpoint_file = config.get_level_paths(1).get("checkpoint_file", "pipeline_checkpoint.txt")
    if os.path.exists(checkpoint_file):
        level = None
        step = None
        with open(checkpoint_file, "r") as f:
            for line in f:
                if line.startswith("level="):
                    level = int(line.split("=")[1].strip())
                elif line.startswith("step="):  # Fixed typo: was "level=" should be "step="
                    step = int(line.split("=")[1].strip())
        if level and step:
            print(f"Resuming from Level {level}, Step {step}")
            run_pipeline(start_from_level=level, start_from_step=step)
            return
    run_pipeline()

if __name__ == "__main__":
    run_pipeline(start_from_level=5, start_from_step=5)
    # Try to resume, otherwise start from beginning
    # if not resume_pipeline():
    #     run_pipeline(start_from_level=2, start_from_step=1)
