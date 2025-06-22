import pandas as pd
import pyodbc
import os
from tqdm import tqdm
from dotenv import load_dotenv
 
 
def get_database_name(table_name):
    # Connect to teradata
    load_dotenv()
 
    # Load environment variables and connect to Teradata
    connStr = os.getenv("connStr")
    connection = pyodbc.connect(connStr)
 
    try:
        query = f"""
        select distinct DatabaseName,CAST(max(LastAccessTimeStamp) as DATE) (FORMAT 'dd/mm/yyyy') as Latest 
from dbc.columnsV 
where tablename='{table_name}'
and  LastAccessTimeStamp IS NOT NULL group by DatabaseName, LastAccessTimeStamp ;
"""
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
 
        if result:
            return result[0]
        else:
            return None
 
    except Exception as e:
        print(f"Error querying database for table {table_name}: {str(e)}")
        return None
 
 
def map_table_to_db(input_file, output_file, mapping_output):
 
    # Read the csv file
    df = pd.read_csv(input_file)
    print(f"length of original df: {len(df)}")
 
    df = df.drop_duplicates(['source_col', 'source_table', 'target_table', 'target_col', 'source_db', 'target_db', 'Derivation_logic'])
    print(f"length of deduplicated df: {len(df)}")
 
    # Create separate dictionaries for source and target table mappings
    source_table_db_mapping = {}
    target_table_db_mapping = {}
 
    # check if source db and target db columns exist in the input file
    has_source_db = 'source_db' in df.columns
    has_target_db = 'target_db' in df.columns
 
    # Pre-fill mapping dictionaries with existing values from DataFrame (Option 1)
    if has_source_db:
        # Get existing source table to db mappings from the DataFrame
        source_existing_mappings = df[['source_table', 'source_db']].dropna().drop_duplicates()
        for _, row in source_existing_mappings.iterrows():
            if not pd.isna(row['source_table']) and not pd.isna(row['source_db']):
                source_table_db_mapping[row['source_table']] = row['source_db']
    
    if has_target_db:
        # Get existing target table to db mappings from the DataFrame
        target_existing_mappings = df[['target_table', 'target_db']].dropna().drop_duplicates()
        for _, row in target_existing_mappings.iterrows():
            if not pd.isna(row['target_table']) and not pd.isna(row['target_db']):
                target_table_db_mapping[row['target_table']] = row['target_db']
 
    # check if mapping_output file exists and load existing mappings
    if mapping_output and os.path.isfile(mapping_output):
        try:
            existing_mapping_df = pd.read_csv(mapping_output)
            # load existing table to db mappings
            for _, row in existing_mapping_df.iterrows():
                if not pd.isna(row['table_name']) and not pd.isna(row['database_name']):
                    # Add to both mappings if not already present (DataFrame values take precedence)
                    if row['table_name'] not in source_table_db_mapping:
                        source_table_db_mapping[row['table_name']] = row['database_name']
                    if row['table_name'] not in target_table_db_mapping:
                        target_table_db_mapping[row['table_name']] = row['database_name']
            print(f"Loaded existing mappings from {mapping_output}")
        except Exception as e:
            print(f"Warning: Could not read existing mapping file {mapping_output}: {e}")
 
    # identify tables that need db lookup (don't have mappings yet)
    source_tables_needing_lookup = set()
    target_tables_needing_lookup = set()
 
    # check source tables
    source_tables = df['source_table'].dropna().unique()
    for table in source_tables:
        if table and not pd.isna(table) and table not in source_table_db_mapping:
            source_tables_needing_lookup.add(table)
 
    # check target tables
    target_tables = df['target_table'].dropna().unique()
    for table in target_tables:
        if table and not pd.isna(table) and table not in target_table_db_mapping:
            target_tables_needing_lookup.add(table)
 
    # Combine all tables that need lookup for the database query
    all_tables_needing_lookup = source_tables_needing_lookup.union(target_tables_needing_lookup)
 
    # process only tables that need database lookup
    if all_tables_needing_lookup:
        print(f"Processing {len(all_tables_needing_lookup)} tables that need database lookup")
        
        # Prepare mapping file for appending (create header if file doesn't exist)
        if mapping_output:
            file_exists = os.path.isfile(mapping_output)
            if not file_exists:
                with open(mapping_output, 'w', newline='') as f:
                    f.write("table_name,database_name\n")
        
        for table in tqdm(all_tables_needing_lookup):
            db_name = get_database_name(table)
            if db_name:
                # Add to both mappings if the table was needed for that context
                if table in source_tables_needing_lookup:
                    source_table_db_mapping[table] = db_name
                if table in target_tables_needing_lookup:
                    target_table_db_mapping[table] = db_name
                
                # Immediately append to mapping file
                if mapping_output:
                    with open(mapping_output, 'a', newline='') as f:
                        f.write(f"{table},{db_name}\n")
                        
            else:
                print(f"Warning: Could not find database for table: {table}")
                # Add "Unknown" to both mappings if the table was needed for that context
                if table in source_tables_needing_lookup:
                    source_table_db_mapping[table] = "Unknown"
                if table in target_tables_needing_lookup:
                    target_table_db_mapping[table] = "Unknown"
                
                # Append "Unknown" to mapping file as well
                if mapping_output:
                    with open(mapping_output, 'a', newline='') as f:
                        f.write(f"{table},Unknown\n")
        
        print(f"Mappings directly appended to {mapping_output}")
    else:
        print("All tables already have database mappings. No lookup needed.")
 
    # create source db column if it doesnt exist
    if not has_source_db:
        df['source_db'] = None
 
    # create target db column if it doesnt exist
    if not has_target_db:
        df['target_db'] = None
 
    # update the dataframe using fast vectorized map operations with proper NaN handling
    # For source_db: only map non-null source_table values, preserve existing source_db for NaN tables
    source_mask = df['source_table'].notna()
    df.loc[source_mask, 'source_db'] = df.loc[source_mask, 'source_table'].map(source_table_db_mapping).fillna(df.loc[source_mask, 'source_db'])
    
    # For target_db: only map non-null target_table values, preserve existing target_db for NaN tables  
    target_mask = df['target_table'].notna()
    df.loc[target_mask, 'target_db'] = df.loc[target_mask, 'target_table'].map(target_table_db_mapping).fillna(df.loc[target_mask, 'target_db'])
 
    # Overwrite the input csv file
    df.to_csv(output_file, index=False)
 
    return output_file
 
 
if __name__ == "__main__":
    input_file = r"C:\Project_Work\Lineage\work\dbql_extraction\parser_output_level_0\full_replace_view_parsed_file.csv"
    output_file = r"C:\Project_Work\Lineage\work\dbql_extraction\parser_output_level_0\full_replace_view_parsed_file_with_db.csv"
    mapping_output = r"C:\Project_Work\Lineage\work\dbql_extraction\parser_output_level_0\db_mapped_output\db_map_data.csv"
 
    x = map_table_to_db(input_file, output_file, mapping_output)
