import pandas as pd
import numpy as np
import sys
from pathlib import Path

def normalize_column_names(df):
    """
    Normalize column names by converting to lowercase and removing extra spaces
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def create_comparison_key(df):
    """
    Create a unique key for each row based on all mapping columns
    """
    # Handle different possible column name variations
    column_mappings = {
        'target_db': ['target_db', 'targetdb', 'target_database'],
        'target_table': ['target_table', 'targettable'],
        'target_column': ['target_column', 'targetcolumn'],
        'source_db': ['source_db', 'sourcedb', 'source_database'],
        'source_table': ['source_table', 'sourcetable'],
        'source_column': ['source_column', 'sourcecolumn']
    }
    
    # Find actual column names in the dataframe
    actual_columns = {}
    for standard_name, variations in column_mappings.items():
        for variation in variations:
            if variation in df.columns:
                actual_columns[standard_name] = variation
                break
        if standard_name not in actual_columns:
            raise ValueError(f"Could not find column for {standard_name}. Available columns: {list(df.columns)}")
    
    # Create comparison key by concatenating all relevant columns
    df['comparison_key'] = (
        df[actual_columns['target_db']].astype(str).str.strip() + '|' +
        df[actual_columns['target_table']].astype(str).str.strip() + '|' +
        df[actual_columns['target_column']].astype(str).str.strip() + '|' +
        df[actual_columns['source_db']].astype(str).str.strip() + '|' +
        df[actual_columns['source_table']].astype(str).str.strip() + '|' +
        df[actual_columns['source_column']].astype(str).str.strip()
    ).str.lower()
    
    return df, actual_columns

def compare_csv_files(file1_path, file2_path, output_path=None):
    """
    Compare two CSV files and generate an error report
    
    Parameters:
    file1_path (str): Path to the first CSV file
    file2_path (str): Path to the second CSV file
    output_path (str): Path for the output error file (optional)
    
    Returns:
    pandas.DataFrame: DataFrame containing the comparison results
    """
    
    try:
        # Read CSV files
        print(f"Reading {file1_path}...")
        df1 = pd.read_csv(file1_path)
        print(f"File 1 loaded: {len(df1)} rows")
        
        print(f"Reading {file2_path}...")
        df2 = pd.read_csv(file2_path)
        print(f"File 2 loaded: {len(df2)} rows")
        
        # Normalize column names
        df1 = normalize_column_names(df1)
        df2 = normalize_column_names(df2)
        
        print("Column names in File 1:", list(df1.columns))
        print("Column names in File 2:", list(df2.columns))
        
        # Create comparison keys
        df1, columns1 = create_comparison_key(df1)
        df2, columns2 = create_comparison_key(df2)
        
        # Remove duplicates based on comparison key (keep first occurrence)
        df1_unique = df1.drop_duplicates(subset=['comparison_key'], keep='first')
        df2_unique = df2.drop_duplicates(subset=['comparison_key'], keep='first')
        
        print(f"Unique records in File 1: {len(df1_unique)}")
        print(f"Unique records in File 2: {len(df2_unique)}")
        
        # Find differences
        keys1 = set(df1_unique['comparison_key'])
        keys2 = set(df2_unique['comparison_key'])
        
        # Records in file1 but not in file2 (error code 12)
        only_in_file1 = keys1 - keys2
        # Records in file2 but not in file1 (error code 21)
        only_in_file2 = keys2 - keys1
        
        print(f"Records only in File 1: {len(only_in_file1)}")
        print(f"Records only in File 2: {len(only_in_file2)}")
        
        # Create error report
        error_records = []
        
        # Add records from file1 not in file2 (code 12)
        for key in only_in_file1:
            row = df1_unique[df1_unique['comparison_key'] == key].iloc[0]
            error_records.append({
                'error_code': '12',
                'target_db': row[columns1['target_db']],
                'target_table': row[columns1['target_table']],
                'target_column': row[columns1['target_column']],
                'source_db': row[columns1['source_db']],
                'source_table': row[columns1['source_table']],
                'source_column': row[columns1['source_column']],
                'description': 'Present in File 1 but not in File 2'
            })
        
        # Add records from file2 not in file1 (code 21)
        for key in only_in_file2:
            row = df2_unique[df2_unique['comparison_key'] == key].iloc[0]
            error_records.append({
                'error_code': '21',
                'target_db': row[columns2['target_db']],
                'target_table': row[columns2['target_table']],
                'target_column': row[columns2['target_column']],
                'source_db': row[columns2['source_db']],
                'source_table': row[columns2['source_table']],
                'source_column': row[columns2['source_column']],
                'description': 'Present in File 2 but not in File 1'
            })
        
        # Create error DataFrame
        if error_records:
            error_df = pd.DataFrame(error_records)
            
            # Sort by error code and then by target_db, target_table
            error_df = error_df.sort_values(['error_code', 'target_db', 'target_table', 'target_column'])
            
            print(f"\nTotal differences found: {len(error_df)}")
            print(f"Error code 12 (File 1 only): {len(error_df[error_df['error_code'] == '12'])}")
            print(f"Error code 21 (File 2 only): {len(error_df[error_df['error_code'] == '21'])}")
            
            # Save to file if output path provided
            if output_path:
                error_df.to_csv(output_path, index=False)
                print(f"Error report saved to: {output_path}")
            
            return error_df
        else:
            print("No differences found between the files!")
            empty_df = pd.DataFrame(columns=['error_code', 'target_db', 'target_table', 'target_column', 
                                           'source_db', 'source_table', 'source_column', 'description'])
            if output_path:
                empty_df.to_csv(output_path, index=False)
                print(f"Empty error report saved to: {output_path}")
            return empty_df
            
    except Exception as e:
        print(f"Error during comparison: {str(e)}")
        raise

def main():
    """
    Main function to run the comparison from command line
    """
    if len(sys.argv) < 3:
        print("Usage: python csv_comparison.py <file1.csv> <file2.csv> [output_file.csv]")
        print("Example: python csv_comparison.py mapping1.csv mapping2.csv errors.csv")
        return
    
    file1_path = sys.argv[1]
    file2_path = sys.argv[2]
    output_path = sys.argv[3] if len(sys.argv) > 3 else "comparison_errors.csv"
    
    # Check if files exist
    if not Path(file1_path).exists():
        print(f"Error: File {file1_path} does not exist")
        return
    
    if not Path(file2_path).exists():
        print(f"Error: File {file2_path} does not exist")
        return
    
    try:
        result_df = compare_csv_files(file1_path, file2_path, output_path)
        
        # Display sample of results
        if len(result_df) > 0:
            print("\nSample of differences found:")
            print(result_df.head(10).to_string(index=False))
            
            if len(result_df) > 10:
                print(f"\n... and {len(result_df) - 10} more rows")
        
    except Exception as e:
        print(f"Failed to compare files: {str(e)}")

# Example usage function
def example_usage():
    """
    Example of how to use the comparison function in your code
    """
    # Example 1: Basic comparison
    error_df = compare_csv_files('file1.csv', 'file2.csv', 'errors.csv')
    
    # Example 2: Just get the DataFrame without saving
    error_df = compare_csv_files('file1.csv', 'file2.csv')
    
    # Example 3: Process the results
    if len(error_df) > 0:
        # Filter only records missing from file2
        missing_from_file2 = error_df[error_df['error_code'] == '12']
        print(f"Records missing from file 2: {len(missing_from_file2)}")
        
        # Filter only records missing from file1
        missing_from_file1 = error_df[error_df['error_code'] == '21']
        print(f"Records missing from file 1: {len(missing_from_file1)}")
        
        # Group by target database
        by_target_db = error_df.groupby('target_db')['error_code'].count()
        print("Errors by target database:")
        print(by_target_db)

if __name__ == "__main__":
    main()
