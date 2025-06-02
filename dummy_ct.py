import pandas as pd
from process_statements import process_statements
from target_table_extract import target_table_extract
from statementtype_count import statementtype_count, logdate_commasep
from determine_access_frequency import determine_access_frequency
from check_refreshing import check_refreshing


def classify_tables(df):
    df = df.copy()
    
    # Create a dictionary to store classification for each table
    table_classifications = {}

    # First pass: Determine classification for each table using latest data
    for table_name in df['ObjectTableName'].unique():
        table_data = df[df['ObjectTableName']==table_name]

        if table_data.empty:
            continue
            
        table_latest_date = table_data['logdate'].max()
        table_latest = table_data[table_data['logdate']==table_latest_date].copy()
        
        classification = 'Active Table'  # Default classification
        
        if table_data['SQLTEXTINFO'].fillna("").str.strip().eq("").all():
            classification = 'Active Table'
        else:
            # Get all create and drop operations for this table from latest data
            creates = table_latest[table_latest['SQLTEXTINFO'].str.contains('Create ', case=False, na=False)]
            mods = table_latest[table_latest['SQLTEXTINFO'].str.contains('Drop|Rename', case=False, na=False)]
            
            is_temp = False
            
            # if we have only create statements, mark as active
            if not creates.empty and mods.empty:
                classification = 'Active Table'
            # if we have only drop/rename statements, mark as isolated
            elif creates.empty and not mods.empty:
                classification = 'Isolated Drop/Rename'
            # if we have both creates and mods, check for temporary pattern
            elif not creates.empty and not mods.empty:
                for _, create_row in creates.iterrows():
                    for _, mod_row in mods.iterrows():
                        if create_row['ProcID'] == mod_row['ProcID'] and mod_row['QueryID'] > create_row['QueryID']:
                            is_temp = True
                            break
                        # different proc_id--check if mods session_id is greater
                        if mod_row['SessionID'] > create_row['SessionID']:
                            is_temp = True    
                            break
                    if is_temp:
                        break
                
                classification = "Temporary" if is_temp else "Active Table"
        
        # Store the determined classification for this table
        table_classifications[table_name] = classification
        
        # Debug print for specific case
        sel = table_latest.loc[
            (table_latest['StatementType'] == 'Select') & (table_latest['Username'] == 'UMB_UKRBCTL_MRTG')]
        if not sel.empty:
            print(f"classify output:{sel[['Username', 'ObjectDatabaseName', 'StatementType', 'logdate_Grouped', 'StatementType_Count']]}")

    # Second pass: Apply classification to ALL rows of each table
    df['Table_Classification'] = df['ObjectTableName'].map(table_classifications)
    
    # Fill any missing classifications with default
    df['Table_Classification'] = df['Table_Classification'].fillna('Active Table')
    
    return df


if __name__ == "__main__":

    input_file = r"C:\Project_Work\Lineage\work\dbql_extraction\test_classify_tables\Usage_Data_180days.csv"
    output_file = r"C:\Project_Work\Lineage\work\dbql_extraction\test_classify_tables\Usage_Data_180days_output.csv"
    df = pd.read_csv(input_file)
    ps = process_statements(df)
    tt = target_table_extract(ps)
    ld = logdate_commasep(tt)
    stc = statementtype_count(ld)
    daf = determine_access_frequency(stc)
    cr = check_refreshing(daf)

    ct = classify_tables(cr)
