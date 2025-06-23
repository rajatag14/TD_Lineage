import re
import csv
import ast
import sys
from typing import List

def parse_log_entries(lines: List[str], verbose: bool = False) -> List[List[str]]:
    """
    Parse log lines to extract error information from consecutive spool and skip error lines.
    
    Args:
        lines: List of log file lines
        verbose: Whether to print verbose output during processing
        
    Returns:
        List of entries, each containing [database, table, date1, date2, batch]
    """
    # More flexible regex patterns with better whitespace handling
    spool_pattern = r"Spool\s+error\s+for\s+(\w+)\s+tables\s+(.*?)$"
    skip_pattern = r"Skipping\s+date\s+range\s+(\d{4}-\d{2}-\d{2})\s*->\s*(\d{4}-\d{2}-\d{2})\s+for\s+(\w+)\s+batch\s+(\d+)\s+due\s+to\s+error"
    
    entries = []
    processed_pairs = 0
    skipped_lines = 0
    
    i = 0
    while i < len(lines) - 1:
        line1 = lines[i].strip()
        line2 = lines[i + 1].strip()
        
        spool_match = re.match(spool_pattern, line1)
        skip_match = re.match(skip_pattern, line2)
        
        if spool_match and skip_match:
            spool_db = spool_match.group(1)
            table_list_str = spool_match.group(2)
            date1, date2, skip_db, batch = skip_match.groups()
            
            # Ensure both lines match on database
            if spool_db == skip_db:
                try:
                    # Safely parse the table list using ast.literal_eval
                    table_list = ast.literal_eval(table_list_str)
                    
                    # Ensure table_list is actually a list
                    if not isinstance(table_list, list):
                        if verbose:
                            print(f"Warning: Expected list but got {type(table_list)} at line {i+1}: {table_list_str}")
                        table_list = [table_list]  # Convert single item to list
                    
                    # Add entries for each table
                    for table in table_list:
                        entries.append([spool_db, str(table), date1, date2, batch])
                    
                    processed_pairs += 1
                    if verbose:
                        print(f"Processed error pair {processed_pairs}: {spool_db} with {len(table_list)} table(s) at lines {i+1}-{i+2}")
                    
                    i += 2  # Move to next pair
                    
                except (ValueError, SyntaxError) as e:
                    if verbose:
                        print(f"Warning: Could not parse table list at line {i+1}: {table_list_str} - Error: {e}")
                    skipped_lines += 1
                    i += 1
            else:
                if verbose:
                    print(f"Database mismatch at lines {i+1}-{i+2}: '{spool_db}' != '{skip_db}'")
                skipped_lines += 1
                i += 1
        else:
            i += 1
    
    if verbose:
        print(f"\nParsing summary:")
        print(f"- Processed {processed_pairs} error pairs")
        print(f"- Extracted {len(entries)} table entries")
        print(f"- Skipped {skipped_lines} problematic lines")
    
    return entries

def write_csv(entries: List[List[str]], output_path: str, verbose: bool = False) -> None:
    """
    Write entries to CSV file.
    
    Args:
        entries: List of entries to write
        output_path: Path to output CSV file
        verbose: Whether to print verbose output
    """
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Database', 'Table', 'Start_Date', 'End_Date', 'Batch'])  # Header
            writer.writerows(entries)
        
        if verbose:
            print(f"Successfully wrote {len(entries)} entries to {output_path}")
            
    except IOError as e:
        print(f"Error writing to CSV file {output_path}: {e}")
        sys.exit(1)

# Configuration - modify these paths as needed
log_file_path = 'pipeline.log'
output_csv_path = 'extracted_errors.csv'
verbose = True  # Set to False to reduce output

# Main execution
try:
    # Read log file
    with open(log_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    if verbose:
        print(f"Read {len(lines)} lines from {log_file_path}")
        
except FileNotFoundError:
    print(f"Error: Log file '{log_file_path}' not found")
    sys.exit(1)
except IOError as e:
    print(f"Error reading log file '{log_file_path}': {e}")
    sys.exit(1)

# Parse log entries
entries = parse_log_entries(lines, verbose)

if not entries:
    print("No matching error patterns found in the log file")
    if not verbose:
        print("Set verbose=True to see more details")
else:
    # Write to CSV
    write_csv(entries, output_csv_path, verbose)
    print(f"Extraction complete: {len(entries)} entries written to {output_csv_path}")
