import re
import csv

# Paths
log_file_path = 'pipeline.log'
output_csv_path = 'extracted_errors.csv'

# Regex patterns
spool_error_pattern = r"Spool error for (\w+) tables (.*?)"
date_skip_pattern = r"Skipping date range (\d{4}-\d{2}-\d{2})->(\d{4}-\d{2}-\d{2}) for \1 due to error"

# Variables
entries = []

with open(log_file_path, 'r') as f:
    lines = f.readlines()

# Process line by line
i = 0
while i < len(lines) - 1:
    line1 = lines[i].strip()
    line2 = lines[i + 1].strip()
    
    match1 = re.match(spool_error_pattern, line1)
    match2 = re.match(date_skip_pattern, line2)

    if match1 and match2:
        db = match1.group(1)
        table_list = eval(match1.group(2))  # converts "['a', 'b']" to actual list
        date1 = match2.group(1)
        date2 = match2.group(2)

        for table in table_list:
            entries.append([db, table, date1, date2])
        
        i += 2  # Move to next pair
    else:
        i += 1

# Write to CSV
with open(output_csv_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Database', 'Table', 'Date1', 'Date2'])  # Header
    writer.writerows(entries)

print(f"Extracted {len(entries)} entries to {output_csv_path}")
