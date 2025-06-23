import re
import csv

log_file_path = 'pipeline.log'
output_csv_path = 'extracted_errors.csv'

# Updated regex patterns
spool_pattern = r"Spool error for (\w+) tables (.*?)"
skip_pattern = r"Skipping date range (\d{4}-\d{2}-\d{2}) *-> *(\d{4}-\d{2}-\d{2}) for (\w+) batch (\d+) due to error"

entries = []

with open(log_file_path, 'r') as f:
    lines = f.readlines()

i = 0
while i < len(lines) - 1:
    line1 = lines[i].strip()
    line2 = lines[i + 1].strip()

    spool_match = re.match(spool_pattern, line1)
    skip_match = re.match(skip_pattern, line2)

    if spool_match and skip_match:
        spool_db = spool_match.group(1)
        table_list = eval(spool_match.group(2))  # Convert string list to actual list
        date1, date2, skip_db, batch = skip_match.groups()

        if spool_db == skip_db:  # Ensure both lines match on DB
            for table in table_list:
                entries.append([spool_db, table, date1, date2, batch])
            i += 2  # Move to next pair
        else:
            i += 1
    else:
        i += 1

# Write to CSV
with open(output_csv_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Database', 'Table', 'Date1', 'Date2', 'Batch'])  # Header
    writer.writerows(entries)

print(f"Extracted {len(entries)} entries to {output_csv_path}")
