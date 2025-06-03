import csv
import os

def merge_csvs_pure_python(file1_path, file2_path, output_path, buffer_size=8192*1024):
    """
    Merge CSV files using pure Python with minimal memory usage.
    Fastest approach for simple concatenation.
    
    Args:
        file1_path: Path to first CSV file
        file2_path: Path to second CSV file
        output_path: Path for output file
        buffer_size: Buffer size for file operations (8MB default)
    """
    
    print("Merging CSV files...")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        # Copy first file completely
        print("Copying first file...")
        with open(file1_path, 'r', encoding='utf-8') as infile1:
            # Copy in chunks for efficiency
            while True:
                chunk = infile1.read(buffer_size)
                if not chunk:
                    break
                outfile.write(chunk)
        
        # Copy second file without header
        print("Appending second file...")
        with open(file2_path, 'r', encoding='utf-8') as infile2:
            # Skip header line
            next(infile2)
            
            # Copy rest of file
            while True:
                chunk = infile2.read(buffer_size)
                if not chunk:
                    break
                outfile.write(chunk)
    
    print(f"Merge complete! Output: {output_path}")

def merge_csvs_with_validation(file1_path, file2_path, output_path):
    """
    Merge CSVs with header validation to ensure compatibility.
    """
    
    # Validate headers match
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        
        header1 = next(reader1)
        header2 = next(reader2)
        
        if header1 != header2:
            print("Warning: Headers don't match exactly!")
            print(f"File 1 headers: {header1}")
            print(f"File 2 headers: {header2}")
            return False
        else:
            print("✓ Headers match - proceeding with merge")
    
    # Proceed with merge
    merge_csvs_pure_python(file1_path, file2_path, output_path)
    return True

# Usage
if __name__ == "__main__":
    merge_csvs_with_validation(
        "large_file_32gb.csv",
        "medium_file_3gb.csv", 
        "merged_output.csv"
    )
