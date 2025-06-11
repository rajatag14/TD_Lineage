import pandas as pd
import logging
from pathlib import Path
from typing import Union, List, Tuple
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def filter_csv_by_db_table_pairs(
    input_csv_path: Union[str, Path],
    output_csv_path: Union[str, Path],
    db_table_pairs: List[Tuple[str, str]],
    db_column: str = 'db',
    table_column: str = 'table',
    chunk_size: int = 100000
) -> bool:
    """
    Filter large CSV file based on specific database/table pairs efficiently.
    
    Args:
        input_csv_path: Path to input CSV file
        output_csv_path: Path to output CSV file  
        db_table_pairs: List of (database, table) tuples to filter for
        db_column: Name of database column in CSV (default: 'db')
        table_column: Name of table column in CSV (default: 'table')
        chunk_size: Number of rows to process at once
        
    Returns:
        bool: True if successful, False otherwise
    """
    
    try:
        # Convert pairs to set for O(1) lookup
        target_pairs = set(db_table_pairs)
        logger.info(f"Filtering for {len(target_pairs)} db/table pairs: {target_pairs}")
        
        # Initialize counters
        total_processed = 0
        total_matched = 0
        first_chunk = True
        
        logger.info(f"Starting to process CSV file: {input_csv_path}")
        
        # Process CSV in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(input_csv_path, chunksize=chunk_size), 1):
            logger.info(f"Processing chunk {chunk_num} ({len(chunk):,} rows)")
            
            # Verify required columns exist
            if db_column not in chunk.columns:
                logger.error(f"Database column '{db_column}' not found in CSV")
                return False
            if table_column not in chunk.columns:
                logger.error(f"Table column '{table_column}' not found in CSV")
                return False
            
            # Create tuple pairs for current chunk
            chunk_pairs = list(zip(chunk[db_column].astype(str), chunk[table_column].astype(str)))
            
            # Create boolean mask for matching rows
            mask = [pair in target_pairs for pair in chunk_pairs]
            
            # Filter chunk based on matching pairs
            filtered_chunk = chunk[mask]
            
            if filtered_chunk.empty:
                logger.info(f"No matches found in chunk {chunk_num}")
                total_processed += len(chunk)
                continue
            
            # Write filtered data to output CSV
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            
            filtered_chunk.to_csv(
                output_csv_path,
                mode=mode,
                header=header,
                index=False
            )
            
            total_processed += len(chunk)
            total_matched += len(filtered_chunk)
            first_chunk = False
            
            logger.info(f"Chunk {chunk_num}: {len(filtered_chunk):,} matches written to output")
            
            # Memory cleanup
            del chunk, filtered_chunk, chunk_pairs, mask
            gc.collect()
        
        logger.info(f"Processing complete!")
        logger.info(f"Total rows processed: {total_processed:,}")
        logger.info(f"Total matching rows: {total_matched:,}")
        logger.info(f"Match rate: {(total_matched/total_processed)*100:.2f}%")
        logger.info(f"Output file: {output_csv_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        return False


def filter_csv_by_single_pair(
    input_csv_path: Union[str, Path],
    output_csv_path: Union[str, Path],
    database: str,
    table: str,
    db_column: str = 'db',
    table_column: str = 'table',
    chunk_size: int = 100000
) -> bool:
    """
    Filter large CSV file for a single database/table pair.
    
    Args:
        input_csv_path: Path to input CSV file
        output_csv_path: Path to output CSV file
        database: Database name to filter for
        table: Table name to filter for
        db_column: Name of database column in CSV (default: 'db')
        table_column: Name of table column in CSV (default: 'table')
        chunk_size: Number of rows to process at once
        
    Returns:
        bool: True if successful, False otherwise
    """
    return filter_csv_by_db_table_pairs(
        input_csv_path=input_csv_path,
        output_csv_path=output_csv_path,
        db_table_pairs=[(database, table)],
        db_column=db_column,
        table_column=table_column,
        chunk_size=chunk_size
    )


def get_unique_db_table_pairs(
    csv_path: Union[str, Path],
    db_column: str = 'db',
    table_column: str = 'table',
    chunk_size: int = 100000
) -> List[Tuple[str, str]]:
    """
    Get all unique database/table pairs from a large CSV file.
    
    Args:
        csv_path: Path to CSV file
        db_column: Name of database column
        table_column: Name of table column
        chunk_size: Number of rows to process at once
        
    Returns:
        List of unique (database, table) tuples
    """
    unique_pairs = set()
    
    try:
        logger.info(f"Scanning CSV for unique db/table pairs: {csv_path}")
        
        for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size), 1):
            # Get unique pairs from current chunk
            chunk_pairs = chunk[[db_column, table_column]].drop_duplicates()
            chunk_pairs_tuples = list(zip(chunk_pairs[db_column].astype(str), 
                                        chunk_pairs[table_column].astype(str)))
            unique_pairs.update(chunk_pairs_tuples)
            
            if chunk_num % 10 == 0:
                logger.info(f"Processed {chunk_num} chunks, found {len(unique_pairs)} unique pairs so far")
        
        result = sorted(list(unique_pairs))
        logger.info(f"Found {len(result)} unique db/table pairs total")
        return result
        
    except Exception as e:
        logger.error(f"Error scanning CSV: {e}")
        return []


# Optimized version using pandas query (fastest for simple filters)
def filter_csv_fast(
    input_csv_path: Union[str, Path],
    output_csv_path: Union[str, Path],
    db_table_pairs: List[Tuple[str, str]],
    db_column: str = 'db',
    table_column: str = 'table',
    chunk_size: int = 100000
) -> bool:
    """
    Ultra-fast CSV filtering using pandas query method.
    """
    try:
        # Build query string for multiple pairs
        conditions = []
        for db, table in db_table_pairs:
            conditions.append(f"({db_column} == '{db}' and {table_column} == '{table}')")
        
        query_string = " or ".join(conditions)
        logger.info(f"Using query: {query_string}")
        
        first_chunk = True
        total_processed = 0
        total_matched = 0
        
        for chunk_num, chunk in enumerate(pd.read_csv(input_csv_path, chunksize=chunk_size), 1):
            # Use pandas query for fast filtering
            filtered_chunk = chunk.query(query_string)
            
            if not filtered_chunk.empty:
                mode = 'w' if first_chunk else 'a'
                header = first_chunk
                filtered_chunk.to_csv(output_csv_path, mode=mode, header=header, index=False)
                first_chunk = False
                total_matched += len(filtered_chunk)
            
            total_processed += len(chunk)
            
            if chunk_num % 50 == 0:
                logger.info(f"Processed {chunk_num} chunks ({total_processed:,} rows)")
        
        logger.info(f"Fast filtering complete: {total_matched:,} matches from {total_processed:,} rows")
        return True
        
    except Exception as e:
        logger.error(f"Fast filtering failed: {e}")
        # Fallback to regular method
        return filter_csv_by_db_table_pairs(input_csv_path, output_csv_path, db_table_pairs, 
                                          db_column, table_column, chunk_size)


# Example usage
if __name__ == "__main__":
    
    # Example 1: Filter for single db/table pair
    success = filter_csv_by_single_pair(
        input_csv_path='large_input.csv',
        output_csv_path='filtered_output.csv',
        database='production_db',
        table='users',
        chunk_size=100000
    )
    
    # Example 2: Filter for multiple db/table pairs
    target_pairs = [
        ('production_db', 'users'),
        ('analytics_db', 'events'),
        ('staging_db', 'products')
    ]
    
    success = filter_csv_by_db_table_pairs(
        input_csv_path='large_input.csv',
        output_csv_path='multi_filtered_output.csv',
        db_table_pairs=target_pairs,
        chunk_size=100000
    )
    
    # Example 3: Get all unique pairs first
    unique_pairs = get_unique_db_table_pairs('large_input.csv')
    print(f"Found unique pairs: {unique_pairs[:10]}...")  # Show first 10
    
    # Example 4: Ultra-fast filtering (best performance)
    success = filter_csv_fast(
        input_csv_path='large_input.csv',
        output_csv_path='fast_filtered_output.csv',
        db_table_pairs=target_pairs,
        chunk_size=100000
    )
    
    if success:
        print("CSV filtering completed successfully!")
    else:
        print("CSV filtering failed!")
