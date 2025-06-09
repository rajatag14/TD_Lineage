import os
import sys
import logging
from datetime import date, timedelta
from multiprocessing import Pool, cpu_count, Manager
from dotenv import load_dotenv
import pandas as pd
import pyodbc
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
# todo: moving old scheduler to new class
load_dotenv()
CONN_STR = os.getenv("connStr")
PARAM_CSV = "C:\Teradata\pipeline_run\\param.csv"
DONE_CSV = "C:\Teradata\pipeline_run\level_3\\done.csv"
COMPLETED_FILE = "C:\Teradata\pipeline_run\level_3\\completed.txt"
OUTPUT_DIR = "C:\Teradata\pipeline_run\\level_3"
LOG_FILE = "C:\Teradata\pipeline_run\level_3\\pipeline.log"

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=180)
BATCH_DAYS = 30
BATCH_SIZE = 100  # Number of tables per batch
STATEMENT_TYPES = [
    'Insert'
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fh.formatter)
logger.addHandler(sh)


# ─────────────────────────────────────────────────────────────────────────────
# BATCH PROCESSING UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
def load_processed_tables():
    """Load already processed tables from done.csv"""
    if not os.path.exists(DONE_CSV):
        return set()
    df = pd.read_csv(DONE_CSV)
    return set(zip(df['target_db'], df['target_table']))


def group_tables_by_db():
    """Group tables by database and create batches"""
    df = pd.read_csv(PARAM_CSV)
    processed_tables = load_processed_tables()
    
    # Filter out already processed tables
    remaining_tables = []
    for _, row in df.iterrows():
        if (row['target_db'], row['target_table']) not in processed_tables:
            remaining_tables.append((row['target_db'], row['target_table']))
    
    # Group by database
    db_groups = defaultdict(list)
    for db, table in remaining_tables:
        db_groups[db].append(table)
    
    # Create batches for each database
    db_batches = {}
    for db, tables in db_groups.items():
        batches = []
        for i in range(0, len(tables), BATCH_SIZE):
            batch_tables = tables[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            batches.append({
                'batch_number': batch_number,
                'tables': batch_tables,
                'batch_size': len(batch_tables)
            })
        db_batches[db] = batches
    
    return db_batches


def update_done_csv(db_name, batch_number, batch_tables, batch_size):
    """Update done.csv with processed tables and batch info"""
    new_rows = []
    for table in batch_tables:
        new_rows.append({
            'target_db': db_name,
            'target_table': table,
            'batch_number': batch_number,
            'batch_size': batch_size,
            'processed_date': date.today().isoformat()
        })
    
    df_new = pd.DataFrame(new_rows)
    
    if os.path.exists(DONE_CSV):
        df_existing = pd.read_csv(DONE_CSV)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    
    df_combined.to_csv(DONE_CSV, index=False)


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY: Generate six 30-day batches
# ─────────────────────────────────────────────────────────────────────────────
def generate_30d_batches(start: date, end: date):
    cur = start
    while cur < end:
        batch_end = min(cur + timedelta(days=BATCH_DAYS - 1), end)
        yield cur, batch_end
        cur = batch_end + timedelta(days=1)


# ─────────────────────────────────────────────────────────────────────────────
# CORE QUERY FUNCTION (modified to use IN operator for multiple tables)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_batch(db, table_list, stmt, batch_start, batch_end):
    """Fetch data for multiple tables using IN operator"""
    # Create placeholders for IN clause
    placeholders = ','.join(['?' for _ in table_list])
    
    sql = f"""
    SELECT DISTINCT
      QL.ProcID, QL.logdate, QL.CollectTimeStamp,
      OB.SessionID, QL.QueryID, OB.StatementType,
      QL.ObjectDatabaseName, QL.ObjectTableName,
      OB.Username, SB.SQLTEXTINFO
    FROM
      ( SELECT ProcID, QueryID, logdate, SessionID, StatementType, Username
          FROM PDCRINFO.DBQLogTbl_Hst
         WHERE TRIM(StatementType)=?
      ) OB
    JOIN
      ( SELECT ProcID, QueryID, logdate, ObjectDatabaseName, ObjectTableName, CollectTimeStamp
          FROM PDCRINFO.DBQLObjTbl_Hst
         WHERE ObjectDatabaseName=? 
           AND ObjectTableName IN ({placeholders})
      ) QL
        ON OB.ProcID=QL.ProcID
       AND OB.QueryID=QL.QueryID
       AND OB.logdate=QL.logdate
    JOIN
      ( SELECT ProcID, QueryID, logdate, SQLTEXTINFO
          FROM PDCRINFO.DBQLSqlTbl_Hst
         WHERE logdate BETWEEN ? AND ?
      ) SB
        ON SB.ProcID=QL.ProcID
       AND SB.QueryID=QL.QueryID
       AND SB.logdate=QL.logdate
    """
    
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=True)
        cur = conn.cursor()
        # Parameters: stmt, db, *table_list, batch_start, batch_end
        params = [stmt, db] + table_list + [batch_start, batch_end]
        cur.execute(sql, params)
        rows = cur.fetchall()
    except pyodbc.Error as e:
        err = str(e)
        if "-2646" in err:
            logger.warning(f"⚠ Spool error for {db} tables {table_list[:3]}... [{batch_start}→{batch_end}]: skip")
            return None
        if "-3149" in err:
            logger.warning(f"⚠ Max-row-count error for {db} tables {table_list[:3]}... [{batch_start}→{batch_end}]: skip")
            return None
        logger.error(f"✖ DB error for {db} tables {table_list[:3]}... [{batch_start}→{batch_end}]: {err}")
        raise
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    if not rows:
        return pd.DataFrame(columns=[
            "ProcID", "logdate", "CollectTimeStamp", "SessionID",
            "QueryID", "StatementType", "ObjectDatabaseName",
            "ObjectTableName", "Username", "SQLTEXTINFO"
        ])

    return pd.DataFrame.from_records(
        rows,
        columns=[
            "ProcID", "logdate", "CollectTimeStamp", "SessionID",
            "QueryID", "StatementType", "ObjectDatabaseName",
            "ObjectTableName", "Username", "SQLTEXTINFO"
        ]
    )


# ─────────────────────────────────────────────────────────────────────────────
# WORKER: Per-batch per-statementtype
# ─────────────────────────────────────────────────────────────────────────────
def process_batch_for_stmt(stmt, db, batch_info, written_flags):
    """Process a batch of tables for a specific statement type"""
    batch_number = batch_info['batch_number']
    table_list = batch_info['tables']
    batch_size = batch_info['batch_size']
    
    logger.info(f"=== {stmt}: {db} Batch {batch_number} ({batch_size} tables) ===")
    logger.info(f"Tables: {table_list[:5]}{'...' if len(table_list) > 5 else ''}")
    
    csv_path = os.path.join(OUTPUT_DIR, f"{stmt}_queries.csv")
    total_rows = 0

    try:
        for start_dt, end_dt in generate_30d_batches(START_DATE, END_DATE):
            df = fetch_batch(db, table_list, stmt, start_dt, end_dt)
            if df is None:
                break
            row_count = len(df)
            total_rows += row_count
            df.to_csv(csv_path,
                      mode="a",
                      index=False,
                      header=written_flags[stmt])
            written_flags[stmt] = False
            logger.info(f"{db} Batch {batch_number} | {stmt} | {start_dt} → {end_dt} | {row_count} rows")
        
        logger.info(f"✅ Success: {stmt} for {db} Batch {batch_number} - Total Rows: {total_rows}")
        return True, None
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"✖ Error: {stmt} for {db} Batch {batch_number}: {error_msg}")
        logger.error(f"Failed tables: {table_list}")
        return False, error_msg


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PROCESSING FUNCTION
# ─────────────────────────────────────────────────────────────────────────────
def process_database_batches():
    """Process all database batches"""
    db_batches = group_tables_by_db()
    
    if not db_batches:
        logger.info("🎉 No remaining tables to process.")
        return True
    
    # Count total batches for progress tracking
    total_batches = sum(len(batches) for batches in db_batches.values())
    processed_batches = 0
    failed_batches = []
    
    logger.info(f"📊 Total databases: {len(db_batches)}")
    logger.info(f"📊 Total batches: {total_batches}")
    
    manager = Manager()
    
    for stmt in STATEMENT_TYPES:
        logger.info(f"\n🟦 Starting StatementType = {stmt}")
        written_flags = manager.dict({stmt: not os.path.exists(os.path.join(OUTPUT_DIR, f"{stmt}_queries.csv"))})
        
        for db_name, batches in db_batches.items():
            logger.info(f"\n📁 Processing Database: {db_name} ({len(batches)} batches)")
            
            for batch_info in batches:
                processed_batches += 1
                batch_number = batch_info['batch_number']
                batch_size = batch_info['batch_size']
                
                logger.info(f"🔄 Processing {db_name} Batch {batch_number}/{len(batches)} "
                           f"({batch_size} tables) - Overall: {processed_batches}/{total_batches}")
                
                try:
                    success, error = process_batch_for_stmt(stmt, db_name, batch_info, written_flags)
                    
                    if success:
                        # Update done.csv with successful batch
                        update_done_csv(db_name, batch_number, batch_info['tables'], batch_size)
                        logger.info(f"✅ Batch {db_name}-{batch_number} completed and logged to done.csv")
                    else:
                        failed_batches.append({
                            'db': db_name,
                            'batch': batch_number,
                            'tables': batch_info['tables'],
                            'error': error,
                            'statement': stmt
                        })
                        logger.error(f"❌ Batch {db_name}-{batch_number} failed and NOT logged to done.csv")
                
                except Exception as e:
                    error_msg = str(e)
                    failed_batches.append({
                        'db': db_name,
                        'batch': batch_number,
                        'tables': batch_info['tables'],
                        'error': error_msg,
                        'statement': stmt
                    })
                    logger.error(f"❌ Unexpected error in batch {db_name}-{batch_number}: {error_msg}")
    
    # Summary
    successful_batches = processed_batches - len(failed_batches)
    logger.info(f"\n📊 PROCESSING SUMMARY:")
    logger.info(f"✅ Successful batches: {successful_batches}/{processed_batches}")
    logger.info(f"❌ Failed batches: {len(failed_batches)}/{processed_batches}")
    
    if failed_batches:
        logger.error(f"\n❌ FAILED BATCHES DETAILS:")
        for fb in failed_batches:
            logger.error(f"  - {fb['statement']}: {fb['db']} Batch {fb['batch']} "
                        f"({len(fb['tables'])} tables) - Error: {fb['error']}")
    
    return len(failed_batches) == 0


# ─────────────────────────────────────────────────────────────────────────────
# MAIN: Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # Check if already completed
    if os.path.exists(COMPLETED_FILE):
        logger.info("🎉 Process already completed. Remove completed.txt to restart.")
        return

    # Load initial table count for progress tracking
    initial_params = pd.read_csv(PARAM_CSV)
    total_tables = len(initial_params)
    processed_tables = len(load_processed_tables())
    
    logger.info(f"📊 Total tables in param.csv: {total_tables}")
    logger.info(f"📊 Already processed tables: {processed_tables}")
    logger.info(f"📊 Remaining tables: {total_tables - processed_tables}")

    try:
        success = process_database_batches()
        
        # Check if all tables are now processed
        final_processed = len(load_processed_tables())
        if final_processed >= total_tables:
            # Create completion marker
            with open(COMPLETED_FILE, 'w') as f:
                f.write(f'Completed on {date.today().isoformat()}\n')
                f.write(f'Total tables processed: {final_processed}\n')
            logger.info("🎉 All tables processed. Created completed.txt")
        else:
            logger.info(f"📊 Progress: {final_processed}/{total_tables} tables completed")
            logger.info("🔄 Some batches may have failed. Run again to retry remaining tables.")
            
    except Exception as e:
        logger.error(f"💥 Fatal error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
