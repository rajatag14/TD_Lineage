import subprocess
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Queue, Empty
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JSONFileHandler(FileSystemEventHandler):
    """File system event handler to monitor JSON file creation"""
    
    def __init__(self, json_queue, processed_files):
        self.json_queue = json_queue
        self.processed_files = processed_files
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            # Wait a bit to ensure file is fully written
            time.sleep(0.5)
            if self._is_file_complete(event.src_path):
                file_path = Path(event.src_path)
                if file_path not in self.processed_files:
                    self.json_queue.put(file_path)
                    logger.info(f"New JSON file detected: {file_path.name}")
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            time.sleep(0.5)
            if self._is_file_complete(event.src_path):
                file_path = Path(event.src_path)
                if file_path not in self.processed_files:
                    self.json_queue.put(file_path)
                    logger.info(f"JSON file modified and ready: {file_path.name}")
    
    def _is_file_complete(self, file_path):
        """Check if file is completely written by checking if size is stable"""
        try:
            size1 = os.path.getsize(file_path)
            time.sleep(0.1)
            size2 = os.path.getsize(file_path)
            return size1 == size2 and size1 > 0
        except (OSError, FileNotFoundError):
            return False

class ConcurrentJavaPythonPipeline:
    def __init__(self, java_jar_path, java_main_class=None, output_dir="./json_output", max_workers=4):
        self.java_jar_path = java_jar_path
        self.java_main_class = java_main_class
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.max_workers = max_workers
        self.json_queue = Queue()
        self.processed_files = set()
        self.results = []
        self.results_lock = threading.Lock()
        self.java_process = None
        self.file_observer = None
        
    def start_file_monitoring(self):
        """Start monitoring the output directory for new JSON files"""
        event_handler = JSONFileHandler(self.json_queue, self.processed_files)
        self.file_observer = Observer()
        self.file_observer.schedule(event_handler, str(self.output_dir), recursive=False)
        self.file_observer.start()
        logger.info(f"Started monitoring directory: {self.output_dir}")
    
    def stop_file_monitoring(self):
        """Stop file monitoring"""
        if self.file_observer:
            self.file_observer.stop()
            self.file_observer.join()
            logger.info("Stopped file monitoring")
    
    def run_java_async(self, java_args=None):
        """Run Java code asynchronously"""
        try:
            if self.java_main_class:
                cmd = ["java", "-cp", self.java_jar_path, self.java_main_class]
            else:
                cmd = ["java", "-jar", self.java_jar_path]
            
            if java_args:
                cmd.extend(java_args)
            
            # Add output directory as argument if not already specified
            if str(self.output_dir) not in cmd:
                cmd.extend(["--output-dir", str(self.output_dir)])
            
            logger.info(f"Starting Java process: {' '.join(cmd)}")
            
            self.java_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Monitor Java process output in separate thread
            def monitor_java_output():
                for line in iter(self.java_process.stdout.readline, ''):
                    logger.info(f"Java: {line.strip()}")
                
                stderr_output = self.java_process.stderr.read()
                if stderr_output:
                    logger.error(f"Java Error: {stderr_output}")
            
            output_thread = threading.Thread(target=monitor_java_output)
            output_thread.daemon = True
            output_thread.start()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Java process: {e}")
            return False
    
    def process_json_file(self, json_file_path):
        """Process a single JSON file"""
        try:
            logger.info(f"Processing {json_file_path.name}")
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Your extraction logic here - customize based on your needs
            extracted_data = self.extract_data(data, json_file_path.name)
            
            # Add to processed files set
            self.processed_files.add(json_file_path)
            
            # Thread-safe result storage
            with self.results_lock:
                self.results.append({
                    'filename': json_file_path.name,
                    'extracted_data': extracted_data,
                    'processed_at': time.time()
                })
            
            logger.info(f"Successfully processed {json_file_path.name}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {json_file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error processing {json_file_path.name}: {e}")
            return False
    
    def extract_data(self, json_data, filename):
        """Your custom extraction logic - modify this based on your requirements"""
        # Example extraction logic
        extracted = {
            'source_file': filename,
            'record_count': 0,
            'extracted_fields': {},
            'metadata': {}
        }
        
        try:
            if isinstance(json_data, dict):
                extracted['extracted_fields'] = json_data
                extracted['record_count'] = len(json_data) if json_data else 0
                
                # Example: Extract specific fields
                if 'data' in json_data:
                    extracted['metadata']['has_data_field'] = True
                    if isinstance(json_data['data'], list):
                        extracted['record_count'] = len(json_data['data'])
                
            elif isinstance(json_data, list):
                extracted['extracted_fields'] = {'items': json_data}
                extracted['record_count'] = len(json_data)
            
            # Add file processing timestamp
            extracted['metadata']['processed_timestamp'] = time.time()
            
        except Exception as e:
            logger.error(f"Extraction error for {filename}: {e}")
            extracted['error'] = str(e)
        
        return extracted
    
    def json_processor_worker(self, timeout=1):
        """Worker function to process JSON files from queue"""
        while True:
            try:
                json_file = self.json_queue.get(timeout=timeout)
                if json_file is None:  # Poison pill to stop worker
                    break
                
                self.process_json_file(json_file)
                self.json_queue.task_done()
                
            except Empty:
                # Check if Java process is still running
                if self.java_process and self.java_process.poll() is None:
                    continue  # Java still running, keep waiting
                else:
                    # Java finished, process remaining items and exit
                    try:
                        while True:
                            json_file = self.json_queue.get_nowait()
                            self.process_json_file(json_file)
                            self.json_queue.task_done()
                    except Empty:
                        break
    
    def run_concurrent_pipeline(self, java_args=None, max_wait_time=600):
        """Run the complete concurrent pipeline"""
        logger.info("Starting concurrent Java-Python pipeline")
        
        # Clear previous results
        self.results = []
        self.processed_files = set()
        
        # Check for existing JSON files and clean up if needed
        existing_files = list(self.output_dir.glob("*.json"))
        if existing_files:
            logger.info(f"Found {len(existing_files)} existing JSON files, cleaning up...")
            for f in existing_files:
                f.unlink()
        
        # Start file monitoring
        self.start_file_monitoring()
        
        # Start JSON processing workers
        workers = []
        for i in range(self.max_workers):
            worker = threading.Thread(target=self.json_processor_worker)
            worker.daemon = True
            worker.start()
            workers.append(worker)
        
        logger.info(f"Started {self.max_workers} processing workers")
        
        try:
            # Start Java process
            if not self.run_java_async(java_args):
                logger.error("Failed to start Java process")
                return None
            
            # Monitor progress
            start_time = time.time()
            last_file_count = 0
            
            while time.time() - start_time < max_wait_time:
                # Check Java process status
                if self.java_process.poll() is not None:
                    logger.info("Java process completed")
                    break
                
                # Log progress
                current_file_count = len(self.processed_files)
                if current_file_count > last_file_count:
                    logger.info(f"Progress: {current_file_count} files processed")
                    last_file_count = current_file_count
                
                time.sleep(2)
            
            # Wait a bit more for any remaining files
            time.sleep(5)
            
            # Process any remaining files in queue
            remaining_files = []
            try:
                while True:
                    remaining_files.append(self.json_queue.get_nowait())
            except Empty:
                pass
            
            if remaining_files:
                logger.info(f"Processing {len(remaining_files)} remaining files")
                for json_file in remaining_files:
                    self.process_json_file(json_file)
            
            # Wait for all processing to complete
            self.json_queue.join()
            
        finally:
            # Cleanup
            self.stop_file_monitoring()
            
            # Stop workers
            for _ in range(self.max_workers):
                self.json_queue.put(None)  # Poison pill
            
            # Terminate Java process if still running
            if self.java_process and self.java_process.poll() is None:
                logger.info("Terminating Java process")
                self.java_process.terminate()
                self.java_process.wait(timeout=10)
        
        logger.info(f"Pipeline completed. Processed {len(self.results)} files total")
        return self.results
    
    def get_processing_stats(self):
        """Get statistics about the processing"""
        if not self.results:
            return {}
        
        with self.results_lock:
            total_files = len(self.results)
            total_records = sum(r['extracted_data'].get('record_count', 0) for r in self.results)
            
            processing_times = [r['processed_at'] for r in self.results]
            if processing_times:
                start_time = min(processing_times)
                end_time = max(processing_times)
                duration = end_time - start_time
            else:
                duration = 0
        
        return {
            'total_files_processed': total_files,
            'total_records_extracted': total_records,
            'processing_duration_seconds': duration,
            'average_records_per_file': total_records / total_files if total_files > 0 else 0
        }

# Usage example
if __name__ == "__main__":
    # For complex Java applications with multiple modules
    pipeline = ConcurrentJavaPythonPipeline(
        java_jar_path="path/to/your/application.jar",  # or classpath for multiple JARs
        # java_main_class="com.yourcompany.MainClass",  # if using classpath
        output_dir="./json_output",
        max_workers=6  # Adjust based on your system
    )
    
    # Run the pipeline
    results = pipeline.run_concurrent_pipeline(
        java_args=["--config", "config.properties", "--batch-size", "1000"],
        max_wait_time=1800  # 30 minutes timeout
    )
    
    if results:
        stats = pipeline.get_processing_stats()
        print(f"\nPipeline Results:")
        print(f"- Files processed: {stats['total_files_processed']}")
        print(f"- Records extracted: {stats['total_records_extracted']}")
        print(f"- Processing duration: {stats['processing_duration_seconds']:.2f} seconds")
        print(f"- Average records per file: {stats['average_records_per_file']:.1f}")
        
        # Access individual results
        for result in results[:5]:  # Show first 5 results
            print(f"- {result['filename']}: {result['extracted_data']['record_count']} records")
    else:
        print("Pipeline execution failed")q
