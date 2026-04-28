import time
import json
from redis import Redis
from dotenv import load_dotenv
import os
from api.config import ClientConfig
load_dotenv('.env')

redis_conn = Redis.from_url(ClientConfig.REDIS_URL, decode_responses=True)

def process_job(job_data):
    """Process a single file from S3"""
    s3_key = None
    filename = None
    client_id = None
    job_id = None
    
    try:
        s3_key = job_data.get("s3_key")
        filename = job_data.get("filename")
        client_id = job_data.get("client_id")
        job_id = job_data.get("job_id")
        
        # Validate required fields
        if not all([s3_key, filename, client_id, job_id]):
            raise ValueError(f"Missing required fields in job data: {job_data}")
        
        print(f"[WORKER] Processing {filename} for client {client_id}")
        print(f"[WORKER] S3 key: {s3_key}")
        
        # Update status to processing
        job_data["status"] = "processing"
        redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))
        
        # Process the file from S3
        from ingestion.ingest import process_single_file_from_s3
        process_single_file_from_s3(s3_key, filename, client_id)
        
        # Update status to completed
        job_data["status"] = "completed"
        job_data["result"] = {"success": True, "message": "File processed successfully"}
        redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))

        print(f"[WORKER] Completed {filename}")
        print(f"[WORKER] Document should now appear in /documents endpoint")
        
    except FileNotFoundError as e:
        error_msg = f"File system error: {str(e)}"
        print(f"[WORKER FILE ERROR] {filename}: {error_msg}")
        
        if job_id:
            job_data["status"] = "failed"
            job_data["error"] = error_msg
            redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))
    
    except ValueError as e:
        error_msg = f"Validation error: {str(e)}"
        print(f"[WORKER VALIDATION ERROR] {filename}: {error_msg}")
        
        if job_id:
            job_data["status"] = "failed"
            job_data["error"] = error_msg
            redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))
    
    except Exception as e:
        error_msg = f"Processing failed: {str(e)}"
        print(f"[WORKER ERROR] {filename}: {error_msg}")
        
        if job_id:
            job_data["status"] = "failed"
            job_data["error"] = error_msg
            redis_conn.setex(f"job:{job_id}", 3600, json.dumps(job_data))
def main():
    print("[WORKER] Starting Redis queue worker...")
    print("[WORKER] Waiting for jobs from S3-based uploads...")
    while True:
        try:
            # Block and wait for jobs (5 second timeout)
            job_json = redis_conn.brpop("processing_queue", timeout=5)
            
            if job_json:
                _, job_data_str = job_json
                job_data = json.loads(job_data_str)
                process_job(job_data)
            
        except KeyboardInterrupt:
            print("[WORKER] Shutting down...")
            break
        except Exception as e:
            print(f"[WORKER ERROR] {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()