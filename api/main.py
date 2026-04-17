import os
import uuid
import re
import logging
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from dask_kubernetes.operator import KubeCluster
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
import pyarrow as pa
import gc
import time
from datetime import datetime
from common.config import logger, ENVIRONMENT, S3_BUCKET_NAME, S3_BUCKET_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY
# --- Configuration ---
    
app = FastAPI(title="Dask Drain3 Log Analyzer")

# Enhanced tracker to store timing info
job_tracker = {}

class AnalysisRequest(BaseModel):
    file_key: str
    column_name: str = "message"

# --- Dask Worker Logic ---
def denoise_text(text: str) -> str:
    text = str(text)
    text = re.sub(r'0x[a-fA-F0-9]+', '<HEX>', text)
    text = re.sub(r'\d{4}-\d{2}-\d{2}|\d{2}:\d{2}:\d{2}', '<TIME>', text)
    text = re.sub(r'\b\d+\b', '<NUM>', text)
    return " ".join(text.split())

def process_partition(df_partition: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Worker task optimized for memory efficiency and handling empty logs."""
    miner = TemplateMiner(config=TemplateMinerConfig())
    tracking = {}

    # itertuples is significantly faster and more memory-efficient than iterrows
    for row in df_partition.itertuples():
        raw_val = getattr(row, column_name)
        idx = row[0] 
        
        # --- NEW: Robust Empty/None Handling ---
        # Checks for pandas NA/NaN, empty whitespace strings, or stringified nulls
        if pd.isna(raw_val) or str(raw_val).strip().lower() in ("", "none", "nan", "null"):
            raw_log = "<EMPTY_LOG>"
            processed_log = "<EMPTY_LOG>"
        else:
            raw_log = str(raw_val)
            processed_log = denoise_text(raw_log)
        # ---------------------------------------
        
        c_id = miner.add_log_message(processed_log)["cluster_id"]

        if c_id not in tracking:
            tracking[c_id] = {"rep": raw_log, "first": idx, "last": idx}
        else:
            tracking[c_id]["last"] = idx

    local_clusters = [{
        "redacted_cluster_representative": c.get_template(),
        "cluster_representative": tracking[c.cluster_id]["rep"],
        "total_occurrences": c.size,
        "first_occurrence": tracking[c.cluster_id]["first"],
        "last_occurrence": tracking[c.cluster_id]["last"]
    } for c in miner.drain.clusters]
    
    # Explicit cleanup to free worker memory immediately
    del miner
    del tracking
    gc.collect()

    return pd.DataFrame(local_clusters).convert_dtypes(dtype_backend="pyarrow")

# --- Background Orchestrator ---
def run_dask_pipeline(job_id: str, file_key: str, column_name: str):
    start_time = time.time()
    job_tracker[job_id].update({
        "status": "starting_cluster",
        "start_time": datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
    })
    
    s3_path = f"s3://{S3_BUCKET_NAME}/{file_key}"
    output_csv = f"output_csvs/tocs_{file_key.replace('.parquet', '')}.csv"
    
    cluster = None
    client = None
    try:
        if ENVIRONMENT == "production":
            cluster = KubeCluster(
                name=f'drain3-{job_id[:6]}',
                image='your-repo/drain3-api:latest', 
                resources={"requests": {"memory": "12Gi", "cpu": "2"}}
            )
            cluster.adapt(minimum=2, maximum=20)
        else:
            # Local: Use 1 worker with more RAM to avoid the 4GB Nanny kill
            cluster = LocalCluster(n_workers=1, threads_per_worker=2, memory_limit='6Gi')
            
        client = Client(cluster)
        job_tracker[job_id]["status"] = "processing_data"

        meta = pd.DataFrame({
            "redacted_cluster_representative": pd.Series(dtype='string[pyarrow]'), 
            "cluster_representative": pd.Series(dtype='string[pyarrow]'), 
            "total_occurrences": pd.Series(dtype='int64'), 
            "first_occurrence": pd.Series(dtype='int64'), 
            "last_occurrence": pd.Series(dtype='int64')
        })
        
        # Blocksize 16MiB ensures tasks are small enough for low-RAM workers
        df = dd.read_parquet(
            s3_path,
            columns=[column_name],
            engine="pyarrow",
            blocksize="32MiB",
            storage_options={
                "client_kwargs": {"endpoint_url": S3_BUCKET_ENDPOINT}, 
                "key": S3_ACCESS_KEY, 
                "secret": S3_SECRET_KEY
            }
        )

        partial_clusters = df.map_partitions(process_partition, column_name=column_name, meta=meta)
        
        # Incremental Aggregation to avoid master node memory spikes
        job_tracker[job_id]["status"] = "aggregating_results"
        master_miner = TemplateMiner(config=TemplateMinerConfig())
        final_tracking = {}

        partitions = partial_clusters.to_delayed()
        for p in partitions:
            worker_chunk = p.compute() 
            for _, row in worker_chunk.iterrows():
                m_id = master_miner.add_log_message(row["redacted_cluster_representative"])["cluster_id"]
                if m_id not in final_tracking:
                    final_tracking[m_id] = {
                        "rep": row["cluster_representative"], 
                        "total": row["total_occurrences"], 
                        "first": row["first_occurrence"], 
                        "last": row["last_occurrence"]
                    }
                else:
                    final_tracking[m_id]["total"] += row["total_occurrences"]
                    final_tracking[m_id]["first"] = min(final_tracking[m_id]["first"], row["first_occurrence"])
                    final_tracking[m_id]["last"] = max(final_tracking[m_id]["last"], row["last_occurrence"])

        final_output = [{
            "cluster_id": c.cluster_id, 
            "cluster_representative": final_tracking[c.cluster_id]["rep"], 
            "redacted_cluster_representative": c.get_template(), 
            "total_occurrences": final_tracking[c.cluster_id]["total"], 
            "first_occurrence": final_tracking[c.cluster_id]["first"], 
            "last_occurrence": final_tracking[c.cluster_id]["last"]
        } for c in master_miner.drain.clusters]

        os.makedirs("output_csvs", exist_ok=True)
        pd.DataFrame(final_output).to_csv(output_csv, index=False)
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        job_tracker[job_id].update({
            "status": "finished",
            "duration_seconds": duration,
            "end_time": datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        job_tracker[job_id].update({
            "status": f"failed: {str(e)}",
            "end_time": datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        })
    finally:
        if client: client.close()
        if cluster: cluster.close()

# --- API Endpoints ---
@app.post("/analyze-logs")
async def trigger_analysis(request: AnalysisRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    job_tracker[job_id] = {
        "status": "queued",
        "file": request.file_key,
        "start_time": None,
        "end_time": None,
        "duration_seconds": None
    }
    background_tasks.add_task(run_dask_pipeline, job_id, request.file_key, request.column_name)
    return {"status": "accepted", "job_id": job_id}

@app.get("/job-status/{job_id}")
async def get_job_status(job_id: str):
    if job_id not in job_tracker:
        raise HTTPException(status_code=404, detail="Job ID not found")
    
    job_info = job_tracker[job_id]
    
    # Calculate live duration if still running
    if job_info["status"] not in ["finished"] and "failed" not in job_info["status"]:
        if job_info["start_time"]:
            start_ts = datetime.strptime(job_info["start_time"], '%Y-%m-%d %H:%M:%S').timestamp()
            job_info["duration_seconds"] = round(time.time() - start_ts, 2)

    return job_info