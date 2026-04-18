#!/bin/bash

echo "🚀 WRAPPER STARTED"

cd /home/rayaan/incremental_log_data_pipeline || exit 1

source airflow_venv/bin/activate

echo "🔥 Running transform..."

python scripts/transform.py

echo "✅ WRAPPER FINISHED"
