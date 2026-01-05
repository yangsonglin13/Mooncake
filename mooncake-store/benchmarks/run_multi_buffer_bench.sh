#!/bin/bash
# Quick start script for multi-buffer benchmark
# Usage: ./run_multi_buffer_bench.sh [protocol] [operation]

set -e

# Default values
PROTOCOL=${1:-tcp}
OPERATION=${2:-both}
METADATA_SERVER=${METADATA_SERVER:-127.0.0.1:50051}
LOCAL_HOSTNAME=${LOCAL_HOSTNAME:-127.0.0.1:50052}
BATCH_SIZE=${BATCH_SIZE:-32}
VALUE_SIZE=${VALUE_SIZE:-1048576}
ROUNDS=${ROUNDS:-100}

echo "=========================================="
echo "Multi-Buffer Benchmark Runner"
echo "=========================================="
echo "Protocol:        $PROTOCOL"
echo "Operation:       $OPERATION"
echo "Metadata Server: $METADATA_SERVER"
echo "Local Hostname:  $LOCAL_HOSTNAME"
echo "Batch Size:      $BATCH_SIZE"
echo "Value Size:      $((VALUE_SIZE / 1024)) KB"
echo "Rounds:          $ROUNDS"
echo "=========================================="
echo ""

# Build command
CMD="python multi_buffer_bench.py \
    --metadata-server $METADATA_SERVER \
    --local-hostname $LOCAL_HOSTNAME \
    --protocol $PROTOCOL \
    --operation $OPERATION \
    --batch-size $BATCH_SIZE \
    --value-size $VALUE_SIZE \
    --rounds $ROUNDS"

# Add RDMA devices if using RDMA
if [ "$PROTOCOL" == "rdma" ]; then
    if [ -z "$RDMA_DEVICES" ]; then
        echo "Warning: RDMA protocol requires --rdma-devices"
        echo "Please set RDMA_DEVICES environment variable (e.g., export RDMA_DEVICES=mlx5_0)"
        exit 1
    fi
    CMD="$CMD --rdma-devices $RDMA_DEVICES"
fi

echo "Running: $CMD"
echo ""

# Run benchmark
eval $CMD

