#!/usr/bin/env bash
# download_data.sh — Download UK Road Safety data from Kaggle and prepare for streaming
#
# Prerequisites:
#   pip install kaggle
#   export KAGGLE_USERNAME=your_username
#   export KAGGLE_KEY=your_api_key

set -euo pipefail

DATA_DIR="data"
DATASET="tsiaras/uk-road-safety-accidents-and-vehicles"

echo "=== Step 1: Download dataset from Kaggle ==="
mkdir -p "$DATA_DIR"
kaggle datasets download -d "$DATASET" -p "$DATA_DIR" --unzip

echo "=== Step 2: Sort Accident_Information.csv by Accident_Index ==="
ACCIDENT_FILE="$DATA_DIR/Accident_Information.csv"
if [[ -f "$ACCIDENT_FILE" ]]; then
    head -1 "$ACCIDENT_FILE" > "$DATA_DIR/Accident_Information_sorted.csv"
    tail -n +2 "$ACCIDENT_FILE" | sort -t',' -k1,1 >> "$DATA_DIR/Accident_Information_sorted.csv"
    mv "$DATA_DIR/Accident_Information_sorted.csv" "$ACCIDENT_FILE"
    ACCIDENT_ROWS=$(wc -l < "$ACCIDENT_FILE")
    echo "   Accident file: $((ACCIDENT_ROWS - 1)) data rows"
else
    echo "ERROR: $ACCIDENT_FILE not found"
    exit 1
fi

echo "=== Step 3: Sort and filter Vehicle_Information.csv ==="
VEHICLE_FILE="$DATA_DIR/Vehicle_Information.csv"
if [[ -f "$VEHICLE_FILE" ]]; then
    head -1 "$VEHICLE_FILE" > "$DATA_DIR/Vehicle_Information_filtered.csv"
    # Remove pre-2005 records (Accident_Index starts with year)
    # and sort by Accident_Index
    tail -n +2 "$VEHICLE_FILE" \
        | awk -F',' '$1 >= "2005"' \
        | sort -t',' -k1,1 \
        >> "$DATA_DIR/Vehicle_Information_filtered.csv"

    BEFORE=$(( $(wc -l < "$VEHICLE_FILE") - 1 ))
    mv "$DATA_DIR/Vehicle_Information_filtered.csv" "$VEHICLE_FILE"
    AFTER=$(( $(wc -l < "$VEHICLE_FILE") - 1 ))
    REMOVED=$((BEFORE - AFTER))
    echo "   Vehicle file: $AFTER data rows (removed $REMOVED pre-2005 records)"
else
    echo "ERROR: $VEHICLE_FILE not found"
    exit 1
fi

echo ""
echo "=== Done ==="
echo "Files ready in $DATA_DIR/"
echo "  - Accident_Information.csv"
echo "  - Vehicle_Information.csv"
echo ""
echo "Next: upload to S3 with:"
echo "  aws s3 cp $DATA_DIR/Accident_Information.csv s3://<bucket>/raw/"
echo "  aws s3 cp $DATA_DIR/Vehicle_Information.csv  s3://<bucket>/raw/"
