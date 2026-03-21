#!/bin/bash

INPUT_FILE=$1
OUTPUT_FILE=$2

echo "[SCRIPT] Enriching data..."

# Add a new column: senior (salary > 200)

echo "name,salary,department,senior" > "$OUTPUT_FILE"

tail -n +2 "$INPUT_FILE" | while IFS=',' read name salary dept
do
  if [ "$salary" -gt 200 ]; then
    echo "$name,$salary,$dept,YES" >> "$OUTPUT_FILE"
  else
    echo "$name,$salary,$dept,NO" >> "$OUTPUT_FILE"
  fi
done

echo "[SCRIPT] Enrichment done"