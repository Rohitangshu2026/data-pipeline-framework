#!/bin/bash

INPUT_FILE=$1
MODE=$2
OUTPUT_FILE=$3

echo "[SCRIPT] Input: $INPUT_FILE"
echo "[SCRIPT] Mode: $MODE"
echo "[SCRIPT] Output: $OUTPUT_FILE"

if [ ! -f "$INPUT_FILE" ]; then
  echo "[ERROR] Input file not found"
  exit 1
fi

# Preserve header
head -n 1 "$INPUT_FILE" > "$OUTPUT_FILE"

if [ "$MODE" = "fast" ]; then
  echo "[SCRIPT] Fast mode → copying"
  tail -n +2 "$INPUT_FILE" >> "$OUTPUT_FILE"
else
  echo "[SCRIPT] Normal mode → filtering names with 'A'"
  tail -n +2 "$INPUT_FILE" | grep "A" >> "$OUTPUT_FILE"
fi

echo "[SCRIPT] Done"
exit 0