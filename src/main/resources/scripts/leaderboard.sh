#!/bin/bash
# ================================================================
#  leaderboard.sh — Black Friday Revenue Intelligence Report
#
#  Called by the pipeline's bash action:
#    $1 = input CSV  (top10.csv)
#    $2 = output file (report.txt)
#
#  Columns in input (after derive_and_rank stage):
#    category_code, sum_price, right_count_product_id, avg_order_value
# ================================================================

INPUT="$1"
OUTPUT="$2"

if [ ! -f "$INPUT" ]; then
  echo "[ERROR] leaderboard.sh: input file not found: $INPUT"
  exit 1
fi

# ── ANSI colours (terminal only) ─────────────────────────────
BOLD=$'\033[1m'
DIM=$'\033[2m'
CYAN=$'\033[36m'
YELLOW=$'\033[33m'
GREEN=$'\033[32m'
MAGENTA=$'\033[35m'
RESET=$'\033[0m'

# ── Compute summary figures from the top-10 rows ─────────────
TOTAL_REV=$(awk -F',' 'NR>1{s+=$2}END{printf "%.2f",s}' "$INPUT")
TOTAL_ORD=$(awk -F',' 'NR>1{s+=$3}END{printf "%.0f",s}' "$INPUT")

# ── Build plain report (written to file + mirrored to stdout) ─
build_report() {
  echo "========================================================================"
  echo "        BLACK FRIDAY 2019  —  TOP 10 REVENUE CATEGORIES"
  printf "        Generated : %s\n" "$(date)"
  printf "        Source    : 2019-Nov.csv  (~67,500,000 raw events)\n"
  echo "========================================================================"
  echo ""
  printf "  %-4s  %-40s  %13s  %8s  %11s\n" \
    "Rank" "Category" "Revenue (USD)" "Orders" "Avg Order"
  echo "  ------------------------------------------------------------------------"

  rank=1
  while IFS=',' read -r category revenue orders aov; do
    printf "  %-4d  %-40s  %13.2f  %8.0f  %11.2f\n" \
      "$rank" "$category" "$revenue" "$orders" "$aov"
    rank=$((rank + 1))
  done < <(tail -n +2 "$INPUT")

  echo "  ------------------------------------------------------------------------"
  echo ""
  printf "  Top-10 combined revenue : \$%'.2f\n" "$TOTAL_REV" 2>/dev/null \
    || printf "  Top-10 combined revenue : \$%s\n" "$TOTAL_REV"
  printf "  Top-10 combined orders  : %s\n" "$TOTAL_ORD"
  echo ""
  echo "  Pipeline topology:"
  echo "    [1] filter_purchases          67M events → purchase rows only"
  echo "    [2a] aggregate_revenue   ─┐   sum(price)  by category  (parallel)"
  echo "    [2b] aggregate_volume    ─┴─► count(orders) by category (parallel)"
  echo "    [3] join_metrics              revenue ⋈ volume on category_code"
  echo "    [4] derive_and_rank           AOV = revenue/orders → sort → top 10"
  echo "    [5] generate_report           this report"
  echo ""
  echo "  Framework : Data Pipeline Framework | Declarative XML | Zero code"
  echo "========================================================================"
}

# Write plain text to the output file
build_report > "$OUTPUT"

# Print coloured version to terminal
echo ""
echo "${BOLD}${YELLOW}========================================================================${RESET}"
echo "${BOLD}${YELLOW}        BLACK FRIDAY 2019  —  TOP 10 REVENUE CATEGORIES${RESET}"
printf "${DIM}        Generated : %s${RESET}\n" "$(date)"
printf "${DIM}        Source    : 2019-Nov.csv  (~67,500,000 raw events)${RESET}\n"
echo "${BOLD}${YELLOW}========================================================================${RESET}"
echo ""
printf "  ${BOLD}%-4s  %-40s  %13s  %8s  %11s${RESET}\n" \
  "Rank" "Category" "Revenue (USD)" "Orders" "Avg Order"
echo "${CYAN}  ------------------------------------------------------------------------${RESET}"

rank=1
while IFS=',' read -r category revenue orders aov; do
  # Highlight rank 1
  if [ "$rank" -eq 1 ]; then
    colour="${BOLD}${GREEN}"
  elif [ "$rank" -le 3 ]; then
    colour="${BOLD}${CYAN}"
  else
    colour="$RESET"
  fi
  printf "  ${colour}%-4d  %-40s  %13.2f  %8.0f  %11.2f${RESET}\n" \
    "$rank" "$category" "$revenue" "$orders" "$aov"
  rank=$((rank + 1))
done < <(tail -n +2 "$INPUT")

echo "${CYAN}  ------------------------------------------------------------------------${RESET}"
echo ""
printf "  ${BOLD}Top-10 combined revenue :${RESET} ${GREEN}\$%s${RESET}\n" "$TOTAL_REV"
printf "  ${BOLD}Top-10 combined orders  :${RESET} ${GREEN}%s${RESET}\n"   "$TOTAL_ORD"
echo ""
echo "${MAGENTA}  Pipeline: 67M events → filter → [agg revenue ‖ agg volume] → join → derive → rank${RESET}"
echo "${YELLOW}========================================================================${RESET}"
echo ""
echo "  ${BOLD}Report saved →${RESET} $OUTPUT"
echo ""

exit 0
