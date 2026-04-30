#!/bin/bash
# ================================================================
#  brand_scorecard.sh — Brand Performance Scorecard Report
#
#  Called by the pipeline's bash action:
#    $1 = input CSV  (brand_top25.csv)
#    $2 = output file (brand_scorecard.txt)
#
#  Columns in input (after score_brands + rank_top_brands stages):
#    brand, sum_price (normalised [0,1]), right_count_product_id,
#    right_avg_price (z-score), right_min_price (€), right_max_price (€),
#    price_spread_pct (€), revenue_per_order (€)
# ================================================================

INPUT="$1"
OUTPUT="$2"

if [ ! -f "$INPUT" ]; then
  echo "[ERROR] brand_scorecard.sh: input file not found: $INPUT"
  exit 1
fi

# ── ANSI colours ──────────────────────────────────────────────
BOLD=$'\033[1m'
DIM=$'\033[2m'
CYAN=$'\033[36m'
YELLOW=$'\033[33m'
GREEN=$'\033[32m'
MAGENTA=$'\033[35m'
BLUE=$'\033[34m'
RED=$'\033[31m'
RESET=$'\033[0m'

# ── Summary figures from the 25 rows ─────────────────────────
TOTAL_ORDERS=$(awk -F',' 'NR>1{s+=$3}END{printf "%.0f",s}' "$INPUT")
MAX_PRICE=$(awk -F',' 'NR>1{if($6>m)m=$6}END{printf "%.2f",m}' "$INPUT")
MAX_ORDERS_BRAND=$(awk -F',' 'NR>1{if($3>m){m=$3;b=$1}}END{print b}' "$INPUT")
MAX_ORDERS_VAL=$(awk -F',' 'NR>1{if($3>m)m=$3}END{printf "%.0f",m}' "$INPUT")
TOP_BRAND=$(awk -F',' 'NR==2{print $1}' "$INPUT")

# ── Bar helper: render a score bar (20 chars wide) ────────────
score_bar() {
  local score="$1"   # float in [0,1]
  local filled
  filled=$(awk -v s="$score" 'BEGIN{printf "%d", s*20}')
  local empty=$((20 - filled))
  local bar=""
  for ((i=0; i<filled; i++));  do bar="${bar}█"; done
  for ((i=0; i<empty; i++));   do bar="${bar}░"; done
  echo "$bar"
}

# ── Build plain-text report (written to file) ─────────────────
build_report() {
  echo "========================================================================"
  echo "   BRAND PERFORMANCE SCORECARD — NOVEMBER 2019"
  printf "   Generated : %s\n" "$(date)"
  printf "   Source    : 2019-Nov.csv  (~67,500,000 raw events → 916,940 purchases)\n"
  printf "   Currency  : EUR  (converted from USD at 0.92 rate)\n"
  echo "========================================================================"
  echo ""
  echo "  Column guide:"
  echo "    Rev Score  — normalised revenue [0.00–1.00]  (1.00 = highest revenue brand)"
  echo "    Avg(z)     — z-score of avg order value      (>0 = pricier than average)"
  echo "    Rev/Order  — raw average transaction value in EUR"
  echo "    Price Range— max single order minus min single order (€)"
  echo ""
  printf "  %-4s  %-28s  %9s  %8s  %8s  %10s  %11s  %-20s\n" \
    "Rank" "Brand" "Rev Score" "Avg(z)" "Orders" "Rev/Order" "Price Range" "Score Bar"
  echo "  ──────────────────────────────────────────────────────────────────────────────────────────────"

  rank=1
  while IFS=',' read -r brand rev_score orders avg_z min_price max_price spread rev_per_order; do
    bar=$(score_bar "$rev_score")
    printf "  %-4d  %-28s  %9.4f  %8.3f  %8.0f  %10.2f  %11.2f  %s\n" \
      "$rank" "$brand" "$rev_score" "$avg_z" "$orders" "$rev_per_order" "$spread" "$bar"
    rank=$((rank + 1))
  done < <(tail -n +2 "$INPUT")

  echo "  ──────────────────────────────────────────────────────────────────────────────────────────────"
  echo ""
  printf "  Top-25 combined orders    : %s\n"   "$TOTAL_ORDERS"
  printf "  Highest single order      : €%s\n"  "$MAX_PRICE"
  printf "  Most-ordered brand        : %s  (%s orders)\n" "$MAX_ORDERS_BRAND" "$MAX_ORDERS_VAL"
  printf "  #1 Revenue brand          : %s  (score = 1.0000)\n" "$TOP_BRAND"
  echo ""
  echo "  Pipeline topology:"
  echo "    [1]  filter_purchases      67M events → 916K purchase rows"
  echo "    [2]  clean_purchases       fill_nulls(brand,cat) → drop_nulls → map(USD→EUR) → select"
  echo "    [3a] agg_revenue    ─┐"
  echo "    [3b] agg_orders     ─┤"
  echo "    [3c] agg_avg        ─┤  5 parallel aggregations (sum/count/avg/min/max)"
  echo "    [3d] agg_min        ─┤  (on_error=proceed)"
  echo "    [3e] agg_max        ─┘  (on_error=retry×2)"
  echo "    [4a] join_rev_orders       revenue ⋈ orders    on brand"
  echo "    [4b] join_with_avg         + avg               on brand"
  echo "    [4c] join_with_min         + min               on brand"
  echo "    [4d] join_with_max         + max               on brand"
  echo "    [5]  derive_metrics        price_spread = max-min | rev_per_order = sum/count"
  echo "    [6]  score_brands          normalize(revenue) + scale(avg_price z-score)"
  echo "    [7]  rank_top_brands       sort(desc) → top 25"
  echo "    [8]  generate_scorecard    this report"
  echo ""
  echo "  Framework : Data Pipeline Framework | Declarative XML | Zero code"
  echo "========================================================================"
}

# Write plain text to the output file
build_report > "$OUTPUT"

# ── Print coloured version to terminal ───────────────────────
echo ""
echo "${BOLD}${YELLOW}========================================================================${RESET}"
echo "${BOLD}${YELLOW}   BRAND PERFORMANCE SCORECARD — NOVEMBER 2019${RESET}"
printf "${DIM}   Generated : %s${RESET}\n" "$(date)"
printf "${DIM}   Source    : 2019-Nov.csv  (~67,500,000 raw events → 916,940 purchases)${RESET}\n"
printf "${DIM}   Currency  : EUR  (converted from USD at 0.92 rate)${RESET}\n"
echo "${BOLD}${YELLOW}========================================================================${RESET}"
echo ""
echo "${DIM}  Column guide:${RESET}"
echo "${DIM}    Rev Score  — normalised revenue [0.00–1.00]  (1.00 = highest revenue brand)${RESET}"
echo "${DIM}    Avg(z)     — z-score of avg order value      (>0 = pricier than average)${RESET}"
echo "${DIM}    Rev/Order  — raw average transaction value in EUR${RESET}"
echo "${DIM}    Price Range— max single order minus min single order (€)${RESET}"
echo ""
printf "  ${BOLD}%-4s  %-28s  %9s  %8s  %8s  %10s  %11s  %-20s${RESET}\n" \
  "Rank" "Brand" "Rev Score" "Avg(z)" "Orders" "Rev/Order" "Price Range" "Score Bar"
echo "${CYAN}  ──────────────────────────────────────────────────────────────────────────────────────────────${RESET}"

rank=1
while IFS=',' read -r brand rev_score orders avg_z min_price max_price spread rev_per_order; do

  bar=$(score_bar "$rev_score")

  # Colour by rank
  if   [ "$rank" -eq 1 ];   then colour="${BOLD}${GREEN}"
  elif [ "$rank" -le 3 ];   then colour="${BOLD}${CYAN}"
  elif [ "$rank" -le 10 ];  then colour="$RESET"
  else                           colour="$DIM"
  fi

  # Colour z-score: green if above average, red if below
  avg_z_clean=$(printf "%.3f" "$avg_z" 2>/dev/null || echo "$avg_z")
  z_int=$(awk -v z="$avg_z" 'BEGIN{printf "%d", (z>=0)?1:0}')
  if [ "$z_int" -eq 1 ]; then
    zcol="${GREEN}"
  else
    zcol="${RED}"
  fi

  printf "  ${colour}%-4d  %-28s  %9.4f  ${zcol}%8.3f${colour}  %8.0f  %10.2f  %11.2f${RESET}  %s\n" \
    "$rank" "$brand" "$rev_score" "$avg_z" "$orders" "$rev_per_order" "$spread" "${colour}${bar}${RESET}"

  rank=$((rank + 1))
done < <(tail -n +2 "$INPUT")

echo "${CYAN}  ──────────────────────────────────────────────────────────────────────────────────────────────${RESET}"
echo ""
printf "  ${BOLD}Top-25 combined orders    :${RESET} ${GREEN}%s${RESET}\n"   "$TOTAL_ORDERS"
printf "  ${BOLD}Highest single order      :${RESET} ${GREEN}€%s${RESET}\n"  "$MAX_PRICE"
printf "  ${BOLD}Most-ordered brand        :${RESET} ${GREEN}%s${RESET}  ${DIM}(%s orders)${RESET}\n" "$MAX_ORDERS_BRAND" "$MAX_ORDERS_VAL"
printf "  ${BOLD}#1 Revenue brand          :${RESET} ${GREEN}%s${RESET}  ${DIM}(score = 1.0000)${RESET}\n" "$TOP_BRAND"
echo ""
echo "${MAGENTA}  Pipeline: 67M events → filter → clean(×5) → [agg×5 ∥] → join×4 → derive → normalize/scale → rank${RESET}"
echo "${BLUE}  Actions used: filter, fill_nulls, drop_nulls, map, select, aggregate(×5), join(×4),${RESET}"
echo "${BLUE}               derive(×2), normalize, scale, sort, limit, bash${RESET}"
echo "${YELLOW}========================================================================${RESET}"
echo ""
echo "  ${BOLD}Report saved →${RESET} $OUTPUT"
echo ""

exit 0
