#!/bin/bash
# ================================================================
#  cep_fraud_alert.sh — CEP Fraud Detection Alert Report
#
#  Part of the CEP-style fraud detection pipeline.
#  Called by the pipeline's bash action (trigger_alert stage):
#    $1 = fraud_alerts.csv  (top-50 flagged sessions)
#    $2 = fraud_report.txt  (output file)
#
#  Input columns (after score_risk + evaluate_rules stages):
#    Col 1 : user_session        — session window identifier (UUID)
#    Col 2 : count_product_id    — purchase count   (velocity signal)
#    Col 3 : right_sum_price     — total spend €    (exposure signal)
#    Col 4 : right_avg_price     — avg transaction € (baseline signal)
#    Col 5 : right_max_price     — max single tx €   (spike signal)
#    Col 6 : right_min_price     — min single tx €   (probe signal)
#    Col 7 : price_range         — max-min z-score   (card-test signal)
#    Col 8 : velocity_risk       — composite score 0-100 (risk points)
#
#  CEP Pattern matched:
#    COUNT(purchase_event, session_window) >= 3 → ALERT
# ================================================================

INPUT="$1"
OUTPUT="$2"

if [ ! -f "$INPUT" ]; then
  echo "[ERROR] cep_fraud_alert.sh: alert file not found: $INPUT"
  exit 1
fi

NUM_ALERTS=$(( $(wc -l < "$INPUT") - 1 ))

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
BLINK=$'\033[5m'

# ── Summary stats from the alert file ─────────────────────────
TOTAL_EXPOSURE=$(awk -F',' 'NR>1{s+=$3}END{printf "%.2f",s}' "$INPUT")
AVG_RISK=$(awk -F',' 'NR>1{s+=$8;n++}END{if(n>0)printf "%.1f",s/n;else print "0"}' "$INPUT")
MAX_RISK=$(awk -F',' 'NR>1{if($8>m)m=$8}END{printf "%.1f",m}' "$INPUT")
TOP_SESSION=$(awk -F',' 'NR==2{print $1}' "$INPUT")
TOP_COUNT=$(awk -F',' 'NR==2{printf "%.0f",$2}' "$INPUT")
TOP_SPEND=$(awk -F',' 'NR==2{printf "%.2f",$3}' "$INPUT")
TOTAL_TXN=$(awk -F',' 'NR>1{s+=$2}END{printf "%.0f",s}' "$INPUT")

# ── Alert level helper ────────────────────────────────────────
alert_level() {
  local score="$1"
  local lvl
  lvl=$(awk -v s="$score" 'BEGIN{
    if (s >= 50) print "HIGH"
    else if (s >= 20) print "MEDIUM"
    else print "LOW"
  }')
  echo "$lvl"
}

# ── Risk bar (20 chars) ────────────────────────────────────────
risk_bar() {
  local score="$1"
  local filled
  filled=$(awk -v s="$score" 'BEGIN{printf "%d", s/5}')
  local empty=$((20 - filled))
  local bar=""
  for ((i=0; i<filled; i++));  do bar="${bar}█"; done
  for ((i=0; i<empty; i++));   do bar="${bar}░"; done
  echo "$bar"
}

# ── Plain-text report (written to file) ───────────────────────
build_report() {
  echo "========================================================================"
  echo "  ██████╗██████╗ ██████╗     ███████╗██████╗  █████╗ ██╗   ██╗██████╗ "
  echo "  ██╔════╝██╔════╝██╔══██╗    ██╔════╝██╔══██╗██╔══██╗██║   ██║██╔══██╗"
  echo "  ██║     ██████╗ ██████╔╝    █████╗  ██████╔╝███████║██║   ██║██║  ██║"
  echo "  ██║     ██╔════╝██╔═══╝     ██╔══╝  ██╔══██╗██╔══██║██║   ██║██║  ██║"
  echo "  ╚██████╗███████╗██║         ██║     ██║  ██║██║  ██║╚██████╔╝██████╔╝"
  echo "   ╚═════╝╚══════╝╚═╝         ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝ "
  echo "========================================================================"
  echo "  CEP-STYLE FRAUD DETECTION — NOVEMBER 2019 E-COMMERCE"
  printf "  Generated  : %s\n" "$(date)"
  printf "  Source     : 2019-Nov.csv  (~67.5M events)\n"
  printf "  Alert rule : COUNT(purchase, session_window) >= 3\n"
  printf "  Currency   : EUR (converted from USD at 0.92)\n"
  echo "========================================================================"
  echo ""
  echo "  ┌─ CEP PATTERN MATCHED ──────────────────────────────────────────────┐"
  echo "  │  Pattern  : SESSION_VELOCITY_BURST                                 │"
  echo "  │  Rule     : IF count_product_id >= 3 IN session_window → ALERT     │"
  echo "  │  Signals  : velocity (count) × exposure (sum) × spread (max-min)   │"
  echo "  │  Scoring  : velocity_risk = count × avg_price  [MinMax → ×100 pts] │"
  echo "  │             price_range = max - min price      [Z-score scaled]     │"
  echo "  └────────────────────────────────────────────────────────────────────┘"
  echo ""
  printf "  %-6s  %-22s  %5s  %10s  %10s  %9s  %9s  %8s  %8s  %7s\n" \
    "RANK" "SESSION (prefix)" "TXN" "TOTAL(€)" "AVG(€)" "MAX(€)" "MIN(€)" "SPREAD(z)" "RISK" "ALERT"
  echo "  ─────────────────────────────────────────────────────────────────────────────────────────────────────"

  rank=1
  while IFS=',' read -r session count total avg maxp minp spread risk; do
    pfx="${session:0:20}"
    lvl=$(alert_level "$risk")
    bar=$(risk_bar "$risk")
    printf "  %-6d  %-22s  %5.0f  %10.2f  %10.2f  %9.2f  %9.2f  %8.3f  %8.1f  %7s\n" \
      "$rank" "$pfx" "$count" "$total" "$avg" "$maxp" "$minp" "$spread" "$risk" "$lvl"
    rank=$((rank + 1))
  done < <(tail -n +2 "$INPUT")

  echo "  ─────────────────────────────────────────────────────────────────────────────────────────────────────"
  echo ""
  printf "  Sessions flagged      : %s\n"  "$NUM_ALERTS"
  printf "  Total transactions    : %s\n"  "$TOTAL_TXN"
  printf "  Total exposure at risk: €%s\n" "$TOTAL_EXPOSURE"
  printf "  Average risk score    : %s/100\n" "$AVG_RISK"
  printf "  Peak risk score       : %s/100\n" "$MAX_RISK"
  echo ""
  echo "  HIGHEST-RISK SESSION:"
  printf "    Session    : %s\n" "$TOP_SESSION"
  printf "    Purchases  : %s transactions\n" "$TOP_COUNT"
  printf "    Total spend: €%s\n" "$TOP_SPEND"
  echo ""
  echo "  Pipeline topology (CEP model):"
  echo "    [L0]  ingest_stream         Event source: 67M rows → purchase filter"
  echo "    [L1]  cleanse_stream        Pre-processing: fill+drop+map(EUR)+select"
  echo "    [L2]  window_velocity  ─┐"
  echo "          window_exposure  ─┤  Temporal aggregation (5 parallel windows)"
  echo "          window_avg_tx    ─┤  COUNT / SUM / AVG / MAX / MIN per session"
  echo "          window_max_tx    ─┤  on_error=retry×2"
  echo "          window_min_tx    ─┘  on_error=proceed"
  echo "    [L3]  correlate_vel_exp     Multi-stream join: velocity ⋈ exposure"
  echo "    [L4]  correlate_add_avg     Signal correlation: + avg baseline"
  echo "    [L5]  correlate_add_max     Signal correlation: + max spike"
  echo "    [L6]  correlate_add_min     Signal correlation: + min probe"
  echo "    [L7]  extract_patterns      Composite derivation: price_range, velocity_risk"
  echo "    [L8]  score_risk            Scoring: normalize + scale + map(×100)"
  echo "    [L9]  evaluate_rules        Rule engine: filter(count≥3) → sort → limit(50)"
  echo "    [L10] trigger_alert         Action: this report (webhook/notify in prod)"
  echo ""
  echo "  Framework : Data Pipeline Framework | Declarative XML | Zero code"
  echo "  CEP model : Batch-streaming iterator engine with session-window aggregation"
  echo "========================================================================"
}

# Write plain text to output file
build_report > "$OUTPUT"

# ── ANSI coloured terminal output ─────────────────────────────
echo ""
echo "${BOLD}${RED}========================================================================${RESET}"
echo "${BOLD}${RED}  ██████╗██████╗ ██████╗     ███████╗██████╗  █████╗ ██╗   ██╗██████╗ ${RESET}"
echo "${BOLD}${RED}  ██╔════╝██╔════╝██╔══██╗    ██╔════╝██╔══██╗██╔══██╗██║   ██║██╔══██╗${RESET}"
echo "${BOLD}${RED}  ██║     ██████╗ ██████╔╝    █████╗  ██████╔╝███████║██║   ██║██║  ██║${RESET}"
echo "${BOLD}${RED}  ██║     ██╔════╝██╔═══╝     ██╔══╝  ██╔══██╗██╔══██║██║   ██║██║  ██║${RESET}"
echo "${BOLD}${RED}  ╚██████╗███████╗██║         ██║     ██║  ██║██║  ██║╚██████╔╝██████╔╝${RESET}"
echo "${BOLD}${RED}   ╚═════╝╚══════╝╚═╝         ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ${RESET}"
echo "${BOLD}${RED}========================================================================${RESET}"
printf "${BOLD}  CEP-STYLE FRAUD DETECTION — NOVEMBER 2019 E-COMMERCE${RESET}\n"
printf "${DIM}  Generated  : %s${RESET}\n" "$(date)"
printf "${DIM}  Source     : 2019-Nov.csv  (~67.5M events)${RESET}\n"
printf "${BOLD}${RED}  Alert rule : COUNT(purchase, session_window) >= 3${RESET}\n"
printf "${DIM}  Currency   : EUR (converted from USD at 0.92)${RESET}\n"
echo "${BOLD}${RED}========================================================================${RESET}"
echo ""
echo "${YELLOW}  ┌─ CEP PATTERN MATCHED ──────────────────────────────────────────────┐${RESET}"
echo "${YELLOW}  │${RESET}  Pattern  : ${BOLD}SESSION_VELOCITY_BURST${RESET}"
echo "${YELLOW}  │${RESET}  Rule     : ${BOLD}IF count_product_id >= 3 IN session_window → ALERT${RESET}"
echo "${YELLOW}  │${RESET}  Signals  : velocity (count) × exposure (sum) × spread (max-min)"
echo "${YELLOW}  │${RESET}  Scoring  : velocity_risk = count × avg_price  ${DIM}[MinMax → ×100 pts]${RESET}"
echo "${YELLOW}  │${RESET}             price_range = max - min price      ${DIM}[Z-score scaled]${RESET}"
echo "${YELLOW}  └────────────────────────────────────────────────────────────────────┘${RESET}"
echo ""
printf "  ${BOLD}%-6s  %-22s  %5s  %10s  %10s  %9s  %9s  %8s  %8s  %7s  %-20s${RESET}\n" \
  "RANK" "SESSION (prefix)" "TXN" "TOTAL(€)" "AVG(€)" "MAX(€)" "MIN(€)" "SPREAD(z)" "RISK" "LEVEL" "RISK BAR"
echo "${CYAN}  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────${RESET}"

rank=1
while IFS=',' read -r session count total avg maxp minp spread risk; do
  pfx="${session:0:20}"
  lvl=$(alert_level "$risk")
  bar=$(risk_bar "$risk")

  # Colour by alert level
  if   [ "$lvl" = "HIGH" ];   then colour="${BOLD}${RED}";    lvl_col="${BOLD}${RED}${BLINK}HIGH  ${RESET}"
  elif [ "$lvl" = "MEDIUM" ]; then colour="${BOLD}${YELLOW}"; lvl_col="${BOLD}${YELLOW}MEDIUM${RESET}"
  else                              colour="$DIM";             lvl_col="${DIM}LOW   ${RESET}"
  fi

  # Colour z-score: high positive spread = suspicious
  spread_flt=$(printf "%.3f" "$spread" 2>/dev/null || echo "$spread")
  z_sign=$(awk -v z="$spread" 'BEGIN{printf "%d",(z>=1)?1:0}')
  if [ "$z_sign" -eq 1 ]; then
    spread_col="${BOLD}${RED}${spread_flt}${RESET}"
  else
    spread_col="${spread_flt}"
  fi

  printf "  ${colour}%-6d  %-22s  %5.0f  %10.2f  %10.2f  %9.2f  %9.2f${RESET}  %8s  ${colour}%8.1f${RESET}  %s  ${colour}%s${RESET}\n" \
    "$rank" "$pfx" "$count" "$total" "$avg" "$maxp" "$minp" \
    "$spread_col" "$risk" "$lvl_col" "$bar"

  rank=$((rank + 1))
done < <(tail -n +2 "$INPUT")

echo "${CYAN}  ─────────────────────────────────────────────────────────────────────────────────────────────────────────────${RESET}"
echo ""
printf "  ${BOLD}Sessions flagged      :${RESET} ${RED}%s${RESET}\n"       "$NUM_ALERTS"
printf "  ${BOLD}Total transactions    :${RESET} ${YELLOW}%s${RESET}\n"    "$TOTAL_TXN"
printf "  ${BOLD}Total exposure at risk:${RESET} ${RED}€%s${RESET}\n"     "$TOTAL_EXPOSURE"
printf "  ${BOLD}Average risk score    :${RESET} %s/100\n"                 "$AVG_RISK"
printf "  ${BOLD}Peak risk score       :${RESET} ${BOLD}${RED}%s/100${RESET}\n"   "$MAX_RISK"
echo ""
echo "${BOLD}${RED}  ⚠ HIGHEST-RISK SESSION:${RESET}"
printf "    Session    : ${CYAN}%s${RESET}\n"           "$TOP_SESSION"
printf "    Purchases  : ${RED}%s transactions${RESET}\n" "$TOP_COUNT"
printf "    Total spend: ${RED}€%s${RESET}\n"            "$TOP_SPEND"
echo ""
echo "${MAGENTA}  CEP pipeline: 67M events → filter → cleanse(×5) → [window×5 ∥] → correlate×4 → derive → score → rule → alert${RESET}"
echo "${BLUE}  All 14 actions: filter, fill_nulls(×2), drop_nulls, map, select, aggregate(×5),${RESET}"
echo "${BLUE}                  join(×4), derive(×2), normalize, scale, map, sort, limit, bash${RESET}"
echo "${YELLOW}========================================================================${RESET}"
echo ""
echo "  ${BOLD}Report saved →${RESET} $OUTPUT"
echo ""

exit 0
