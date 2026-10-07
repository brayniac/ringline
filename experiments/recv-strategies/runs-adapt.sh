#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-adapt.toml:
# per-connection rings with adaptive region sizing. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "plain_4M|--strategy shared --shared-bufs 256 --shared-buf-size 16384"
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "ring|--strategy ring"
  "ring_adapt_256k|--strategy ring --adapt --region-max 262144"
  "ring_adapt_1m|--strategy ring --adapt --region-max 1048576"
  "norewrite_adapt_1m|--strategy ring_norewrite --adapt --region-max 1048576"
)
cells=(
  "stream-16k|64|--msg-size 16384 --stream|--no-ack"
  "stream-16k|1000|--msg-size 16384 --stream|--no-ack"
  "stream-mix|1000|--mix 4096:1,16384:1,65536:1 --stream|--no-ack"
  "pipe8-mixed|1000|--mix 256:9,65536:1 --depth 8|"
  "reqack-1m|64|--msg-size 1048576|"
  "reqack-64k|1000|--msg-size 65536|"
  "mixed|1000|--mix 256:9,65536:1|"
  "reqack-256|10000|--msg-size 256|"
  "reqack-64k|10000|--msg-size 65536|"
)
i=0
for rep in $(seq 1 "$reps"); do
  for cell in "${cells[@]}"; do
    IFS='|' read -r wl conns cargs sextra <<< "$cell"
    k=${#configs[@]}
    for j in $(seq 0 $((k-1))); do
      IFS='|' read -r label sargs <<< "${configs[$(( (j + rep) % k ))]}"
      echo "$i|$rep|$label|$sargs|$wl|$conns|$cargs|$sextra"
      i=$((i+1))
    done
  done
done
