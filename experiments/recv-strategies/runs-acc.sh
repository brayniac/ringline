#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-acc.toml:
# bounded accumulator. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "inc_64M_1m_bounded|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --bounded-acc"
  "inc_64M_1m_len64k|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --recv-len 65536"
  "plain_1G_1m_nothp|--strategy shared --shared-bufs 1024 --shared-buf-size 1048576 --no-thp"
  "plain_1G_1m_nothp_bounded|--strategy shared --shared-bufs 1024 --shared-buf-size 1048576 --no-thp --bounded-acc"
)
cells=(
  "stream-mix|1000|--mix 4096:1,16384:1,65536:1 --stream|--no-ack"
  "mixed|1000|--mix 256:9,65536:1|"
  "pipe8-mixed|1000|--mix 256:9,65536:1 --depth 8|"
  "reqack-64k|10000|--msg-size 65536|"
  "reqack-1m|64|--msg-size 1048576|"
  "reqack-256|10000|--msg-size 256|"
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
