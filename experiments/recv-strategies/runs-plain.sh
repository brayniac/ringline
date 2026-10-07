#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-plain.toml:
# plain-ring geometry for kernels without INC. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "plain_4M|--strategy shared --shared-bufs 256 --shared-buf-size 16384"
  "plain_16M|--strategy shared --shared-bufs 1024 --shared-buf-size 16384"
  "plain_64M|--strategy shared --shared-bufs 4096 --shared-buf-size 16384"
  "plain_64M_64k|--strategy shared --shared-bufs 1024 --shared-buf-size 65536"
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
)
cells=(
  "reqack-256|64|--msg-size 256|"
  "reqack-256|1000|--msg-size 256|"
  "reqack-256|10000|--msg-size 256|"
  "reqack-64k|64|--msg-size 65536|"
  "reqack-64k|1000|--msg-size 65536|"
  "reqack-64k|10000|--msg-size 65536|"
  "mixed|1000|--mix 256:9,65536:1|"
  "stream-16k|1000|--msg-size 16384 --stream|--no-ack"
  "reqack-1m|64|--msg-size 1048576|"
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
