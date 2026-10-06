#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-pool.toml:
# the shared_inc pool sweep. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "inc_4M|--strategy shared_inc --shared-bufs 64 --shared-buf-size 65536"
  "inc_16M|--strategy shared_inc --shared-bufs 256 --shared-buf-size 65536"
  "inc_64M|--strategy shared_inc --shared-bufs 1024 --shared-buf-size 65536"
  "inc_128M|--strategy shared_inc --shared-bufs 2048 --shared-buf-size 65536"
  "inc_256M|--strategy shared_inc --shared-bufs 4096 --shared-buf-size 65536"
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "inc_256M_1m|--strategy shared_inc --shared-bufs 256 --shared-buf-size 1048576"
  "ring|--strategy ring"
  "ring_norewrite|--strategy ring_norewrite"
)
cells=(
  "reqack-64k|10000|--msg-size 65536|"
  "reqack-4k|10000|--msg-size 4096|"
  "reqack-64k|1000|--msg-size 65536|"
  "stream-16k|1000|--msg-size 16384 --stream|--no-ack"
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
