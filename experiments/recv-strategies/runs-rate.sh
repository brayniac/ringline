#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-rate.toml:
# latency at fixed offered load. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "today_4M|--strategy shared --shared-bufs 256 --shared-buf-size 16384"
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "plain_64M_64k|--strategy shared --shared-bufs 1024 --shared-buf-size 65536"
  "plain_64M_1m|--strategy shared --shared-bufs 64 --shared-buf-size 1048576"
)
cells=(
  "reqack-1m-r375|64|--msg-size 1048576 --rate 375|"
  "reqack-1m-r750|64|--msg-size 1048576 --rate 750|"
  "reqack-1m-r1125|64|--msg-size 1048576 --rate 1125|"
  "reqack-1m-r1350|64|--msg-size 1048576 --rate 1350|"
  "reqack-256k-r1350|64|--msg-size 262144 --rate 1350|"
  "reqack-256k-r2700|64|--msg-size 262144 --rate 2700|"
  "reqack-256k-r4050|64|--msg-size 262144 --rate 4050|"
  "reqack-256k-r4850|64|--msg-size 262144 --rate 4850|"
  "reqack-64k-r5000|1000|--msg-size 65536 --rate 5000|"
  "reqack-64k-r10000|1000|--msg-size 65536 --rate 10000|"
  "reqack-64k-r15000|1000|--msg-size 65536 --rate 15000|"
  "reqack-64k-r18000|1000|--msg-size 65536 --rate 18000|"
  "mixed-r20000|1000|--mix 256:9,65536:1 --rate 20000|"
  "mixed-r40000|1000|--mix 256:9,65536:1 --rate 40000|"
  "mixed-r60000|1000|--mix 256:9,65536:1 --rate 60000|"
  "mixed-r72000|1000|--mix 256:9,65536:1 --rate 72000|"
  "reqack-4k-r19000|1000|--msg-size 4096 --rate 19000|"
  "reqack-4k-r38000|1000|--msg-size 4096 --rate 38000|"
  "reqack-4k-r56000|1000|--msg-size 4096 --rate 56000|"
  "reqack-4k-r67000|1000|--msg-size 4096 --rate 67000|"
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
