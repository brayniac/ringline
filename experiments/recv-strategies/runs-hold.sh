#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-hold.toml:
# the lend-cap sweep. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "nohold|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "h5ms_cap0|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 5000 --lend-cap 0"
  "h5ms_cap0.25|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 5000 --lend-cap 0.25"
  "h5ms_cap0.5|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 5000 --lend-cap 0.5"
  "h5ms_cap0.75|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 5000 --lend-cap 0.75"
  "h5ms_cap1.0|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 5000 --lend-cap 1.0"
  "h50ms_cap0|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 50000 --lend-cap 0"
  "h50ms_cap0.25|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 50000 --lend-cap 0.25"
  "h50ms_cap0.5|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 50000 --lend-cap 0.5"
  "h50ms_cap0.75|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 50000 --lend-cap 0.75"
  "h50ms_cap1.0|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --hold-every 2 --hold-us 50000 --lend-cap 1.0"
)
cells=(
  "reqack-4k|1000|--msg-size 4096|"
  "reqack-64k|1000|--msg-size 65536|"
  "mixed|1000|--mix 256:9,65536:1|"
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
