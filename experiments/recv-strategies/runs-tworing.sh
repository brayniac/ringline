#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-tworing.toml:
# one ring against two (a second, large-buffer group for connections
# promoted as streaming or holding). `$2` picks the kernel's configs:
# `inc` (6.12+) or `plain` (before 6.12). Both machines read it, so they
# execute the same runs in the same order; run N listens on port BASE+N.
# One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra|streamers|streamer client args
# Streamers connect first, so they are connections 0..streamers; the
# server counts their bytes apart (`--split`), and `--hold-first` makes
# them the holding connections.
reps=${1:-3}
kind=${2:-plain}
if [ "$kind" = inc ]; then
  configs=(
    "inc_64x1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --bounded-acc"
    "tr_inc_64x1m+64x1m|--strategy two_ring_inc --shared-bufs 64 --shared-buf-size 1048576 --large-bufs 64 --large-buf-size 1048576 --promote-on-hold --bounded-acc"
  )
else
  configs=(
    "p4096_64k|--strategy shared --shared-bufs 4096 --shared-buf-size 65536 --no-thp --bounded-acc"
    "p1024_1m|--strategy shared --shared-bufs 1024 --shared-buf-size 1048576 --no-thp --bounded-acc"
    "tr_4096x64k+256x1m|--strategy two_ring --shared-bufs 4096 --shared-buf-size 65536 --large-bufs 256 --large-buf-size 1048576 --promote-on-hold --no-thp --bounded-acc"
  )
fi
stream="--mix 4096:1,16384:1,65536:1 --stream"
cells=(
  "stream-mix|1000|$stream|--no-ack|0|"
  "reqack-1m|64|--msg-size 1048576||0|"
  "reqack-64k|1000|--msg-size 65536||0|"
  "reqack-256|10000|--msg-size 256||0|"
  "mixed-r20000|1000|--msg-size 256 --rate 20000||16|$stream"
  "mixed-hold-r20000|1000|--msg-size 256 --rate 20000|--hold-first 16 --hold-us 10000|16|$stream"
)
i=0
for rep in $(seq 1 "$reps"); do
  for cell in "${cells[@]}"; do
    IFS='|' read -r wl conns cargs sextra scount scargs <<< "$cell"
    k=${#configs[@]}
    for j in $(seq 0 $((k-1))); do
      IFS='|' read -r label sargs <<< "${configs[$(( (j + rep) % k ))]}"
      echo "$i|$rep|$label|$sargs|$wl|$conns|$cargs|$sextra|$scount|$scargs"
      i=$((i+1))
    done
  done
done
