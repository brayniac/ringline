#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-tworing.toml:
# one ring against two (a second, large-buffer group for connections
# promoted as streaming or holding). `$2` picks the kernel's configs:
# `inc` (6.12+), `plain` (before 6.12), or `inc2` / `plain2` (promotion on a
# socket that still has data queued, plus tiered-cache cells). Both machines read it, so they
# execute the same runs in the same order; run N listens on port BASE+N.
# One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra|streamers|streamer client args
# Streamers connect first, so they are connections 0..streamers; the
# server counts their bytes apart (`--split`), and `--hold-first` makes
# them the holding connections.
reps=${1:-3}
kind=${2:-plain}
# A trailing "m" (inc2m, plain2m) runs the same configs on the mixed-size
# cells instead.
cellset=default
case "$kind" in *2m) cellset=sizes; kind=${kind%m} ;; esac
# inc3 / plain3: the streamer cells only, with and without a 1 s quiet
# period before demotion, to time migrations.
case "$kind" in
  inc3) cellset=streamers; kind=inc2; quiet=1 ;;
  plain3) cellset=streamers; kind=plain2; quiet=1 ;;
esac
if [ "$kind" = inc ]; then
  configs=(
    "inc_64x1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --bounded-acc"
    "tr_inc_64x1m+64x1m|--strategy two_ring_inc --shared-bufs 64 --shared-buf-size 1048576 --large-bufs 64 --large-buf-size 1048576 --promote-on-hold --bounded-acc"
  )
elif [ "$kind" = inc2 ]; then
  tri="--strategy two_ring_inc --shared-bufs 64 --shared-buf-size 1048576 --large-bufs 64 --large-buf-size 1048576 --promote-nonempty --bounded-acc"
  configs=(
    "inc_64x1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576 --bounded-acc"
    "tr_inc_ne_hold|$tri --promote-on-hold"
    "tr_inc_ne|$tri"
  )
elif [ "$kind" = plain2 ]; then
  # Promotion on IORING_CQE_F_SOCK_NONEMPTY: a full completion with data
  # still queued, which a 64 KiB request/response exchange does not leave.
  tr="--strategy two_ring --shared-bufs 4096 --shared-buf-size 65536 --large-bufs 256 --large-buf-size 1048576 --promote-on-hold --promote-nonempty --no-thp --bounded-acc"
  configs=(
    "p4096_64k|--strategy shared --shared-bufs 4096 --shared-buf-size 65536 --no-thp --bounded-acc"
    "tr_ne|$tr"
    "tr_ne_cap64|$tr --max-promoted 64"
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
  # A RAM+disk tiered cache: a quarter of the connections hold each
  # received range for 5 ms, as a request that misses to disk does.
  "tiered-r20000|1000|--msg-size 4096 --rate 20000|--hold-every 4 --hold-us 5000|0|"
  "tiered-mixed-r20000|1000|--msg-size 4096 --rate 20000|--hold-every 4 --hold-us 5000|16|$stream"
)
# Get/set-like request sizes, heavy-tailed: about half 64 B, 0.5% 1 MiB,
# mean about 12.5 KB.
sizes="--mix 64:500,256:200,1024:120,4096:80,16384:50,65536:30,262144:15,1048576:5"
if [ "${quiet:-0}" = 1 ]; then
  # Replace the third config with the second plus a demotion quiet period.
  configs[2]="${configs[1]%%|*}_q1000|${configs[1]#*|} --demote-quiet-ms 1000"
fi
if [ "$cellset" = streamers ]; then
  cells=(
    "mixed-r20000|1000|--msg-size 256 --rate 20000||16|$stream"
    "mixed-hold-r20000|1000|--msg-size 256 --rate 20000|--hold-first 16 --hold-us 10000|16|$stream"
    "tiered-mixed-r20000|1000|--msg-size 4096 --rate 20000|--hold-every 4 --hold-us 5000|16|$stream"
    "sizes-tiered-mixed-r20000|1000|$sizes --rate 20000|--hold-every 4 --hold-us 5000|16|$stream"
  )
fi
if [ "$cellset" = sizes ]; then
  cells=(
    "sizes-r20000|1000|$sizes --rate 20000||0|"
    "sizes-tiered-r20000|1000|$sizes --rate 20000|--hold-every 4 --hold-us 5000|0|"
    "sizes-tiered-mixed-r20000|1000|$sizes --rate 20000|--hold-every 4 --hold-us 5000|16|$stream"
  )
fi
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
