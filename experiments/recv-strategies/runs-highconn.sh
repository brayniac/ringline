#!/usr/bin/env bash
# The two-machine run list for experiments/recv-strategies-2host-highconn.toml:
# 10k and 50k connections, fixed rates and synchronized bursts. Both
# machines read it, so they execute the same runs in the same order; run N
# listens on port BASE+N. One line per run:
#   index|rep|label|server args|workload|conns|client args|server extra
reps=${1:-5}
configs=(
  "today_4M|--strategy shared --shared-bufs 256 --shared-buf-size 16384"
  "inc_64M_1m|--strategy shared_inc --shared-bufs 64 --shared-buf-size 1048576"
  "plain_64M_64k|--strategy shared --shared-bufs 1024 --shared-buf-size 65536"
  "plain_64M_1m|--strategy shared --shared-bufs 64 --shared-buf-size 1048576"
  "plain_1G_1m_nothp|--strategy shared --shared-bufs 1024 --shared-buf-size 1048576 --no-thp"
)
cells=(
  "reqack-256-closed|10000|--msg-size 256|"
  "reqack-256-r53000|10000|--msg-size 256 --rate 53000|"
  "reqack-256-r96000|10000|--msg-size 256 --rate 96000|"
  "reqack-256-burst53000|10000|--msg-size 256 --rate 53000 --burst|"
  "reqack-64k-closed|10000|--msg-size 65536|"
  "reqack-64k-r9000|10000|--msg-size 65536 --rate 9000|"
  "reqack-64k-r16000|10000|--msg-size 65536 --rate 16000|"
  "reqack-64k-burst9000|10000|--msg-size 65536 --rate 9000 --burst|"
  "mixed-closed|10000|--mix 256:9,65536:1|"
  "mixed-r40000|10000|--mix 256:9,65536:1 --rate 40000|"
  "mixed-r72000|10000|--mix 256:9,65536:1 --rate 72000|"
  "mixed-burst40000|10000|--mix 256:9,65536:1 --rate 40000 --burst|"
  "reqack-256-closed|50000|--msg-size 256|"
  "reqack-256-r53000|50000|--msg-size 256 --rate 53000|"
  "reqack-256-r96000|50000|--msg-size 256 --rate 96000|"
  "reqack-256-burst53000|50000|--msg-size 256 --rate 53000 --burst|"
  "reqack-64k-closed|50000|--msg-size 65536|"
  "reqack-64k-r9000|50000|--msg-size 65536 --rate 9000|"
  "reqack-64k-r16000|50000|--msg-size 65536 --rate 16000|"
  "reqack-64k-burst9000|50000|--msg-size 65536 --rate 9000 --burst|"
  "mixed-closed|50000|--mix 256:9,65536:1|"
  "mixed-r40000|50000|--mix 256:9,65536:1 --rate 40000|"
  "mixed-r72000|50000|--mix 256:9,65536:1 --rate 72000|"
  "mixed-burst40000|50000|--mix 256:9,65536:1 --rate 40000 --burst|"
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
