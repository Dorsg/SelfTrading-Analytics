#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=1
[ "${1-}" = "--kill" ] && DRY_RUN=0

echo "== MEMORY: reclaim pagecache (no processes killed) =="
sync
echo 3 > /proc/sys/vm/drop_caches || sysctl -w vm.drop_caches=3
echo 1 > /proc/sys/vm/compact_memory || true

echo "== SWAP: create temporary 2G swap if none =="
if ! swapon --noheadings | grep -q . ; then
  if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile
  fi
  swapon /swapfile || true
  echo "Swap enabled at /swapfile (2G). Remove later with: swapoff /swapfile && rm -f /swapfile"
else
  echo "Swap already enabled; skipping."
fi

echo "== DISK: clean caches and logs =="
apt-get clean -y || true
rm -rf /var/lib/apt/lists/* || true
journalctl --vacuum-time=7d || true
pip cache purge -y 2>/dev/null || true
npm cache clean --force 2>/dev/null || true

# Project logs: truncate >100MB (keeps files, avoids breaking tailers)
if [ -d "./logs" ]; then
  find ./logs -type f -size +100M -print -exec sh -c ': > "$1"' _ {} \;
fi

echo "== DOCKER: safe prune (won't touch running containers/images) =="
docker system prune -af --volumes || true

echo "== DOCKER: truncate oversized container JSON logs (>100MB) =="
find /var/lib/docker/containers -type f -name "*-json.log" -size +100M -print \
  -exec sh -c ': > "$1"' _ {} \; 2>/dev/null || true

echo "== PROCESS TRIM: build *safe* candidate list (dry-run by default) =="

# Helpers
has_docker_cgroup() {
  grep -qE '(docker|containerd)' "/proc/$1/cgroup" 2>/dev/null
}
has_tty() {
  # returns 0 if process has a controlling TTY (e.g., pts/0)
  local stat
  stat="$(ps -p "$1" -o tty= 2>/dev/null | awk '{$1=$1};1')"
  [ -n "$stat" ] && [ "$stat" != "?" ]
}
ancestor_is_ssh() {
  local p="$1" comm
  for _ in {1..20}; do
    [ -d "/proc/$p" ] || return 1
    comm="$(ps -p "$p" -o comm= 2>/dev/null || true)"
    if echo "$comm" | grep -qiE '^sshd$|^ssh$'; then return 0; fi
    # go up one parent
    p="$(awk '/^PPid:/{print $2}' "/proc/$p/status" 2>/dev/null || echo 1)"
    [ -z "$p" ] && p=1
    [ "$p" -le 1 ] && break
  done
  return 1
}

ME=$$
PP=$PPID

# Build list of heavy non-interactive, non-docker, non-ssh candidates
mapfile -t CANDIDATES < <(
  ps -eo pid,ppid,comm,%mem,%cpu --sort=-%mem \
  | awk 'NR>1 {print $1}' \
  | head -n 80 \
  | while read -r pid; do
      [ -d "/proc/$pid" ] || continue
      # exclude docker/containerd processes
      has_docker_cgroup "$pid" && continue
      # exclude processes with controlling TTY (interactive shells/SSH sessions)
      has_tty "$pid" && continue
      # exclude ssh/sshd ancestry and common system/network daemons
      if ancestor_is_ssh "$pid"; then continue; fi
      comm="$(ps -p "$pid" -o comm= 2>/dev/null || echo '')"
      cmd="$(tr -d "\0" < /proc/$pid/cmdline 2>/dev/null | sed 's/\x00/ /g')"
      if printf '%s\n' "$comm $cmd" | grep -qiE \
          '(sshd|ssh|systemd|systemd-journald|systemd-logind|dbus-daemon|cron|crond|rsyslogd|NetworkManager|systemd-networkd|ifplugd|udevd|polkitd)'; then
        continue
      fi
      # exclude ourselves/parent
      [ "$pid" -eq "$ME" ] && continue
      [ "$pid" -eq "$PP" ] && continue
      echo "$pid"
    done
)

if ((${#CANDIDATES[@]})); then
  echo "Safe candidates to terminate (non-interactive, non-docker, non-ssh):"
  printf '  %s\n' "${CANDIDATES[@]}"
  echo
  ps -o pid,ppid,tty,comm,%mem,%cpu --pid "$(printf '%s,' "${CANDIDATES[@]}" | sed 's/,$//')" || true

  if [ "$DRY_RUN" -eq 1 ]; then
    echo
    echo "Dry-run mode: no processes were harmed. Re-run with '--kill' to actually terminate."
  else
    echo "Sending SIGTERM to candidates…"
    for pid in "${CANDIDATES[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
    sleep 2
    echo "Re-checking survivors (still safe subset) and sending SIGKILL to stubborn ones…"
    for pid in "${CANDIDATES[@]}"; do
      [ -d "/proc/$pid" ] || continue
      # re-validate safety before KILL
      has_docker_cgroup "$pid" && continue
      has_tty "$pid" && continue
      ancestor_is_ssh "$pid" && continue
      kill -KILL "$pid" 2>/dev/null || true
    done
  fi
else
  echo "No safe candidates found to kill."
fi

echo
echo "== Final health =="
free -h
df -h
echo "== DONE =="
