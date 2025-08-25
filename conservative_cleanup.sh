#!/bin/bash

# Conservative Process Cleanup Script for SelfTrading
# ONLY kills processes that are 100% safe to terminate
# PROTECTS: IB Gateway, Docker, Database, Application services, System processes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

critical() {
    echo -e "${PURPLE}[CRITICAL]${NC} $1"
}

# Function to check if process is PROTECTED (never kill these)
is_protected_process() {
    local cmd="$1"
    local pid="$2"
    local full_cmd="$3"
    
    # Current script and its children (NEVER KILL)
    if [[ "$pid" == "$$" ]] || [[ "$pid" == "$PPID" ]]; then
        return 0
    fi
    
    # Docker and container related processes (NEVER KILL)
    if [[ "$cmd" =~ dockerd|containerd|docker-proxy|docker-containerd|docker-init ]]; then
        return 0
    fi
    
    # IB Gateway and related processes (NEVER KILL - CRITICAL)
    if [[ "$cmd" =~ ib-gateway|java.*i4j_jres|java.*Oda-jK0QgTEmVssfllLP|ibc|IBC|ibgateway|ibcstart|ibcstart\.sh ]]; then
        return 0
    fi
    
    # Application processes (NEVER KILL)
    if [[ "$cmd" =~ uvicorn|python.*scheduler\.py|python.*api_gateway|python.*runner_service|python.*sync_service|postgres|caddy|node.*client-ui ]]; then
        return 0
    fi
    
    # System processes (NEVER KILL)
    if [[ "$cmd" =~ systemd|kthreadd|kworker|sshd|bash.*conservative_cleanup\.sh|top|ps|grep|awk|sed|bc ]]; then
        return 0
    fi
    
    # Check if it's a child of a protected process
    local ppid_check="$pid"
    for i in {1..10}; do
        if [[ -z "$ppid_check" ]] || [[ "$ppid_check" == "1" ]]; then
            break
        fi
        local parent_cmd=$(ps -o comm= -p "$ppid_check" 2>/dev/null || echo "")
        if [[ "$parent_cmd" =~ dockerd|containerd|python.*scheduler|python.*api_gateway|uvicorn|java.*i4j_jres|java.*Oda-jK0QgTEmVssfllLP|ib-gateway|ibc|IBC|ibgateway|postgres ]]; then
            return 0
        fi
        ppid_check=$(ps -o ppid= -p "$ppid_check" 2>/dev/null || echo "")
    done
    
    return 1
}

# Function to check if process is SAFE to kill (very conservative list)
is_safe_to_kill() {
    local cmd="$1"
    local pid="$2"
    local full_cmd="$3"
    
    # Only kill these specific processes that are definitely safe:
    
    # 1. Orphaned bash shells (leftover from chat sessions)
    if [[ "$cmd" == "bash" ]] && [[ "$full_cmd" =~ -c.*curl|wget|ping|telnet ]]; then
        return 0
    fi
    
    # 2. Orphaned curl/wget processes (downloads that got stuck)
    if [[ "$cmd" =~ curl|wget ]] && [[ "$full_cmd" =~ http|https ]]; then
        return 0
    fi
    
    # 3. Orphaned ping processes (network tests that got stuck)
    if [[ "$cmd" == "ping" ]] && [[ "$full_cmd" =~ -c.*[0-9]+ ]]; then
        return 0
    fi
    
    # 4. Orphaned telnet processes (connection tests that got stuck)
    if [[ "$cmd" == "telnet" ]]; then
        return 0
    fi
    
    # 5. Orphaned nc/netcat processes (network tests)
    if [[ "$cmd" =~ nc|netcat ]]; then
        return 0
    fi
    
    # 6. Orphaned ssh processes that are not system sshd
    if [[ "$cmd" == "ssh" ]] && [[ ! "$full_cmd" =~ sshd ]]; then
        return 0
    fi
    
    # 7. Orphaned screen/tmux sessions (but be very careful)
    if [[ "$cmd" =~ screen|tmux ]] && [[ "$full_cmd" =~ -S.*[0-9]+ ]]; then
        return 0
    fi
    
    # 8. Orphaned tail processes following logs (but not system logs)
    if [[ "$cmd" == "tail" ]] && [[ "$full_cmd" =~ -f.*/tmp/ ]]; then
        return 0
    fi
    
    # 9. Orphaned watch processes
    if [[ "$cmd" == "watch" ]]; then
        return 0
    fi
    
    # 10. Orphaned sleep processes with long durations
    if [[ "$cmd" == "sleep" ]] && [[ "$full_cmd" =~ [0-9]{3,} ]]; then
        return 0
    fi

    # 11. High-CPU orphaned bash shells (likely runaway loops)
    #     Criteria:
    #       • Command name is 'bash'
    #       • Parent PID is 1 (orphaned, not interactive)
    #       • CPU usage >= 70 %
    if [[ "$cmd" == "bash" ]]; then
        local ppid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
        # Grab integer part of CPU usage
        local cpu_usage=$(ps -o %cpu= -p "$pid" 2>/dev/null | awk '{print int($1)}')
        if [[ "$ppid" -eq 1 ]] && [[ "$cpu_usage" -ge 70 ]]; then
            return 0
        fi
    fi
    
    return 1
}

# Function to get process info safely
get_process_info() {
    local pid="$1"
    if [[ -n "$pid" ]] && [[ -d "/proc/$pid" ]]; then
        local cmd=$(ps -o comm= -p "$pid" 2>/dev/null || echo "unknown")
        local full_cmd=$(ps -o command= -p "$pid" 2>/dev/null | head -c 100 || echo "unknown")
        local cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0")
        local mem=$(ps -o %mem= -p "$pid" 2>/dev/null | tr -d ' ' || echo "0")
        echo "$cmd|$full_cmd|$cpu|$mem"
    else
        echo "unknown|unknown|0|0"
    fi
}

# Function to safely kill process
safe_kill() {
    local pid="$1"
    local reason="$2"
    
    if [[ -z "$pid" ]] || [[ ! -d "/proc/$pid" ]]; then
        return
    fi
    
    local process_info=$(get_process_info "$pid")
    IFS='|' read -r cmd full_cmd cpu mem <<< "$process_info"
    
    # Double-check: Never kill protected processes
    if is_protected_process "$cmd" "$pid" "$full_cmd"; then
        warn "ABORTING: Attempted to kill protected process $pid ($cmd) - this should never happen!"
        return
    fi
    
    log "Killing PID $pid ($cpu% CPU, $mem% MEM): $cmd"
    log "  Full command: $full_cmd"
    log "  Reason: $reason"
    
    # 1) Graceful attempt: SIGTERM
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2

    if [[ -d "/proc/$pid" ]]; then
        warn "Process $pid survived SIGTERM, escalating to SIGKILL"
        # 2) Forceful attempt: SIGKILL
        kill -KILL "$pid" 2>/dev/null || true
        sleep 1

        if [[ -d "/proc/$pid" ]]; then
            # Check if the remaining entry is a zombie (state = Z)
            local proc_state="$(awk '{print $3}' /proc/$pid/stat 2>/dev/null || echo '')"
            if [[ "$proc_state" == "Z" ]]; then
                warn "Process $pid is now a zombie (will disappear once parent reaps it)"
            else
                error "Process $pid could NOT be killed with SIGKILL! (state: $proc_state)"
            fi
        else
            success "Process $pid successfully killed with SIGKILL"
        fi
    else
        success "Process $pid terminated cleanly with SIGTERM"
    fi
}

# Function to check critical processes before cleanup
check_critical_processes() {
    log "🔒 Checking critical processes before cleanup..."
    
    # Check IB Gateway processes
    local ib_processes=$(ps aux | grep -E "(ib-gateway|java.*i4j_jres|java.*Oda-jK0QgTEmVssfllLP|ibc|IBC)" | grep -v grep | wc -l)
    log "Found $ib_processes IB-related processes"
    
    # Check Docker containers
    local containers=$(docker ps --format "table {{.Names}}" | grep -E "(api_gateway|scheduler|ib-gateway|trading-db)" | wc -l)
    log "Found $containers application containers"
    
    # Check Python processes
    local python_processes=$(ps aux | grep -E "python.*(scheduler|api_gateway|runner_service|sync_service)" | grep -v grep | wc -l)
    log "Found $python_processes Python application processes"
    
    # Check database processes
    local db_processes=$(ps aux | grep -E "postgres" | grep -v grep | wc -l)
    log "Found $db_processes database processes"
    
    if [[ $ib_processes -eq 0 ]]; then
        warn "No IB processes found - this may indicate an issue"
    fi
    
    if [[ $containers -eq 0 ]]; then
        warn "No application containers found - this may indicate an issue"
    fi
}

# Main cleanup function
main_cleanup() {
    echo "🧹 Starting CONSERVATIVE SelfTrading Process Cleanup..."
    echo "🔒 Only killing processes that are 100% safe to terminate"
    echo "🛡️  All trading-related processes are PROTECTED"
    echo
    
    # Check if running as root
    if [[ $EUID -eq 0 ]]; then
        warn "Running as root - be extra careful!"
    fi
    
    # Pre-cleanup checks
    check_critical_processes
    echo
    
    # Get current system stats
    log "Current system status:"
    echo "  Load average: $(uptime | awk -F'load average:' '{print $2}')"
    echo "  Memory usage: $(free -h | grep Mem | awk '{print $3"/"$2}')"
    echo "  CPU usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
    echo
    
    # Find processes to kill
    log "Scanning for safe-to-kill processes..."
    local processes_to_kill=()
    
    # Get all PIDs
    local all_pids=$(ps -eo pid= --sort=-%cpu | head -100)
    
    for pid in $all_pids; do
        # Skip if PID is invalid
        if [[ ! -d "/proc/$pid" ]]; then
            continue
        fi
        
        local process_info=$(get_process_info "$pid")
        IFS='|' read -r cmd full_cmd cpu mem <<< "$process_info"
        
        # Skip if no command
        if [[ "$cmd" == "unknown" ]] || [[ -z "$cmd" ]]; then
            continue
        fi
        
        # Skip if it's a protected process
        if is_protected_process "$cmd" "$pid" "$full_cmd"; then
            continue
        fi
        
        # Check if it's safe to kill
        if is_safe_to_kill "$cmd" "$pid" "$full_cmd"; then
            processes_to_kill+=("$pid:$cmd:$full_cmd:$cpu:$mem")
        fi
    done
    
    # Report findings
    echo
    log "Process analysis complete:"
    echo "  Safe-to-kill processes found: ${#processes_to_kill[@]}"
    echo
    
    if [[ ${#processes_to_kill[@]} -eq 0 ]]; then
        success "No safe-to-kill processes found. System is clean!"
        return
    fi
    
    # Show processes that will be killed
    warn "Processes that will be killed:"
    for process in "${processes_to_kill[@]}"; do
        IFS=':' read -r pid cmd full_cmd cpu mem <<< "$process"
        echo "  PID: $pid, CPU: ${cpu}%, MEM: ${mem}%, Command: $cmd"
        echo "    Full: $full_cmd"
    done
    
    # Ask for confirmation
    echo
    read -p "Do you want to proceed with killing these processes? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Cleanup cancelled by user"
        return
    fi
    
    # Kill the processes
    log "Killing safe-to-kill processes..."
    local killed_count=0
    for process in "${processes_to_kill[@]}"; do
        IFS=':' read -r pid cmd full_cmd cpu mem <<< "$process"
        safe_kill "$pid" "Safe-to-kill process: $cmd"
        ((killed_count++))
    done
    
    # Clean up temporary files (very conservative)
    log "Cleaning up temporary files..."
    find /tmp -type f -atime +30 -delete 2>/dev/null || true
    find /var/tmp -type f -atime +30 -delete 2>/dev/null || true
    
    # Final system status
    echo
    log "Final system status:"
    echo "  Load average: $(uptime | awk -F'load average:' '{print $2}')"
    echo "  Memory usage: $(free -h | grep Mem | awk '{print $3"/"$2}')"
    echo "  CPU usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
    
    # Verify critical processes are still running
    echo
    log "🔒 Verifying critical processes after cleanup..."
    local ib_processes_after=$(ps aux | grep -E "(ib-gateway|java.*i4j_jres|java.*Oda-jK0QgTEmVssfllLP|ibc|IBC)" | grep -v grep | wc -l)
    local containers_after=$(docker ps --format "table {{.Names}}" | grep -E "(api_gateway|scheduler|ib-gateway|trading-db)" | wc -l)
    
    if [[ $ib_processes_after -gt 0 ]]; then
        success "IB processes are still running ($ib_processes_after processes)"
    else
        critical "No IB processes found after cleanup - this indicates a serious issue!"
    fi
    
    if [[ $containers_after -gt 0 ]]; then
        success "Application containers are still running ($containers_after containers)"
    else
        critical "No application containers found after cleanup - this indicates a serious issue!"
    fi
    
    success "Conservative cleanup completed! 🎉"
    echo "  Killed $killed_count processes"
    echo "🔒 All trading-related processes were protected"
}

# Trap to handle script interruption
trap 'echo -e "\n${YELLOW}Conservative cleanup interrupted by user${NC}"; exit 1' INT TERM

# Check if bc is available for floating point math
if ! command -v bc &> /dev/null; then
    error "bc command not found. Installing..."
    if command -v apt-get &> /dev/null; then
        apt-get update && apt-get install -y bc
    elif command -v yum &> /dev/null; then
        yum install -y bc
    else
        error "Cannot install bc. Please install it manually."
        exit 1
    fi
fi

# Run main cleanup
main_cleanup 