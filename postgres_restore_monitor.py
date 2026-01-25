#!/usr/bin/env python3
"""
PostgreSQL Restore Progress Monitor
Connects to a PostgreSQL server and displays restore operation status
"""

import subprocess
import sys
from datetime import datetime
import argparse

def run_ssh_command(host, user, command):
    """Execute a command via SSH and return the output"""
    # Escape quotes in the command for SSH
    escaped_command = command.replace('"', '\\"')
    ssh_cmd = f'ssh {user}@{host} "{escaped_command}"'
    try:
        result = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"SSH command failed: {e}")
        return None

def get_restore_status(host, ssh_user='nutanix', db_user='postgres'):
    """Get PostgreSQL restore status via SSH"""
    
    # Query for active pg_restore processes
    restore_query = """
    SELECT pid, datname, usename, application_name, state, 
           now() - query_start AS duration, 
           LEFT(query, 150) as current_operation 
    FROM pg_stat_activity 
    WHERE application_name = 'pg_restore' 
    ORDER BY query_start;
    """
    
    # Query for all active processes (non-idle)
    active_query = """
    SELECT pid, datname, usename, application_name, client_addr, state, 
           LEFT(query, 100) as query_preview, backend_start, query_start 
    FROM pg_stat_activity 
    WHERE state != 'idle' 
    ORDER BY query_start;
    """
    
    # Query for table sizes in the target database
    size_query = """
    SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) as table_size 
    FROM pg_class 
    WHERE relname IN ('stock', 'order_line') AND relkind = 'r';
    """
    
    print(f"🔍 Connecting to PostgreSQL server: {host}")
    print("=" * 80)
    
    # Get restore processes
    print("\n📊 **RESTORE OPERATIONS STATUS**\n")
    
    restore_cmd = f'psql -U {db_user} -d postgres -c "{restore_query}"'
    restore_output = run_ssh_command(host, ssh_user, restore_cmd)
    
    if restore_output:
        lines = restore_output.split('\n')
        # Skip SSH warning and process the psql output
        psql_lines = [line for line in lines if not line.startswith('Warning:')]
        
        if len(psql_lines) > 2:  # Has header and data
            restore_processes = []
            in_data = False
            for line in psql_lines:
                if '---' in line:
                    in_data = True
                    continue
                if in_data and line.strip() and not line.startswith('('):
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 7:
                        restore_processes.append({
                            'pid': parts[0],
                            'database': parts[1],
                            'user': parts[2],
                            'app': parts[3],
                            'state': parts[4],
                            'duration': parts[5],
                            'operation': parts[6]
                        })
            
            if restore_processes:
                active_restores = [p for p in restore_processes if p['state'] == 'active']
                idle_restores = [p for p in restore_processes if p['state'] == 'idle']
                
                print(f"🔄 **Status: {'ACTIVE' if active_restores else 'COMPLETING'}**")
                print(f"📈 **Total restore processes:** {len(restore_processes)}")
                print(f"⚡ **Active processes:** {len(active_restores)}")
                print(f"⏸️  **Idle processes:** {len(idle_restores)}")
                
                if active_restores:
                    print(f"\n🚀 **ACTIVE OPERATIONS:**")
                    for proc in active_restores:
                        print(f"   • PID {proc['pid']} - {proc['database']} database")
                        print(f"     Duration: {proc['duration']}")
                        print(f"     Operation: {proc['operation'][:100]}...")
                        print()
                
                if idle_restores:
                    print(f"✅ **COMPLETED RECENT OPERATIONS:**")
                    for proc in idle_restores[-3:]:  # Show last 3 completed
                        print(f"   • PID {proc['pid']} - Completed {proc['duration']} ago")
                        print(f"     Last operation: {proc['operation'][:80]}...")
                        print()
            else:
                print("✅ **No active restore operations found**")
        else:
            print("✅ **No restore operations currently running**")
    
    # Get table sizes for the target database
    print("\n💾 **DATABASE SIZE INFORMATION**\n")
    
    size_cmd = f'psql -U {db_user} -d vglb8diskdb -c "{size_query}"'
    size_output = run_ssh_command(host, ssh_user, size_cmd)
    
    if size_output:
        lines = size_output.split('\n')
        psql_lines = [line for line in lines if not line.startswith('Warning:')]
        
        in_data = False
        for line in psql_lines:
            if '---' in line:
                in_data = True
                continue
            if in_data and line.strip() and not line.startswith('('):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 2:
                    print(f"📊 **{parts[0]}** table: {parts[1]}")
    
    # Get all active processes for context
    print(f"\n🔍 **ALL ACTIVE DATABASE PROCESSES**\n")
    
    active_cmd = f'psql -U {db_user} -d postgres -c "{active_query}"'
    active_output = run_ssh_command(host, ssh_user, active_cmd)
    
    if active_output:
        lines = active_output.split('\n')
        psql_lines = [line for line in lines if not line.startswith('Warning:')]
        
        active_count = 0
        restore_count = 0
        vacuum_count = 0
        
        in_data = False
        for line in psql_lines:
            if '---' in line:
                in_data = True
                continue
            if in_data and line.strip() and not line.startswith('('):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 4:
                    active_count += 1
                    app_name = parts[3] if len(parts) > 3 else ''
                    query = parts[6] if len(parts) > 6 else ''
                    
                    if 'pg_restore' in app_name:
                        restore_count += 1
                    elif 'autovacuum' in query.lower():
                        vacuum_count += 1
        
        print(f"📈 **Summary:**")
        print(f"   • Total active processes: {active_count}")
        print(f"   • Restore processes: {restore_count}")
        print(f"   • Maintenance (autovacuum): {vacuum_count}")
        print(f"   • Other processes: {active_count - restore_count - vacuum_count}")

def main():
    parser = argparse.ArgumentParser(description='Monitor PostgreSQL restore progress')
    parser.add_argument('--host', default='perf105-xray-postgres-server', 
                       help='PostgreSQL server hostname')
    parser.add_argument('--ssh-user', default='nutanix', 
                       help='SSH username for server access')
    parser.add_argument('--db-user', default='postgres', 
                       help='PostgreSQL username')
    parser.add_argument('--watch', '-w', action='store_true',
                       help='Watch mode - refresh every 30 seconds')
    
    args = parser.parse_args()
    
    try:
        if args.watch:
            import time
            print("🔄 **WATCH MODE ENABLED** - Press Ctrl+C to exit\n")
            while True:
                print(f"\n{'='*80}")
                print(f"📅 **Refresh Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*80}")
                
                get_restore_status(args.host, args.ssh_user, args.db_user)
                
                print(f"\n⏰ **Next refresh in 30 seconds...**")
                time.sleep(30)
        else:
            get_restore_status(args.host, args.ssh_user, args.db_user)
            
    except KeyboardInterrupt:
        print("\n\n👋 **Monitoring stopped by user**")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ **Error:** {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
