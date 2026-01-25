#!/usr/bin/env python3
"""
PostgreSQL Restore Progress Monitor - Direct Connection
Connects directly to PostgreSQL server and displays restore operation status
"""

import psycopg2
import sys
from datetime import datetime
import argparse
import time


def connect_to_postgres(host, port, user, password, database):
    """Create a connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10,
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ **Connection failed:** {e}")
        return None


def execute_query(conn, query):
    """Execute a query and return results"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return columns, rows
    except psycopg2.Error as e:
        print(f"❌ **Query failed:** {e}")
        return None, None


def format_duration(duration_str):
    """Format duration string for better readability"""
    if not duration_str:
        return "N/A"
    return str(duration_str).split(".")[0]  # Remove microseconds


def get_restore_status(
    host, port=5432, user="postgres", password="postgres", target_db="vglb8diskdb"
):
    """Get PostgreSQL restore status via direct connection"""

    print(f"🔍 Connecting to PostgreSQL server: {host}:{port}")
    print("=" * 80)

    # Connect to postgres database first to check overall activity
    conn = connect_to_postgres(host, port, user, password, "postgres")
    if not conn:
        return False

    try:
        # Query for active pg_restore processes
        restore_query = """
        SELECT pid, datname, usename, application_name, state, 
               EXTRACT(EPOCH FROM (now() - query_start))::int AS duration_seconds,
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

        print("\n📊 **RESTORE OPERATIONS STATUS**\n")

        # Get restore processes
        columns, rows = execute_query(conn, restore_query)

        if rows:
            restore_processes = []
            for row in rows:
                restore_processes.append(
                    {
                        "pid": row[0],
                        "database": row[1],
                        "user": row[2],
                        "app": row[3],
                        "state": row[4],
                        "duration_seconds": row[5],
                        "duration": row[6],
                        "operation": row[7],
                    }
                )

            active_restores = [p for p in restore_processes if p["state"] == "active"]
            idle_restores = [p for p in restore_processes if p["state"] == "idle"]

            print(f"🔄 **Status: {'ACTIVE' if active_restores else 'COMPLETING'}**")
            print(f"📈 **Total restore processes:** {len(restore_processes)}")
            print(f"⚡ **Active processes:** {len(active_restores)}")
            print(f"⏸️  **Idle processes:** {len(idle_restores)}")

            if active_restores:
                print(f"\n🚀 **ACTIVE OPERATIONS:**")
                for proc in active_restores:
                    duration_mins = (
                        proc["duration_seconds"] // 60
                        if proc["duration_seconds"]
                        else 0
                    )
                    duration_secs = (
                        proc["duration_seconds"] % 60 if proc["duration_seconds"] else 0
                    )
                    print(f"   • PID {proc['pid']} - {proc['database']} database")
                    print(f"     Duration: {duration_mins}m {duration_secs}s")
                    print(f"     Operation: {proc['operation'][:100]}...")
                    print()

            if idle_restores:
                print(f"✅ **COMPLETED RECENT OPERATIONS:**")
                for proc in idle_restores[-3:]:  # Show last 3 completed
                    print(
                        f"   • PID {proc['pid']} - Last active {format_duration(proc['duration'])} ago"
                    )
                    print(f"     Last operation: {proc['operation'][:80]}...")
                    print()
        else:
            print("✅ **No restore operations currently running**")

        # Get table sizes for the target database
        print("\n💾 **DATABASE SIZE INFORMATION**\n")

        # Connect to target database to check table sizes
        target_conn = connect_to_postgres(host, port, user, password, target_db)
        if target_conn:
            try:
                size_query = """
                SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) as table_size,
                       pg_total_relation_size(oid) as size_bytes
                FROM pg_class 
                WHERE relname IN ('stock', 'order_line') AND relkind = 'r'
                ORDER BY size_bytes DESC;
                """

                columns, rows = execute_query(target_conn, size_query)
                if rows:
                    for row in rows:
                        print(f"📊 **{row[0]}** table: {row[1]}")
                else:
                    print("📊 No target tables found in database")

            finally:
                target_conn.close()
        else:
            print(f"⚠️  Could not connect to target database: {target_db}")

        # Get all active processes for context
        print(f"\n🔍 **ALL ACTIVE DATABASE PROCESSES**\n")

        columns, rows = execute_query(conn, active_query)

        if rows:
            active_count = 0
            restore_count = 0
            vacuum_count = 0

            for row in rows:
                active_count += 1
                app_name = row[3] if row[3] else ""
                query = row[6] if row[6] else ""

                if "pg_restore" in app_name:
                    restore_count += 1
                elif "autovacuum" in query.lower():
                    vacuum_count += 1

            print(f"📈 **Summary:**")
            print(f"   • Total active processes: {active_count}")
            print(f"   • Restore processes: {restore_count}")
            print(f"   • Maintenance (autovacuum): {vacuum_count}")
            print(
                f"   • Other processes: {active_count - restore_count - vacuum_count}"
            )

            # Show some active processes for context
            if active_count > 0:
                print(f"\n🔍 **Recent Active Processes:**")
                for i, row in enumerate(rows[-5:]):  # Show last 5
                    pid, datname, usename, app_name, client_addr, state, query = row[:7]
                    print(f"   • PID {pid} ({state}) - {datname} - {query[:60]}...")

        return True

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Monitor PostgreSQL restore progress via direct connection"
    )
    parser.add_argument(
        "--host",
        default="perf105-xray-postgres-server",
        help="PostgreSQL server hostname",
    )
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL server port")
    parser.add_argument("--user", default="postgres", help="PostgreSQL username")
    parser.add_argument("--password", default="postgres", help="PostgreSQL password")
    parser.add_argument(
        "--target-db", default="vglb8diskdb", help="Target database being restored"
    )
    parser.add_argument(
        "--watch",
        "-w",
        action="store_true",
        help="Watch mode - refresh every 30 seconds",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Refresh interval in seconds for watch mode",
    )

    args = parser.parse_args()

    # Check if psycopg2 is available
    try:
        import psycopg2
    except ImportError:
        print("❌ **Error:** psycopg2 library not found.")
        print("Install it with: pip install psycopg2-binary")
        sys.exit(1)

    try:
        if args.watch:
            print("🔄 **WATCH MODE ENABLED** - Press Ctrl+C to exit\n")
            while True:
                print(f"\n{'='*80}")
                print(
                    f"📅 **Refresh Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                print(f"{'='*80}")

                success = get_restore_status(
                    args.host, args.port, args.user, args.password, args.target_db
                )
                if not success:
                    print("❌ **Connection failed. Retrying in next cycle...**")

                print(f"\n⏰ **Next refresh in {args.interval} seconds...**")
                time.sleep(args.interval)
        else:
            get_restore_status(
                args.host, args.port, args.user, args.password, args.target_db
            )

    except KeyboardInterrupt:
        print("\n\n👋 **Monitoring stopped by user**")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ **Error:** {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
