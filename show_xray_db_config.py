#!/usr/bin/env python3
"""
Script to show current configuration of XRAY PostgreSQL database
Author: Generated script (Python version)
Usage: python3 show_xray_db_config.py [--env-file <path>] [--exact-counts]

This script analyzes PostgreSQL database configuration for XRAY workloads.
By default, it uses estimates for fast performance. Use --exact-counts for
precise measurements at the cost of slower execution on large databases.
"""

import os
import sys
import subprocess
import psutil
import psycopg2
import psycopg2.extras
from pathlib import Path
import shutil
import argparse
from typing import Optional, Dict, Any, List, Tuple, Union

# ANSI color codes
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

class XrayDBConfig:
    def __init__(self, use_exact_counts: bool = False, xray_env_file: str = "/home/nutanix/bin/xray_set_env"):
        self.xray_env_file = xray_env_file
        self.env_vars = {}
        self.db_name = None
        self.db_tablespace = None
        self.pg_connection = None
        self.db_connection = None
        self.use_exact_counts = use_exact_counts
        
        # Default PostgreSQL connection parameters
        self.pg_host = 'localhost'
        self.pg_port = 5432
        self.pg_user = 'postgres'
        self.pg_password = None
        
    def print_status(self, color: str, message: str):
        """Print colored status message"""
        print(f"{color}{message}{Colors.NC}")
        
    def print_section(self, title: str):
        """Print section header"""
        print()
        self.print_status(Colors.CYAN, "━" * 50)
        self.print_status(Colors.CYAN, f"{Colors.BOLD}{title}{Colors.NC}")
        self.print_status(Colors.CYAN, "━" * 50)
        
    def print_header(self):
        """Print script header"""
        print("=" * 50)
        self.print_status(Colors.BLUE, f"{Colors.BOLD}XRAY PostgreSQL Database Configuration{Colors.NC}")
        print("=" * 50)
        print()
        
    def load_xray_environment(self) -> bool:
        """Load XRAY environment variables from file"""
        if not os.path.exists(self.xray_env_file):
            self.print_status(Colors.RED, f"✗ XRAY environment file not found at {self.xray_env_file}")
            return False
            
        try:
            # Read and parse the environment file
            with open(self.xray_env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        # Handle export statements
                        if line.startswith('export '):
                            line = line[7:]  # Remove 'export ' prefix
                        
                        # Split on first = only
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            # Remove quotes if present
                            if (value.startswith('"') and value.endswith('"')) or \
                               (value.startswith("'") and value.endswith("'")):
                                value = value[1:-1]
                            
                            # Handle variable expansion (basic support for $VAR)
                            if '$' in value:
                                # Simple variable substitution
                                import re
                                def replace_var(match):
                                    var_name = match.group(1)
                                    return self.env_vars.get(var_name, os.environ.get(var_name, match.group(0)))
                                value = re.sub(r'\$([A-Za-z_][A-Za-z0-9_]*)', replace_var, value)
                            
                            self.env_vars[key] = value
                            os.environ[key] = value
                        
            self.db_name = self.env_vars.get('XRAY_PG_DB_NAME')
            self.db_tablespace = self.env_vars.get('XRAY_PG_DB_TS')
            
            # Load PostgreSQL connection parameters if available
            self.pg_host = self.env_vars.get('PGHOST', self.pg_host)
            self.pg_port = int(self.env_vars.get('PGPORT', self.pg_port))
            self.pg_user = self.env_vars.get('PGUSER', self.pg_user)
            self.pg_password = self.env_vars.get('PGPASSWORD', self.pg_password)
            
            if not self.db_name:
                self.print_status(Colors.RED, "✗ XRAY_PG_DB_NAME environment variable is not set")
                return False
                
            self.print_status(Colors.GREEN, "✓ Sourced XRAY environment variables")
            return True
            
        except Exception as e:
            self.print_status(Colors.RED, f"✗ Error reading environment file: {e}")
            return False
            
    def check_postgres_running(self) -> bool:
        """Check if PostgreSQL is running"""
        postgres_processes = []
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if proc.info['name'] == 'postgres':
                    postgres_processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
                
        if postgres_processes:
            self.print_status(Colors.GREEN, "✓ PostgreSQL is running")
            self.print_status(Colors.BLUE, f"Active processes: {len(postgres_processes)}")
            return True
        else:
            self.print_status(Colors.RED, "✗ PostgreSQL is not running")
            return False
            
    def connect_to_postgres(self, database: str = 'postgres') -> Optional[psycopg2.extensions.connection]:
        """Create a connection to PostgreSQL database"""
        connection_params = {
            'host': self.pg_host,
            'port': self.pg_port,
            'database': database,
            'user': self.pg_user
        }
        
        # Add password if provided
        if self.pg_password:
            connection_params['password'] = self.pg_password
            
        try:
            # Try to connect with specified parameters
            connection = psycopg2.connect(**connection_params)
            return connection
        except psycopg2.Error as e:
            # Try alternative connection methods
            fallback_methods = [
                # Try with current system user
                {'host': self.pg_host, 'port': self.pg_port, 'database': database},
                # Try peer authentication as postgres user
                {'host': self.pg_host, 'port': self.pg_port, 'database': database, 'user': 'postgres'},
                # Try local socket connection
                {'database': database, 'user': 'postgres'},
                # Try with current user on local socket
                {'database': database}
            ]
            
            for params in fallback_methods:
                try:
                    connection = psycopg2.connect(**params)
                    return connection
                except psycopg2.Error:
                    continue
                    
            self.print_status(Colors.YELLOW, f"⚠ Could not connect to database '{database}': {e}")
            return None
                
    def close_connections(self):
        """Close all database connections"""
        if self.pg_connection:
            self.pg_connection.close()
        if self.db_connection:
            self.db_connection.close()
            
    def get_postgres_version(self) -> Optional[str]:
        """Get PostgreSQL version"""
        if not self.pg_connection:
            self.pg_connection = self.connect_to_postgres()
            
        if self.pg_connection:
            try:
                with self.pg_connection.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    result = cursor.fetchone()
                    return result[0] if result else "Unknown"
            except psycopg2.Error:
                return "Unknown"
        else:
            # Fallback to subprocess if connection fails
            try:
                result = subprocess.run(
                    ['sudo', '-u', 'postgres', 'psql', '-t', '-c', 'SELECT version();'],
                    capture_output=True, text=True, check=True
                )
                return result.stdout.strip()
            except subprocess.CalledProcessError:
                return "Unknown"
            
    def execute_postgres_query(self, query: str, database: str = 'postgres') -> Optional[List[Tuple]]:
        """Execute a PostgreSQL query and return results"""
        connection = None
        
        # Use existing connection or create new one
        if database == 'postgres':
            if not self.pg_connection:
                self.pg_connection = self.connect_to_postgres(database)
            connection = self.pg_connection
        elif database == self.db_name:
            if not self.db_connection:
                self.db_connection = self.connect_to_postgres(database)
            connection = self.db_connection
        else:
            connection = self.connect_to_postgres(database)
            
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    return results
            except psycopg2.Error as e:
                self.print_status(Colors.YELLOW, f"⚠ Query error: {e}")
                return None
        else:
            # Fallback to subprocess if connection fails
            try:
                cmd = ['sudo', '-u', 'postgres', 'psql', '-d', database, '-t', '-c', query]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                if result.stdout.strip():
                    rows = []
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            row = tuple(col.strip() for col in line.split('|'))
                            rows.append(row)
                    return rows
                return []
                
            except subprocess.CalledProcessError as e:
                self.print_status(Colors.YELLOW, f"⚠ Query error: {e}")
                return None
            
    def execute_postgres_formatted_query(self, query: str, database: str = 'postgres') -> bool:
        """Execute a PostgreSQL query with formatted output"""
        connection = None
        
        # Use existing connection or create new one
        if database == 'postgres':
            if not self.pg_connection:
                self.pg_connection = self.connect_to_postgres(database)
            connection = self.pg_connection
        elif database == self.db_name:
            if not self.db_connection:
                self.db_connection = self.connect_to_postgres(database)
            connection = self.db_connection
        else:
            connection = self.connect_to_postgres(database)
            
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    if results:
                        # Get column names
                        colnames = [desc[0] for desc in cursor.description]
                        
                        # Calculate column widths
                        widths = [len(col) for col in colnames]
                        for row in results:
                            for i, val in enumerate(row):
                                widths[i] = max(widths[i], len(str(val)) if val is not None else 4)
                        
                        # Print header
                        header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(colnames))
                        print(header)
                        print("-" * len(header))
                        
                        # Print rows
                        for row in results:
                            formatted_row = " | ".join(
                                str(val).ljust(widths[i]) if val is not None else "NULL".ljust(widths[i])
                                for i, val in enumerate(row)
                            )
                            print(formatted_row)
                        
                        print(f"({len(results)} row{'s' if len(results) != 1 else ''})")
                    else:
                        print("(0 rows)")
                    
                return True
            except psycopg2.Error as e:
                self.print_status(Colors.YELLOW, f"⚠ Query error: {e}")
                return False
        else:
            # Fallback to subprocess if connection fails
            try:
                cmd = ['sudo', '-u', 'postgres', 'psql', '-d', database, '-c', query]
                result = subprocess.run(cmd, check=True)
                return True
            except subprocess.CalledProcessError:
                return False
            
    def check_database_exists(self) -> bool:
        """Check if the target database exists"""
        query = "SELECT datname FROM pg_database WHERE datname = '%s';" % self.db_name
        result = self.execute_postgres_query(query)
        
        if result and len(result) > 0:
            self.print_status(Colors.GREEN, f"✓ Database '{self.db_name}' exists")
            return True
        else:
            self.print_status(Colors.RED, f"✗ Database '{self.db_name}' does not exist")
            self.print_status(Colors.YELLOW, "Available databases:")
            subprocess.run(['sudo', '-u', 'postgres', 'psql', '-l'])
            return False
            
    def get_database_info(self):
        """Get basic database information"""
        if self.use_exact_counts:
            # Use actual disk size calculation
            size_query = """
            pg_size_pretty(pg_database_size(current_database())) as db_size,
            pg_database_size(current_database()) as db_size_bytes,
            """
        else:
            # Use estimated size from pg_stat_database
            size_query = """
            pg_size_pretty(
                COALESCE(
                    (SELECT SUM(pg_total_relation_size(c.oid)) 
                     FROM pg_class c 
                     JOIN pg_namespace n ON n.oid = c.relnamespace 
                     WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')),
                    pg_database_size(current_database())
                )
            ) as db_size,
            COALESCE(
                (SELECT SUM(pg_total_relation_size(c.oid)) 
                 FROM pg_class c 
                 JOIN pg_namespace n ON n.oid = c.relnamespace 
                 WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')),
                pg_database_size(current_database())
            ) as db_size_bytes,
            """
            
        query = f"""
        SELECT 
            current_database() as db_name,
            {size_query}
            (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) as active_connections,
            pg_encoding_to_char(encoding) as encoding,
            datcollate as collation,
            datctype as ctype
        FROM pg_database 
        WHERE datname = current_database();
        """
        
        result = self.execute_postgres_query(query, self.db_name)
        if result and len(result) > 0:
            row = result[0]
            size_type = "Actual" if self.use_exact_counts else "Estimated"
            print(f"  - Database Name: {row[0]}")
            print(f"  - Size ({size_type}): {row[1]} ({row[2]} bytes)")
            print(f"  - Active Connections: {row[3]}")
            print(f"  - Encoding: {row[4]}")
            print(f"  - Collation: {row[5]}")
            print(f"  - Character Type: {row[6]}")
            
    def get_tablespace_info(self):
        """Get database tablespace information"""
        query = f"""
        SELECT 
            d.datname,
            COALESCE(t.spcname, 'pg_default') as tablespace_name,
            COALESCE(pg_tablespace_location(t.oid), 'default location') as tablespace_location,
            CASE 
                WHEN t.spcname IS NULL THEN 'Default tablespace'
                ELSE 'Custom tablespace'
            END as tablespace_type,
            t.oid as tablespace_oid
        FROM pg_database d 
        LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid 
        WHERE d.datname = '{self.db_name}';
        """
        
        result = self.execute_postgres_query(query)
        if result and len(result) > 0:
            row = result[0]
            actual_tablespace = row[1]
            tablespace_location = row[2]
            
            print(f"  - Database: {row[0]}")
            print(f"  - Tablespace: {row[1]}")
            print(f"  - Location: {row[2]}")
            print(f"  - Type: {row[3]}")
            
            # Get detailed filesystem information for the tablespace location
            self.get_tablespace_filesystem_info(actual_tablespace, tablespace_location)
            
            # Check if using expected tablespace
            if self.db_tablespace:
                if actual_tablespace == self.db_tablespace:
                    self.print_status(Colors.GREEN, f"✓ Database is using expected tablespace: {self.db_tablespace}")
                else:
                    self.print_status(Colors.YELLOW, "⚠ Database tablespace mismatch!")
                    print(f"  - Expected: {self.db_tablespace}")
                    print(f"  - Actual: {actual_tablespace}")
                    
            return actual_tablespace
        return None
        
    def get_tablespace_filesystem_info(self, tablespace_name: str, location: str):
        """Get filesystem information for tablespace location"""
        print()
        self.print_status(Colors.BLUE, f"Tablespace '{tablespace_name}' Filesystem Details:")
        
        if location == 'default location':
            # For default tablespace, get the data directory location
            data_dir = self.get_data_directory()
            if data_dir:
                location = f"{data_dir}/base"
                print(f"  - Default Location: {location}")
            else:
                print("  - Location: PostgreSQL default data directory")
                return
        else:
            print(f"  - Custom Location: {location}")
            
        # Check if location exists and get filesystem info
        try:
            location_path = Path(location)
            if location_path.exists():
                self.print_status(Colors.GREEN, "✓ Tablespace location accessible")
                
                # Get filesystem usage information
                self.get_filesystem_usage(str(location_path))
                
                # Get directory size if accessible
                self.get_directory_info(str(location_path))
                
                # Check permissions
                self.check_directory_permissions(str(location_path))
                
            else:
                self.print_status(Colors.YELLOW, "⚠ Tablespace location not accessible or does not exist")
                
        except Exception as e:
            self.print_status(Colors.YELLOW, f"⚠ Could not access tablespace location: {e}")
            
    def get_filesystem_usage(self, path: str):
        """Get filesystem usage information for a path"""
        try:
            # Use df command to get filesystem information
            result = subprocess.run(['df', '-h', path], capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            
            if len(lines) >= 2:
                # Parse df output
                header = lines[0].split()
                data = lines[1].split()
                
                if len(data) >= 6:
                    filesystem = data[0]
                    size = data[1]
                    used = data[2]
                    available = data[3]
                    use_percent = data[4]
                    mount_point = ' '.join(data[5:])  # Handle mount points with spaces
                    
                    print(f"  - Filesystem: {filesystem}")
                    print(f"  - Mount Point: {mount_point}")
                    print(f"  - Total Size: {size}")
                    print(f"  - Used: {used}")
                    print(f"  - Available: {available}")
                    print(f"  - Usage: {use_percent}")
                    
                    # Warn if usage is high
                    try:
                        usage_num = float(use_percent.rstrip('%'))
                        if usage_num >= 90:
                            self.print_status(Colors.RED, f"⚠ WARNING: Filesystem usage is {use_percent} - critically high!")
                        elif usage_num >= 80:
                            self.print_status(Colors.YELLOW, f"⚠ CAUTION: Filesystem usage is {use_percent} - getting high")
                    except ValueError:
                        pass
                        
        except subprocess.CalledProcessError:
            self.print_status(Colors.YELLOW, "⚠ Could not retrieve filesystem usage information")
            
    def get_directory_info(self, path: str):
        """Get directory size and file count information"""
        try:
            path_obj = Path(path)
            if path_obj.is_dir():
                # Get directory size using du
                try:
                    result = subprocess.run(['sudo', 'du', '-sh', path], 
                                          capture_output=True, text=True, check=True)
                    dir_size = result.stdout.split()[0]
                    print(f"  - Directory Size: {dir_size}")
                except subprocess.CalledProcessError:
                    print("  - Directory Size: Unable to determine")
                
                # Count files and subdirectories
                try:
                    file_count = 0
                    dir_count = 0
                    
                    # Use find command for better performance on large directories
                    result = subprocess.run(['sudo', 'find', path, '-type', 'f'], 
                                          capture_output=True, text=True, check=True)
                    file_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
                    
                    result = subprocess.run(['sudo', 'find', path, '-type', 'd'], 
                                          capture_output=True, text=True, check=True)
                    dir_count = len(result.stdout.strip().split('\n')) - 1 if result.stdout.strip() else 0  # -1 to exclude the path itself
                    
                    print(f"  - Files: {file_count}")
                    print(f"  - Subdirectories: {dir_count}")
                    
                except subprocess.CalledProcessError:
                    print("  - File Count: Unable to determine")
                    
        except Exception as e:
            self.print_status(Colors.YELLOW, f"⚠ Could not get directory information: {e}")
            
    def check_directory_permissions(self, path: str):
        """Check directory permissions and ownership"""
        try:
            path_obj = Path(path)
            if path_obj.exists():
                # Get file stats
                stat_info = path_obj.stat()
                
                # Get owner and group names
                try:
                    import pwd
                    import grp
                    owner = pwd.getpwuid(stat_info.st_uid).pw_name
                    group = grp.getgrgid(stat_info.st_gid).gr_name
                except (KeyError, ImportError):
                    owner = str(stat_info.st_uid)
                    group = str(stat_info.st_gid)
                
                # Get permissions in octal format
                permissions = oct(stat_info.st_mode)[-3:]
                
                print(f"  - Owner: {owner}")
                print(f"  - Group: {group}")
                print(f"  - Permissions: {permissions}")
                
                # Check if postgres user can access
                try:
                    result = subprocess.run(['sudo', '-u', 'postgres', 'test', '-r', path], 
                                          capture_output=True, check=True)
                    self.print_status(Colors.GREEN, "✓ PostgreSQL user has read access")
                except subprocess.CalledProcessError:
                    self.print_status(Colors.YELLOW, "⚠ PostgreSQL user may not have proper access")
                    
        except Exception as e:
            self.print_status(Colors.YELLOW, f"⚠ Could not check permissions: {e}")
            
    def get_xray_tablespace_info(self):
        """Get detailed information about the XRAY tablespace specified in XRAY_PG_DB_TS"""
        if not self.db_tablespace:
            self.print_status(Colors.YELLOW, "⚠ XRAY_PG_DB_TS not specified - no specific tablespace to analyze")
            return
            
        query = f"""
        SELECT 
            spcname as tablespace_name,
            pg_tablespace_location(oid) as location,
            oid as tablespace_oid,
            (SELECT count(*) FROM pg_database WHERE dattablespace = t.oid) as databases_using,
            (SELECT count(*) FROM pg_class WHERE reltablespace = t.oid) as objects_using
        FROM pg_tablespace t
        WHERE spcname = '{self.db_tablespace}';
        """
        
        result = self.execute_postgres_query(query)
        if result and len(result) > 0:
            row = result[0]
            tablespace_name = row[0]
            location = row[1] if row[1] else 'Default PostgreSQL data directory'
            databases_count = row[3]
            objects_count = row[4]
            
            self.print_status(Colors.GREEN, f"✓ Found XRAY tablespace: {tablespace_name}")
            print(f"  📁 Tablespace: {tablespace_name}")
            print(f"     Location: {location}")
            print(f"     Databases using: {databases_count}")
            print(f"     Objects using: {objects_count}")
            
            # Get detailed filesystem info
            if row[1]:  # Custom location
                try:
                    location_path = Path(location)
                    if location_path.exists():
                        print()
                        self.print_status(Colors.BLUE, f"Detailed Filesystem Analysis for {tablespace_name}:")
                        
                        # Get comprehensive filesystem information
                        self.get_filesystem_usage(location)
                        self.get_directory_info(location)
                        self.check_directory_permissions(location)
                        
                        # Get tablespace-specific database objects
                        self.get_tablespace_objects_info(tablespace_name)
                        
                    else:
                        self.print_status(Colors.RED, f"✗ Tablespace location not accessible: {location}")
                        
                except Exception as e:
                    self.print_status(Colors.YELLOW, f"⚠ Error analyzing tablespace location: {e}")
            else:
                self.print_status(Colors.YELLOW, f"⚠ Tablespace {tablespace_name} uses default location")
                
        else:
            self.print_status(Colors.RED, f"✗ XRAY tablespace '{self.db_tablespace}' not found!")
            print("Available tablespaces:")
            self.list_available_tablespaces()
            
    def get_tablespace_objects_info(self, tablespace_name: str):
        """Get information about objects in the specified tablespace"""
        print()
        self.print_status(Colors.BLUE, f"Objects in Tablespace '{tablespace_name}':")
        
        # Get tables in this tablespace
        tables_query = f"""
        SELECT 
            n.nspname as schema_name,
            c.relname as table_name,
            pg_size_pretty(pg_total_relation_size(c.oid)) as size,
            c.reltuples::bigint as est_rows
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE t.spcname = '{tablespace_name}'
        AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 10;
        """
        
        result = self.execute_postgres_query(query=tables_query, database=self.db_name)
        if result and len(result) > 0:
            print("  Top 10 Tables:")
            for row in result:
                schema, table, size, rows = row
                print(f"    - {schema}.{table}: {size} (~{rows:,} rows)")
        else:
            print("  - No tables found in this tablespace")
            
        # Get indexes in this tablespace
        indexes_query = f"""
        SELECT 
            n.nspname as schema_name,
            c.relname as index_name,
            pg_size_pretty(pg_relation_size(c.oid)) as size,
            tc.relname as table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_tablespace t ON t.oid = c.reltablespace
        LEFT JOIN pg_index i ON i.indexrelid = c.oid
        LEFT JOIN pg_class tc ON tc.oid = i.indrelid
        WHERE t.spcname = '{tablespace_name}'
        AND c.relkind = 'i'
        ORDER BY pg_relation_size(c.oid) DESC
        LIMIT 5;
        """
        
        result = self.execute_postgres_query(query=indexes_query, database=self.db_name)
        if result and len(result) > 0:
            print("  Top 5 Indexes:")
            for row in result:
                schema, index, size, table = row
                print(f"    - {schema}.{index} on {table}: {size}")
        else:
            print("  - No indexes found in this tablespace")
            
    def list_available_tablespaces(self):
        """List all available tablespaces for troubleshooting"""
        query = """
        SELECT 
            spcname as tablespace_name,
            pg_tablespace_location(oid) as location
        FROM pg_tablespace
        ORDER BY spcname;
        """
        
        result = self.execute_postgres_query(query)
        if result and len(result) > 0:
            for row in result:
                tablespace_name = row[0]
                location = row[1] if row[1] else 'Default location'
                print(f"  - {tablespace_name}: {location}")
        else:
            print("  - Could not retrieve tablespace list")
        
    def get_postgres_config(self):
        """Get PostgreSQL configuration"""
        query = """
        SELECT 
            name,
            setting,
            unit,
            context
        FROM pg_settings 
        WHERE name IN (
            'data_directory',
            'wal_level',
            'archive_mode',
            'archive_command',
            'max_wal_size',
            'min_wal_size',
            'checkpoint_completion_target',
            'shared_buffers',
            'effective_cache_size',
            'work_mem',
            'maintenance_work_mem'
        )
        ORDER BY name;
        """
        
        result = self.execute_postgres_query(query)
        if result:
            self.print_status(Colors.BLUE, "Key PostgreSQL Settings:")
            for row in result:
                name, setting, unit, context = row
                if unit and unit.strip():
                    print(f"  - {name}: {setting} {unit} ({context})")
                else:
                    print(f"  - {name}: {setting} ({context})")
                    
    def get_data_directory(self) -> Optional[str]:
        """Get PostgreSQL data directory"""
        if not self.pg_connection:
            self.pg_connection = self.connect_to_postgres()
            
        if self.pg_connection:
            try:
                with self.pg_connection.cursor() as cursor:
                    cursor.execute("SHOW data_directory;")
                    result = cursor.fetchone()
                    return result[0] if result else None
            except psycopg2.Error:
                pass
                
        # Fallback to query method
        result = self.execute_postgres_query("SHOW data_directory;")
        if result and len(result) > 0:
            return result[0][0]
        return None
        
    def check_wal_directory(self, data_dir: str):
        """Check WAL directory information"""
        if not data_dir:
            return
            
        print()
        self.print_status(Colors.BLUE, "PostgreSQL Data and WAL Locations:")
        print(f"  - Data Directory: {data_dir}")
        print(f"  - WAL Directory: {data_dir}/pg_wal")
        
        wal_path = Path(data_dir) / "pg_wal"
        
        try:
            if wal_path.is_symlink():
                wal_target = str(wal_path.resolve())
                print(f"  - WAL Symlink Target: {wal_target}")
                
                if Path(wal_target).exists():
                    wal_size = self.get_directory_size(wal_target)
                    wal_files = len(list(Path(wal_target).glob("0*")))
                    print(f"  - WAL Size: {wal_size}")
                    print(f"  - WAL Files: {wal_files}")
                    self.print_status(Colors.GREEN, "✓ WAL directory accessible (symlink)")
                else:
                    self.print_status(Colors.YELLOW, "⚠ WAL symlink target not accessible")
                    
            elif wal_path.exists():
                wal_size = self.get_directory_size(str(wal_path))
                wal_files = len(list(wal_path.glob("0*")))
                print(f"  - WAL Size: {wal_size}")
                print(f"  - WAL Files: {wal_files}")
                self.print_status(Colors.GREEN, "✓ WAL directory accessible (regular directory)")
            else:
                self.print_status(Colors.YELLOW, "⚠ Cannot access WAL directory (permission or path issue)")
                
        except PermissionError:
            self.print_status(Colors.YELLOW, "⚠ Cannot access WAL directory (permission denied)")
            
    def get_directory_size(self, path: str) -> str:
        """Get human-readable directory size"""
        try:
            result = subprocess.run(['sudo', 'du', '-sh', path], 
                                  capture_output=True, text=True, check=True)
            return result.stdout.split()[0]
        except subprocess.CalledProcessError:
            return "Unknown"
            
    def get_table_info(self):
        """Get database table information"""
        if self.use_exact_counts:
            # Use actual disk size calculations
            size_query = """
            pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_table_size,
            SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total_size_bytes
            """
            table_size_col = "pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))"
            rows_col = "COUNT(*)"
            rows_label = "Exact Rows"
        else:
            # Use estimated sizes from pg_class statistics
            size_query = """
            pg_size_pretty(
                SUM(COALESCE(relpages, 0) * 8192)
            ) as total_table_size,
            SUM(COALESCE(relpages, 0) * 8192) as total_size_bytes
            """
            table_size_col = "pg_size_pretty(COALESCE(c.relpages, 0) * 8192)"
            rows_col = "c.reltuples::bigint"
            rows_label = "Est. Rows"
            
        if self.use_exact_counts:
            query = f"""
            SELECT 
                COUNT(*) as table_count,
                {size_query}
            FROM pg_tables 
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog');
            """
        else:
            query = f"""
            SELECT 
                COUNT(*) as table_count,
                {size_query}
            FROM pg_tables t
            JOIN pg_class c ON c.relname = t.tablename
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
            WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog');
            """
        
        result = self.execute_postgres_query(query, self.db_name)
        if result and len(result) > 0:
            row = result[0]
            size_type = "Actual" if self.use_exact_counts else "Estimated"
            print(f"  - Total Tables: {row[0]}")
            print(f"  - Total Table Size ({size_type}): {row[1]}")
            print(f"  - Size in Bytes: {row[2]}")
            
            # Show top 5 largest tables
            print()
            self.print_status(Colors.BLUE, f"Top 5 Largest Tables ({size_type} sizes):")
            
            if self.use_exact_counts:
                table_query = """
                SELECT 
                    schemaname as "Schema",
                    tablename as "Table Name",
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as "Size",
                    (SELECT reltuples::bigint FROM pg_class WHERE relname = tablename) as "Est. Rows"
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                LIMIT 5;
                """
            else:
                table_query = """
                SELECT 
                    t.schemaname as "Schema",
                    t.tablename as "Table Name",
                    pg_size_pretty(COALESCE(c.relpages, 0) * 8192) as "Size",
                    c.reltuples::bigint as "Est. Rows"
                FROM pg_tables t
                JOIN pg_class c ON c.relname = t.tablename
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
                WHERE t.schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY COALESCE(c.relpages, 0) DESC
                LIMIT 5;
                """
            
            if not self.execute_postgres_formatted_query(table_query, self.db_name):
                self.print_status(Colors.YELLOW, "⚠ Could not retrieve table information")
                
    def get_connection_info(self):
        """Get connection and activity information"""
        query = f"""
        SELECT 
            (SELECT setting FROM pg_settings WHERE name = 'max_connections') as max_connections,
            COUNT(*) as current_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active_connections,
            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
            COUNT(*) FILTER (WHERE datname = '{self.db_name}') as db_connections
        FROM pg_stat_activity;
        """
        
        result = self.execute_postgres_query(query)
        if result and len(result) > 0:
            row = result[0]
            print(f"  - Max Connections: {row[0]}")
            print(f"  - Current Connections: {row[1]}")
            print(f"  - Active Connections: {row[2]}")
            print(f"  - Idle Connections: {row[3]}")
            print(f"  - Connections to {self.db_name}: {row[4]}")
            
    def get_database_statistics(self):
        """Get database performance statistics"""
        self.print_status(Colors.BLUE, "Database Performance Metrics:")
        
        stats_query = f"""
        SELECT 
            'Active Backends' as metric, numbackends::text as value
        FROM pg_stat_database WHERE datname = '{self.db_name}'
        UNION ALL
        SELECT 
            'Transactions Committed', xact_commit::text
        FROM pg_stat_database WHERE datname = '{self.db_name}'
        UNION ALL
        SELECT 
            'Transactions Rolled Back', xact_rollback::text
        FROM pg_stat_database WHERE datname = '{self.db_name}'
        UNION ALL
        SELECT 
            'Cache Hit Ratio (%)', 
            ROUND((blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100, 2)::text
        FROM pg_stat_database WHERE datname = '{self.db_name}'
        UNION ALL
        SELECT 
            'Tuples Returned', tup_returned::text
        FROM pg_stat_database WHERE datname = '{self.db_name}'
        UNION ALL
        SELECT 
            'Tuples Inserted', tup_inserted::text
        FROM pg_stat_database WHERE datname = '{self.db_name}';
        """
        
        if not self.execute_postgres_formatted_query(stats_query, self.db_name):
            self.print_status(Colors.YELLOW, "⚠ Could not retrieve database statistics")
            
    def get_tablespace_mountpoint(self, tablespace_name: str) -> str:
        """Get the mountpoint for a tablespace"""
        if tablespace_name == 'pg_default':
            # For default tablespace, use data directory
            data_dir = self.get_data_directory()
            if data_dir:
                try:
                    result = subprocess.run(['df', data_dir], capture_output=True, text=True, check=True)
                    lines = result.stdout.strip().split('\n')
                    if len(lines) >= 2:
                        data = lines[1].split()
                        if len(data) >= 6:
                            return ' '.join(data[5:])  # Mount point
                except subprocess.CalledProcessError:
                    pass
            return "Unknown"
        else:
            # For custom tablespace, get its location and find mountpoint
            query = f"""
            SELECT pg_tablespace_location(oid) as location
            FROM pg_tablespace 
            WHERE spcname = '{tablespace_name}';
            """
            result = self.execute_postgres_query(query)
            if result and len(result) > 0 and result[0][0]:
                location = result[0][0]
                try:
                    result = subprocess.run(['df', location], capture_output=True, text=True, check=True)
                    lines = result.stdout.strip().split('\n')
                    if len(lines) >= 2:
                        data = lines[1].split()
                        if len(data) >= 6:
                            return ' '.join(data[5:])  # Mount point
                except subprocess.CalledProcessError:
                    pass
            return "Unknown"

    def print_summary(self, actual_tablespace: str, data_dir: str):
        """Print configuration summary"""
        self.print_section("Configuration Summary")
        self.print_status(Colors.GREEN, "✓ Database configuration analysis complete")
        print()
        self.print_status(Colors.BLUE, "Quick Summary:")
        print(f"  - Database: {self.db_name}")
        
        # Check database status
        query = f"SELECT datname FROM pg_database WHERE datname = '{self.db_name}';"
        result = self.execute_postgres_query(query)
        status = "EXISTS" if result and len(result) > 0 else "MISSING"
        print(f"  - Status: {status}")
        
        print(f"  - Tablespace: {actual_tablespace}")
        
        # Add tablespace mountpoint
        mountpoint = self.get_tablespace_mountpoint(actual_tablespace)
        print(f"  - Tablespace Mountpoint: {mountpoint}")
        
        print(f"  - Data Directory: {data_dir}")
        print(f"  - WAL Directory: {data_dir}/pg_wal")
        
        # Get shared_buffers setting
        shared_buffers_query = "SHOW shared_buffers;"
        result = self.execute_postgres_query(shared_buffers_query)
        shared_buffers = result[0][0] if result and len(result) > 0 else "Unknown"
        print(f"  - Shared Buffers: {shared_buffers}")
        
        # Check WAL symlink
        wal_path = Path(data_dir) / "pg_wal"
        if wal_path.is_symlink():
            try:
                wal_target = str(wal_path.resolve())
                print(f"  - WAL Target: {wal_target}")
            except:
                print(f"  - WAL Target: Unknown")
                
        # Tablespace match check
        if self.db_tablespace:
            print(f"  - Expected Tablespace: {self.db_tablespace}")
            if actual_tablespace == self.db_tablespace:
                print("  - Tablespace Match: ✓ YES")
            else:
                print("  - Tablespace Match: ✗ NO")
                
    def run(self):
        """Main execution function"""
        try:
            self.print_header()
            
            # Show mode information
            mode = "Exact Counts" if self.use_exact_counts else "Estimates"
            self.print_status(Colors.BLUE, f"Analysis Mode: {Colors.BOLD}{mode}{Colors.NC}")
            if not self.use_exact_counts:
                self.print_status(Colors.YELLOW, "Using estimates for faster performance. Use --exact-counts for precise measurements.")
            
            # Load environment variables
            if not self.load_xray_environment():
                sys.exit(1)
                
            print()
            self.print_status(Colors.BLUE, f"Target Database: {Colors.BOLD}{self.db_name}{Colors.NC}")
            if self.db_tablespace:
                self.print_status(Colors.BLUE, f"Expected Tablespace: {Colors.BOLD}{self.db_tablespace}{Colors.NC}")
                
            # Check PostgreSQL service status
            self.print_section("PostgreSQL Service Status")
            if not self.check_postgres_running():
                sys.exit(1)
                
            # Get PostgreSQL version
            version = self.get_postgres_version()
            self.print_status(Colors.BLUE, f"PostgreSQL Version: {version}")
            
            # Check database existence
            self.print_section("Database Existence and Basic Info")
            if not self.check_database_exists():
                sys.exit(1)
                
            # Get database basic information
            self.get_database_info()
            
            # Get tablespace information
            self.print_section("Database Tablespace Configuration")
            actual_tablespace = self.get_tablespace_info()
            
            # Show XRAY tablespace details if specified
            if self.db_tablespace:
                self.print_section(f"XRAY Tablespace Details: {self.db_tablespace}")
                self.get_xray_tablespace_info()
            
            # Get PostgreSQL configuration
            self.print_section("PostgreSQL Configuration and WAL Location")
            self.get_postgres_config()
            
            # Get data directory and WAL info
            data_dir = self.get_data_directory()
            if data_dir:
                self.check_wal_directory(data_dir)
                
            # Get table information
            self.print_section("Database Table Information")
            self.get_table_info()
            
            # Get connection information
            self.print_section("Connection and Activity Information")
            self.get_connection_info()
            
            # Get database statistics
            self.print_section("Database Statistics and Performance")
            self.get_database_statistics()
            
            # Print summary
            self.print_summary(actual_tablespace or "Unknown", data_dir or "Unknown")
            
            print()
            self.print_status(Colors.CYAN, "Configuration analysis completed successfully!")
            
        finally:
            # Always close connections
            self.close_connections()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="XRAY PostgreSQL Database Configuration Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Run with estimates (fast)
  %(prog)s --exact-counts     # Run with exact calculations (slower but precise)
  %(prog)s -e                 # Short form for exact counts

Performance Notes:
  - Default mode uses PostgreSQL statistics for fast estimates
  - Exact mode calculates actual disk usage and row counts (slower on large databases)
  - Estimates are usually sufficient for configuration analysis
        """
    )
    
    parser.add_argument(
        '--exact-counts', '-e',
        action='store_true',
        help='Use exact calculations instead of estimates (slower but more precise)'
    )
    
    parser.add_argument(
        '--env-file',
        default='/home/nutanix/bin/xray_set_env',
        help='Path to XRAY environment file (default: /home/nutanix/bin/xray_set_env)'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='XRAY DB Config Analyzer v2.0 (Python)'
    )
    
    return parser.parse_args()

def main():
    """Main entry point"""
    try:
        args = parse_arguments()
        config_checker = XrayDBConfig(use_exact_counts=args.exact_counts, xray_env_file=args.env_file)
        config_checker.run()
        sys.exit(0)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()