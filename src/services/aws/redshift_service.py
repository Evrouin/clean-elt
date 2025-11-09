import os
from contextlib import contextmanager
from typing import Any, Dict, List

import boto3
import psycopg2
import psycopg2.pool

from src.utils.logger import StructuredLogger


class RedshiftService:
    """Service for Redshift database operations and COPY commands"""

    def __init__(self, cluster_endpoint: str = None, database: str = None, user: str = None, port: int = 5439):
        self.logger = StructuredLogger(__name__)
        self.cluster_endpoint = cluster_endpoint or os.getenv('REDSHIFT_HOST')
        self.database = database or os.getenv('REDSHIFT_DATABASE')
        self.user = user or os.getenv('REDSHIFT_USER')
        self.port = port
        self._connection_pool = None

    def _get_connection_pool(self):
        """Get or create connection pool for better resource management"""
        if self._connection_pool is None:
            try:
                redshift_client = boto3.client('redshift')
                cluster_identifier = self.cluster_endpoint.split('.')[0]

                response = redshift_client.get_cluster_credentials(
                    ClusterIdentifier=cluster_identifier,
                    DbUser=self.user,
                    DbName=self.database,
                    AutoCreate=False
                )

                self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=self.cluster_endpoint,
                    port=self.port,
                    database=self.database,
                    user=response['DbUser'],
                    password=response['DbPassword']
                )

                self.logger.info("Redshift connection pool created")

            except Exception as e:
                self.logger.error(f"Failed to create connection pool: {e}")
                raise

        return self._connection_pool

    @contextmanager
    def get_connection(self):
        """Context manager for connection handling"""
        pool = self._get_connection_pool()
        conn = pool.getconn()
        try:
            yield conn
        finally:
            pool.putconn(conn)

    def execute_copy_command(self, table_name: str, s3_path: str, iam_role: str, copy_options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute COPY command to load data from S3 to Redshift"""

        start_time = self.logger.get_timestamp()

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                self.logger.info(f"Executing COPY command for table {table_name}")

                source_file = s3_path.split('/')[-1]

                if 'sales' in table_name:
                    copy_sql = f"""
                    COPY {table_name} (transaction_id, transaction_date, customer_id, product_id, quantity, unit_price, total_amount, payment_method)
                    FROM '{s3_path}'
                    IAM_ROLE '{iam_role}'
                    DELIMITER ','
                    IGNOREHEADER 1
                    REMOVEQUOTES
                    EMPTYASNULL
                    BLANKSASNULL
                    COMPUPDATE OFF
                    STATUPDATE OFF
                    PARALLEL ON
                    MAXERROR 1000
                    """
                elif 'inventory' in table_name:
                    copy_sql = f"""
                    COPY {table_name} (product_id, product_name, category, quantity_on_hand, reorder_level, last_updated, cost_price, status)
                    FROM '{s3_path}'
                    IAM_ROLE '{iam_role}'
                    DELIMITER ','
                    IGNOREHEADER 1
                    REMOVEQUOTES
                    EMPTYASNULL
                    BLANKSASNULL
                    TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS'
                    COMPUPDATE OFF
                    STATUPDATE OFF
                    PARALLEL ON
                    MAXERROR 1000
                    """
                elif 'expense' in table_name:
                    copy_sql = f"""
                    COPY {table_name} (expense_id, expense_date, category, amount, description, approved_by, justification)
                    FROM '{s3_path}'
                    IAM_ROLE '{iam_role}'
                    DELIMITER ','
                    IGNOREHEADER 1
                    REMOVEQUOTES
                    EMPTYASNULL
                    BLANKSASNULL
                    COMPUPDATE OFF
                    STATUPDATE OFF
                    PARALLEL ON
                    MAXERROR 1000
                    """
                else:
                    raise ValueError(f"Unknown table type: {table_name}")

                cursor.execute("BEGIN")
                cursor.execute(copy_sql)

                cursor.execute("""
                    SELECT query, rows, bytes, starttime, endtime
                    FROM stl_load_commits
                    WHERE query = pg_last_copy_id()
                """)

                load_stats = cursor.fetchone()

                cursor.execute(f"UPDATE {table_name} SET source_file = '{source_file}' WHERE source_file IS NULL")
                cursor.execute("COMMIT")
                cursor.close()

                end_time = self.logger.get_timestamp()

                result = {
                    'status': 'SUCCESS',
                    'table_name': table_name,
                    's3_path': s3_path,
                    'rows_loaded': load_stats[1] if load_stats else 0,
                    'bytes_loaded': load_stats[2] if load_stats else 0,
                    'duration_ms': (end_time - start_time) * 1000,
                    'copy_command': copy_sql.strip()
                }

                self.logger.info("COPY command completed successfully", **result)
                return result

        except Exception as e:
            self.logger.error(f"COPY command failed: {e}")
            return {
                'status': 'FAILED',
                'table_name': table_name,
                's3_path': s3_path,
                'error': str(e),
                'duration_ms': (self.logger.get_timestamp() - start_time) * 1000,
                'copy_command': copy_sql.strip() if 'copy_sql' in locals() else 'N/A'
            }

    def execute_query(self, query: str, params: List = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(query, params)

                if cursor.description is None:
                    conn.commit()
                    cursor.close()
                    return []

                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(columns, row)) for row in rows]

                cursor.close()
                return results

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def execute_manifest_copy(self, table_name: str, manifest_s3_path: str, iam_role: str) -> Dict[str, Any]:
        """Execute COPY using manifest file for batch loading"""

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                copy_sql = f"""
                COPY {table_name}
                FROM '{manifest_s3_path}'
                IAM_ROLE '{iam_role}'
                MANIFEST
                DELIMITER ','
                IGNOREHEADER 1
                REMOVEQUOTES
                EMPTYASNULL
                BLANKSASNULL
                COMPUPDATE OFF
                STATUPDATE OFF
                PARALLEL ON
                """

                cursor.execute(copy_sql)

                cursor.execute("""
                    SELECT SUM(rows), SUM(bytes), COUNT(*)
                    FROM stl_load_commits
                    WHERE query >= pg_last_copy_id() - 10
                """)

                stats = cursor.fetchone()
                cursor.close()

                return {
                    'status': 'SUCCESS',
                    'table_name': table_name,
                    'manifest_path': manifest_s3_path,
                    'total_rows': stats[0] if stats else 0,
                    'total_bytes': stats[1] if stats else 0,
                    'files_processed': stats[2] if stats else 0
                }

        except Exception as e:
            self.logger.error(f"Manifest COPY failed: {e}")
            return {
                'status': 'FAILED',
                'table_name': table_name,
                'manifest_path': manifest_s3_path,
                'error': str(e)
            }

    def close_connection(self):
        """Close Redshift connection pool"""
        if self._connection_pool:
            try:
                self._connection_pool.closeall()
                self._connection_pool = None
                self.logger.info("Redshift connection pool closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection pool: {e}")
