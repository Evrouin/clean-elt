import boto3
import psycopg2
import os
from typing import Dict, Any, List, Optional
from src.utils.logger import StructuredLogger


class RedshiftService:
    """Service for Redshift database operations and COPY commands"""

    def __init__(self, cluster_endpoint: str = None, database: str = None, user: str = None, port: int = 5439):
        self.logger = StructuredLogger(__name__)
        self.cluster_endpoint = cluster_endpoint or os.getenv('REDSHIFT_CLUSTER_ENDPOINT')
        self.database = database or os.getenv('REDSHIFT_DATABASE')
        self.user = user or os.getenv('REDSHIFT_USER')
        self.port = port
        self._connection = None

    def get_connection(self):
        """Get or create Redshift connection using temporary credentials"""
        if self._connection is None:
            try:
                # Get temporary credentials using boto3
                redshift_client = boto3.client('redshift')
                cluster_identifier = self.cluster_endpoint.split('.')[0]
                
                response = redshift_client.get_cluster_credentials(
                    ClusterIdentifier=cluster_identifier,
                    DbUser=self.user,
                    DbName=self.database,
                    AutoCreate=False
                )
                
                self._connection = psycopg2.connect(
                    host=self.cluster_endpoint,
                    port=self.port,
                    database=self.database,
                    user=response['DbUser'],
                    password=response['DbPassword']
                )

                self.logger.info("Redshift connection established")

            except Exception as e:
                self.logger.error(f"Failed to connect to Redshift: {e}")
                raise

        return self._connection

    def execute_copy_command(self, table_name: str, s3_path: str, iam_role: str,
                           copy_options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute COPY command to load data from S3 to Redshift - simple and efficient"""

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            self.logger.info(f"Executing COPY command for table {table_name}")

            source_file = s3_path.split('/')[-1]

            if 'sales' in table_name:
                # CSV: transaction_id,date,customer_id,item_id,quantity,unit_price,total_amount,payment_method
                copy_sql = f"""
                COPY {table_name} (transaction_id, transaction_date, customer_id, product_id, quantity, unit_price, total_amount, payment_method)
                FROM '{s3_path}'
                IAM_ROLE '{iam_role}'
                DELIMITER ','
                IGNOREHEADER 1
                REMOVEQUOTES
                EMPTYASNULL
                BLANKSASNULL
                """
            elif 'inventory' in table_name:
                # CSV: item_id,item_name,category,quantity_on_hand,reorder_level,last_updated,cost_price,status
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
                """
            elif 'expense' in table_name:
                # CSV: expense_id,date,category,amount,description,approved_by,justification
                copy_sql = f"""
                COPY {table_name} (expense_id, expense_date, category, amount, description, approved_by, justification)
                FROM '{s3_path}'
                IAM_ROLE '{iam_role}'
                DELIMITER ','
                IGNOREHEADER 1
                REMOVEQUOTES
                EMPTYASNULL
                BLANKSASNULL
                """
            else:
                raise ValueError(f"Unknown table type: {table_name}")

            cursor.execute(copy_sql)

            cursor.execute(f"UPDATE {table_name} SET source_file = '{source_file}' WHERE source_file IS NULL")

            conn.commit()

            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE source_file = '{source_file}'")
            rows_loaded = cursor.fetchone()[0]

            cursor.close()

            result = {
                'status': 'SUCCESS',
                'table_name': table_name,
                's3_path': s3_path,
                'rows_loaded': rows_loaded,
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
                'copy_command': copy_sql.strip() if 'copy_sql' in locals() else 'N/A'
            }

    def execute_query(self, query: str, params: List = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            conn = self.get_connection()
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

    def close_connection(self):
        """Close Redshift connection"""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                self.logger.info("Redshift connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
