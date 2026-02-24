import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config=None):
        self.config = config
        self.connection_pool = None

    def initialize_with_config(self, config):
        self.config = config
        self._init_connection_pool()

    def _init_connection_pool(self):
        try:
            connection_kwargs = {
                'host': self.config.DB_HOST,
                'port': self.config.DB_PORT,
                'database': self.config.DB_NAME,
                'user': self.config.DB_USER,
                'password': self.config.DB_PASSWORD,
                'cursor_factory': RealDictCursor,
                'client_encoding': 'UTF8',
                'connect_timeout': 30,
                'application_name': 'qualtrics_data_processor'
            }

            logger.info("Testing database connection...")
            test_conn = psycopg2.connect(**connection_kwargs)
            test_conn.close()
            logger.info("Database connection test successful")

            self.connection_pool = ThreadedConnectionPool(
                minconn=self.config.DB_POOL_MIN_CONN,
                maxconn=self.config.DB_POOL_MAX_CONN,
                **connection_kwargs
            )
            logger.info("Database connection pool initialized successfully")

        except psycopg2.OperationalError as e:
            error_msg = f"Failed to connect to database: {e}"
            logger.error(error_msg)
            logger.error(
                f"Connection details: host={self.config.DB_HOST}, port={self.config.DB_PORT}, database={self.config.DB_NAME}, user={self.config.DB_USER}")
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        if not self.connection_pool:
            raise Exception("Database connection pool not initialized")

        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as test_cursor:
                test_cursor.execute("SELECT 1")
            yield conn
        except psycopg2.InterfaceError as e:
            logger.warning(f"Connection lost, attempting to reconnect: {e}")
            if conn:
                try:
                    self.connection_pool.putconn(conn, close=True)
                except:
                    pass
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                try:
                    self.connection_pool.putconn(conn)
                except Exception as e:
                    logger.warning(f"Failed to return connection to pool: {e}")

    @contextmanager
    def get_cursor(self, autocommit=False):
        with self.get_connection() as conn:
            old_autocommit = conn.autocommit
            if autocommit:
                conn.autocommit = True
            try:
                cursor = conn.cursor()
                yield cursor
                if not autocommit:
                    conn.commit()
            except Exception as e:
                if not autocommit:
                    conn.rollback()
                raise
            finally:
                if autocommit:
                    conn.autocommit = old_autocommit

    def test_connection(self):
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                return result['test'] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def close_all_connections(self):
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("All database connections closed")
            except Exception as e:
                logger.error(f"Error closing database connections: {e}")


db_manager = DatabaseManager()