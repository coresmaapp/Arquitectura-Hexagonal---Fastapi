import psycopg2
from psycopg2 import pool

class DatabaseConnectionFactory:
    _connection_pool = None

    @classmethod
    def initialize(cls, minconn: int = 1, maxconn: int = 5):
        if cls._connection_pool is None:
            cls._connection_pool = pool.SimpleConnectionPool(
                minconn, maxconn,
                database="malla",
                user="mallauser",
                password="Mallapassword",
                host="143.198.224.159",
                port="5432"
            )

    @classmethod
    def get_connection(cls):
        if cls._connection_pool is None:
            raise Exception("Connection pool is not initialized")
        return cls._connection_pool.getconn()

    @classmethod
    def release_connection(cls, connection):
        cls._connection_pool.putconn(connection)

    @classmethod
    def close_pool(cls):
        if cls._connection_pool is not None:
            cls._connection_pool.closeall()
            cls._connection_pool = None