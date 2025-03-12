import psycopg2
from psycopg2 import sql
import os

class PostgresConnection:
    def __init__(self, hostname, database, username, password, port='5432'):
        """Initialize the PostgreSQL connection parameters."""
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection to PostgreSQL established successfully.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise

    def execute_query(self, query, params=None):
        """Execute a SQL query and return the results."""
        if self.cursor:
            try:
                self.cursor.execute(query, params)
                if query.strip().lower().startswith("select"):
                    return self.cursor.fetchall()
                elif query.strip().lower().startswith("insert"):
                    # Get the ID of the inserted row
                    self.connection.commit()
                    return self.cursor.fetchone()[0]  # Fetch the returned id
                else:
                    self.connection.commit()
            except Exception as e:
                print(f"Error executing query: {e}")
                raise

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("PostgreSQL connection closed.")
