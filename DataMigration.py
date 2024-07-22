import pyodbc

class DataMigration:
    def __init__(self, source_config, dest_config):
        self.source_config = source_config
        self.dest_config = dest_config
        self.source_conn = None
        self.dest_conn = None

    def connect_to_source(self):
        try:
            self.source_conn = pyodbc.connect(
                f"DRIVER={{SQL Server}};SERVER={self.source_config['server']};DATABASE={self.source_config['database']};UID={self.source_config['username']};PWD={self.source_config['password']}"
            )
            print("Connected to source database")
        except pyodbc.Error as e:
            print(f"Error connecting to source database: {e}")
            raise

    def connect_to_dest(self):
        try:
            self.dest_conn = pyodbc.connect(
                f"DRIVER={{SQL Server}};SERVER={self.dest_config['server']};DATABASE={self.dest_config['database']};UID={self.dest_config['username']};PWD={self.dest_config['password']}"
            )
            print("Connected to destination database")
        except pyodbc.Error as e:
            print(f"Error connecting to destination database: {e}")
            raise

    def fetch_data_from_source(self, query):
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
                print("Data fetched from source")
                return data
        except pyodbc.Error as e:
            print(f"Error fetching data from source: {e}")
            raise

    def insert_data_to_dest(self, table, columns, data):
        try:
            with self.dest_conn.cursor() as cursor:
                placeholders = ', '.join(['?'] * len(columns))
                insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                cursor.executemany(insert_query, data)
                self.dest_conn.commit()
                print("Data inserted into destination")
        except pyodbc.Error as e:
            print(f"Error inserting data into destination: {e}")
            self.dest_conn.rollback()
            raise

    def migrate_data(self, source_query, dest_table, dest_columns):
        try:
            self.connect_to_source()
            self.connect_to_dest()
            data = self.fetch_data_from_source(source_query)
            if data:
                self.insert_data_to_dest(dest_table, dest_columns, data)
            else:
                print("No data to migrate")
        finally:
            if self.source_conn:
                self.source_conn.close()
            if self.dest_conn:
                self.dest_conn.close()

# Example usage
source_config = {
    'server': 'source_server',
    'database': 'source_db',
    'username': 'source_user',
    'password': 'source_password'
}

dest_config = {
    'server': 'dest_server',
    'database': 'dest_db',
    'username': 'dest_user',
    'password': 'dest_password'
}

source_query = "SELECT * FROM source_table"
dest_table = "dest_table"
dest_columns = ['column1', 'column2', 'column3']  # Replace with actual column names

migration = DataMigration(source_config, dest_config)
migration.migrate_data(source_query, dest_table, dest_columns)
