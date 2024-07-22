# Python_data_migration_between_two_mssql_servers
  a python class to manage data migration from one source to another source


- The DataMigration class uses pyodbc to connect to SQL Server databases.
- The connect_to_source and connect_to_dest methods establish connections to the source and destination databases using the configurations provided.
- The fetch_data_from_source method fetches data from the source database.
- The insert_data_to_dest method inserts fetched data into the destination database. The columns parameter is used to specify the columns in the destination table.
- The migrate_data method manages the entire migration process, ensuring connections are closed after completion.
- Replace the placeholder values for server, database, username, password, and column names with the actual values for your databases.
