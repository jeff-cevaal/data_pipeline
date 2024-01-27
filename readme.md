# data_pipeline

data_pipeline methods
	close				- close database connection
	create_data_transfer_schema	- create the dt schema and tables
	create_schema			- create a schema
	create_system_defaults		- create database defaults and functions
	etl_dataflex_data		- extract, transform & load dataflex data (vld files)
	etl_fox_pro_data		- extract, transform & load fox pro data (dbf files)
	etl_mysql_data			- extract, transform & load mysql data
	etl_spreadsheet_data		- extract, transform & load spreadsheets (xls, xlsx, xlsm, csv files)
	etl_sql_server_data		- extract, transform & load sql server data
	read_sql_file			- read a sql file
	rename_schema			- rename a schema
	sql_count			- row count of a table
	sql_all_records			- gets all records
	sql_refresh_data		- rebuilds the table from a sql create & insert statements
	sql_remove_empty_tables		- finds empty tables and drops them
	sql_next_record			- gets the next record
	sql_queue_exec			- queues all sql commands until the max is reached and then executes them in a batch
	sql_vacuum			- vacuum the database

utilities.py is the support classes used in data_pipeline.py
	array_list 			- extends list
	data_location 			- stores file path and list of files
	debugger			- writes a debug.txt file
	timer				- displays the time a process took to complete

dt_system.py is for initializing the default system database

login.toml is where the credentials are for accessing database connections.
