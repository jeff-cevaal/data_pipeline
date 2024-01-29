# standard library imports
#import csv			# inside etl_spreadsheet_data()
import decimal
import os
import re
import tomllib
import time
#from binascii import hexlify	# inside __transform_byte_data()
from datetime import datetime

# third party library imports
# import dbfread	# inside etl_fox_pro_data()
# import psycopg2	# inside __init__()
# import psycopg2	# inside __init__() - for 32-bit python
# import pyexcel	# inside etl_spreadsheet_data()
# import pyodbc		# inside etl_dataflex_data() dataflex's odbc driver is 32-bit only - reason 32-bit python is needed
# import pymssql	# inside etl_sql_server_data()
# import pymysql	# inside etl_mysql_data()

# local library imports
from utilities import array_list
from utilities import data_location
from utilities import debugger
from utilities import timer


# PostgreSQL database access
class pg_sql:
	# Init Constructor
	def __init__(self, database:str, schema:str='sys', port:str='5432', cloud:bool=False, pg2:bool=True, single_exec:bool=False, etl_debugger:bool=False):
		try:
			# import
			if pg2:
				global psycopg2
				import psycopg2
			else:
				global psycopg
				import psycopg

			# read login file
			with open(os.path.join(os.path.dirname(__file__), 'settings', 'login.toml'), mode='rb') as toml_file:
				self.__login_info = tomllib.load(toml_file)

			# constants
			self.__PG_SERVER = self.__login_info['cloud_pg' if cloud else 'local_pg']['host']
			self.__PG_PORT = self.__login_info['cloud_pg' if cloud else 'local_pg']['port']
			self.__PG_USERNAME = self.__login_info['cloud_pg' if cloud else 'local_pg']['user']
			self.__PG_PASSWORD = self.__login_info['cloud_pg' if cloud else 'local_pg']['password']
			self.__SSL = self.__login_info['cloud_pg' if cloud else 'local_pg']['ssl']
			self.__ENCODING = 'utf-8'
			self.__DT_SCHEMA = 'dt'
			self.__SYS_SCHEMA = 'sys'
			self.__SQL_SCHEMA = 'sql_'
			self.__PG_DEFAULT_DB = 'postgres'
			self.__SQL_STRING_MAXIMUM = 100000000	# larger than 230,000,000 creates a string failure
			# used for translation step
			if single_exec:
				self.__SEMICOLON_MAXIMUM = 1
			# used for debugging on processing - single sql statement from sql_[data_type] & data_source
			elif etl_debugger:
				self.__SEMICOLON_MAXIMUM = 2
			# else normal operation
			else:
				self.__SEMICOLON_MAXIMUM = 500				# to small ~< 100 and to big ~> 800 it becomes slower
			self.__RESERVED_KEYWORDS = array_list()
			
			# variables
			self.__pg_database = database
			self.__pg_schema = schema
			self.__sql_statements = ''
			self.__semicolon_count = 0
			self.__log_queue = array_list()
			self.__log_written = array_list()

			# check if database exists
			if pg2:
				self.__pg_sql_connection = psycopg2.connect(
							host = self.__PG_SERVER,
							port = self.__PG_PORT,
							user = self.__PG_USERNAME,
							password = self.__PG_PASSWORD,
							database = self.__PG_DEFAULT_DB,
							sslmode = self.__SSL)
			else:
				self.__pg_sql_connection = psycopg.connect(
							host = self.__PG_SERVER,
							port = self.__PG_PORT,
							user = self.__PG_USERNAME,
							password = self.__PG_PASSWORD,
							dbname = self.__PG_DEFAULT_DB,
							sslmode = self.__SSL)
			self.__pg_sql_connection.autocommit = True

			# check if database exists or not
			self.__pg_sql_cursor = self.__pg_sql_connection.cursor()
			self.__pg_sql_cursor.execute("SELECT datname from pg_catalog.pg_database where datname = '{0}';".format(database))
			# create database if empty rowcount
			if self.__pg_sql_cursor.rowcount == 0:
				self.__pg_sql_cursor.execute('CREATE database "{0}";'.format(database))

			# get reserved keywords - constant
			self.__pg_sql_cursor.execute("SELECT word from pg_get_keywords() where catcode in ('R', 'T');")
			for keyword in self.__pg_sql_cursor.fetchall():
				self.__RESERVED_KEYWORDS.append(keyword[0])

			# connect to database
			if pg2:
				self.__pg_connection = psycopg2.connect(
							host = self.__PG_SERVER,
							port = self.__PG_PORT,
							user = self.__PG_USERNAME,
							password = self.__PG_PASSWORD,
							database = self.__pg_database,
							sslmode = self.__SSL)
			else:
				self.__pg_connection = psycopg.connect(
							host = self.__PG_SERVER,
							port = self.__PG_PORT,
							user = self.__PG_USERNAME,
							password = self.__PG_PASSWORD,
							dbname = self.__pg_database,
							sslmode = self.__SSL)
			self.__pg_connection.autocommit = True
			self.__pg_cursor = self.__pg_connection.cursor()

			# create sys.log if not exists
			self.__create_log()
		except Exception as error:
			raise error

	#Private
	# Checks if check_string is in log array_lists
	def __check_log(self, check_string:str) -> bool:
		try:
			# check log_written
			for element in self.__log_written:
				if element.startswith(check_string):
					return True

			# check log_queue
			for element in self.__log_queue:
				if element.startswith(check_string):
					return True

			return False
		except Exception as error:
			raise error

	# Creates the schema & table: sys.log
	def __create_log(self):
		try:
			sys_created = False

			# create sys schema
			self.__pg_cursor.execute("SELECT schema_name from information_schema.schemata where schema_name = '{0}';".format(self.__SYS_SCHEMA))
			if self.__pg_cursor.rowcount == 0:
				self.__pg_cursor.execute('CREATE schema if not exists {0};'.format(self.__SYS_SCHEMA))
				sys_created = True
				
			log_created = False
			# create log table
			self.__pg_cursor.execute("SELECT schemaname, relname, n_live_tup from pg_stat_user_tables where schemaname = '{0}' and relname = 'log'".format(self.__SYS_SCHEMA))
			if self.__pg_cursor.rowcount == 0:
				self.__pg_cursor.execute(self._sql_log())
				self.__pg_cursor.execute("DROP schema if exists public cascade;")

			if sys_created:
				self.__log_queue.append('{0}~{1}~{2}~{3}'.format(self.__SYS_SCHEMA, '', 'schema created', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
			if sys_created:
				self.__log_queue.append('{0}~{1}~{2}~{3}'.format(self.__SYS_SCHEMA, 'log', 'table created', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		except Exception as error:
			raise error

	# ETL - Makes create table statements
	def __create_table_statements(self, data_schema:str, table_name:str, sql_create:str, create_sql_schema:bool=True):
		try:
			# sql_[data_type] schema
			if create_sql_schema:
				self.sql_queue_exec(self._sql_schema(data_schema, table_name))

			# data schema
			self.sql_queue_exec(sql_create)
		except Exception as error:
			raise error

	# Drop the schema
	def __drop_schema(self, schema:str):
		try:
			self.__pg_cursor.execute('DROP SCHEMA if exists {0} cascade;'.format(schema))
		except Exception as error:
			raise error

	# ETL - Makes drop table statements
	def __drop_table_statements(self, data_schema:str, table_name:str, create_sql_schema:bool=True) -> str:
		try:
			# sql_[data_type] schema
			if create_sql_schema:
				self.sql_queue_exec("DROP TABLE if exists {0}.{1} cascade;".format(self.__SQL_SCHEMA + data_schema, table_name))
			
			# data schema
			sql_drop = "DROP TABLE if exists {0}.{1} cascade;".format(data_schema, table_name)
			self.sql_queue_exec(sql_drop)
			
			return sql_drop
		except Exception as error:
			raise error
	
	# Executes the sql statements if maximum semicolons or string length
	def __execute_sql_when_maximum(self, sql_line:str):
		try:
			# queue in log array_list
			self.__queue_log(sql_line)
			
			# set sql_statements and semicolon_count if under string maximum or current sql_line is greater than string maximum
			if len(self.__sql_statements) + len(sql_line) <= self.__SQL_STRING_MAXIMUM or (self.__sql_statements == '' and len(sql_line) > self.__SQL_STRING_MAXIMUM):
				self.__sql_statements += sql_line
				self.__semicolon_count += 1
				sql_line = ''

			# semicolon count reaches max constant or string length max then execute sql string
			if self.__semicolon_count == self.__SEMICOLON_MAXIMUM or len(self.__sql_statements) + len(sql_line) > self.__SQL_STRING_MAXIMUM:
				self.__insert_log()
				self.__pg_cursor.execute(self.__sql_statements)
				self.__sql_statements = sql_line
				self.__semicolon_count = 1 if sql_line.endswith(';') else 0
		except Exception as error:
			raise error

	# Initialize another connection for independent data usage
	def __init_pg_sql_connection(self, database:str='postgres', pg2:bool=True):
		try:
			# if already connected to the database
			if str(self.__pg_sql_cursor.connection).count(database) == 0:
				# new connection for getting sql.table data
				if pg2:
					self.__pg_sql_connection = psycopg2.connect(
								host = self.__PG_SERVER,
								port = self.__PG_PORT,
								user = self.__PG_USERNAME,
								password = self.__PG_PASSWORD,
								database = database,
								sslmode = self.__SSL)
				else:
					self.__pg_sql_connection = psycopg.connect(
								host = self.__PG_SERVER,
								port = self.__PG_PORT,
								user = self.__PG_USERNAME,
								password = self.__PG_PASSWORD,
								dbname = database,
								sslmode = self.__SSL)				
				self.__pg_sql_connection.autocommit = True

				# connection -> cursor
				self.__pg_sql_cursor = self.__pg_sql_connection.cursor()
		except Exception as error:
			raise error

	# ETL - Makes insert into statements
	def __insert_into_statements(self, data_schema:str, table_name:str, sql_row:str, create_sql_schema:bool=True):
		try:
			# sql_[data_type] schema
			sql_insert = "INSERT INTO {0}.{1} VALUES ({2});".format(data_schema, table_name, sql_row)
			if create_sql_schema:
				self.sql_queue_exec("INSERT INTO {0}.{1} (sorting, data) VALUES (3, '{2}');".format(self.__SQL_SCHEMA + data_schema, table_name, sql_insert.replace("'", "''")))
			
			# data schema
			self.sql_queue_exec(sql_insert)				
		except Exception as error:
			print(sql_insert)
			raise error

	# Inserts db_schema, db_table, description & current date/time in the log table
	def __insert_log(self):
		try:
			# loop thru log_queue and insert into pg sql
			while len(self.__log_queue) >= 1:
				# pop first record
				log_record = self.__log_queue.pop(0)
				# write popped record into self.__log_written
				self.__log_written.append(log_record)
				# log[0] = schema, log[1] = table, log[2] = description
				log = log_record.split('~')

				# inserts into log table from self.__log_queue
				self.__pg_cursor.execute("INSERT into {0}.log (db_schema, db_table, description, start) values ('{1}', '{2}', '{3}', '{4}')".format(self.__SYS_SCHEMA, log[0], log[1], log[2], log[3]))
		except Exception as error:
			raise error

	# Adds an underscore after the column name if it's a reserved keyword in postgresql
	def __modify_column_name(self, column:str) -> str:
		try:
			# replaces non-alphanumeric characters with an underscore
			column = re.sub('\W+', '_', column)
			
			if self.__RESERVED_KEYWORDS.exists(column):
				return column + '_'
			else:
				return column
		except Exception as error:
			raise error

	# Adds log statements if it doesn't exist in array_list
	def __queue_log(self, sql_statement:str):
		try:
			# creates time_stamp in yyyy-mm-dd hh:mm:ss format
			time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

			# only log sql queries that use a schema.table syntax
			sql_schema_table = re.findall('from \w+\.\w+', str(sql_statement), flags=re.IGNORECASE)
			if len(sql_schema_table) > 1:
				sql_schema = re.sub('from ', '', sql_schema_table[0].split('.')[0], flags=re.IGNORECASE)
				sql_table = sql_schema_table[0].split('.')[1]

				# add to log_queue array_list when sql_schema & sql_table are not empty
				if sql_schema != '' and sql_table != '':
					# create table
					if str(sql_statement).lower().startswith('create table'):
						description = 'table created'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
					# delete from
					elif str(sql_statement).lower().startswith('delete from'):
						description = 'values deleted'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
					# drop table
					elif str(sql_statement).lower().startswith('drop table'):
						description = 'table dropped'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
					# function
					elif str(sql_statement).lower().startswith('drop function'):
						description = 'function dropped'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, 'function created', time_stamp))
					# insert
					elif str(sql_statement).lower().startswith('insert into'):
						description = 'values inserted'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
					# update
					elif str(sql_statement).lower().startswith('update'):
						description = 'values updated'
						if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
							self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
			# renaming schema
			elif len(re.findall('alter schema \w+ rename to \w;', str(sql_statement), flags=re.IGNORECASE)) > 0:
				sql_schema = re.sub('alter schema ', '', str(sql_statement).split(' rename')[0], flags=re.IGNORECASE)
				sql_table = ''
				description = 'schema renamed'
				if self.__check_log('{0}~{1}~{2}'.format(sql_schema, sql_table, description)) == False:
					self.__log_queue.append('{0}~{1}~{2}~{3}'.format(sql_schema, sql_table, description, time_stamp))
		except Exception as error:
			raise error

	# ETL - Insert into sql_[data_schema] drop & create - delayed until sql_[data_type].table is setup
	def __sql_table_setup(self, data_schema:str, table_name:str, sql_drop:str, sql_create:str):
		try:
			# sql_[data_type] schema
			self.sql_queue_exec("INSERT INTO {0}.{1} (sorting, data) VALUES (1, '{2}');".format(self.__SQL_SCHEMA + data_schema, table_name, sql_drop.replace("'", "''")))
			self.sql_queue_exec("INSERT INTO {0}.{1} (sorting, data) VALUES (2, '{2}');".format(self.__SQL_SCHEMA + data_schema, table_name, sql_create.replace("'", "''")), exec=True)
		except Exception as error:
			raise error

	# ETL - Formats byte data for pg_sql
	def __transform_byte_data(self, byte_data:bytes) -> str:
		try:
			from binascii import hexlify

			# converts bytes data into proper hex then into a string
			return "'\\x{0}'::bytea".format(hexlify(byte_data).decode(self.__ENCODING))
		except Exception as error:
			raise error

	# ETL - Formats string data for pg_sql
	def __transform_string_data(self, str_data:str) -> str:
		try:
			# Remove spaces
			str_data = str_data.rstrip()

			# remove hexadecimal values - 20 thru 7E are valid
			str_data = re.sub(r"\x00|\x01|\x02|\x03|\x04|\x05|\x06|\x07|\x08|\x0B|\x0C|\x0E|\x0F", '', str_data, flags=re.IGNORECASE)
			#str_data = re.sub(r"\x09", '\t', str_data, flags=re.IGNORECASE)	# tab
			#str_data = re.sub(r"\x0A", '\n', str_data, flags=re.IGNORECASE)	# newline / line feed
			#str_data = re.sub(r"\x0D", '\r', str_data, flags=re.IGNORECASE)	# carriage return
			str_data = re.sub(r"\x10|\x11|\x12|\x13|\x14|\x15|\x16|\x17|\x18|\x19|\x1A|\x1B|\x1C|\x1D|\x1E|\x1F", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\x5C", '\\\\\\\\', str_data, flags=re.IGNORECASE)	# backslash
			str_data = re.sub(r"\x7F", '', str_data, flags=re.IGNORECASE)		# delete
			str_data = re.sub(r"\x80|\x81|\x82|\x83|\x84|\x85|\x86|\x87|\x88|\x89|\x8A|\x8B|\x8C|\x8D|\x8E|\x8F", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\x90|\x91|\x92|\x93|\x94|\x95|\x96|\x97|\x98|\x99|\x9A|\x9B|\x9C|\x9D|\x9E|\x9F", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xA0|\xA1|\xA2|\xA3|\xA4|\xA5|\xA6|\xA7|\xA8|\xA9|\xAA|\xAB|\xAC|\xAD|\xAE|\xAF", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xB0|\xB1|\xB2|\xB3|\xB4|\xB5|\xB6|\xB7|\xB8|\xB9|\xBA|\xBB|\xBC|\xBD|\xBE|\xBF", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xC0|\xC1|\xC2|\xC3|\xC4|\xC5|\xC6|\xC7|\xC8|\xC9|\xCA|\xCB|\xCC|\xCD|\xCE|\xCF", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xD0|\xD1|\xD2|\xD3|\xD4|\xD5|\xD6|\xD7|\xD8|\xD9|\xDA|\xDB|\xDC|\xDD|\xDE|\xDF", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xE0|\xE1|\xE2|\xE3|\xE4|\xE5|\xE6|\xE7|\xE8|\xE9|\xEA|\xEB|\xEC|\xED|\xEE|\xEF", '', str_data, flags=re.IGNORECASE)
			str_data = re.sub(r"\xF0|\xF1|\xF2|\xF3|\xF4|\xF5|\xF6|\xF7|\xF8|\xF9|\xFA|\xFB|\xFC|\xFD|\xFE|\xFF", '', str_data, flags=re.IGNORECASE)

			# replace single quote with two quotes - quote escaping for postgresql
			str_data = str_data.replace("'", "''")
			
			return str_data
		except Exception as error:
			raise error

	#Protected
	# Returns create table sql statement for ...
	def _sql_data_transfer(self, schema:str) -> str:
		try:
			# billing address
			sql_statement = """
				CREATE table if not exists {0}.billing_address (
						id serial,
						account_num varchar(7) default '',
						branch_num varchar(5) default '',
						ref_num varchar(12) default '',
						street_address_1 text default '',
						street_address_2 text default '',
						city text default '',
						state text default '',
						zip varchar(10) default '',
						country varchar(3) default '',
						inactive boolean default false,
						comments text default '');""".format(schema)

			# contact
			sql_statement += """
				CREATE table if not exists {0}.contact (
						id serial,
						account_num varchar(7) default '',
						address_num numeric(4,0) default 0,
						branch_num varchar(5) default '',
						company_name text default '',
						first_name text default '',
						last_name text default '',
						attention_line text default '',
						title text default '',
						gender varchar(1) default '',
						inactive boolean default false,
						comments text default '');""".format(schema)

			# email
			sql_statement += """
				CREATE table if not exists {0}.email (
						id serial,
						account_num varchar(7) default '',
						address_num numeric(4,0) default 0,
						branch_num varchar(5) default '',
						email text default '',
						billing boolean default false,
						receiving boolean default false,
						inactive boolean default false,
						comments text default '');""".format(schema)

			# phone
			sql_statement += """
				CREATE table if not exists {0}.phone (
						id serial,
						account_num varchar(7) default '',
						address_num numeric(4,0) default 0,
						branch_num varchar(5) default '',
						phone text default '',
						ext text default '',
						type text default '',
						texting boolean default false,
						autodial boolean default false,
						inactive boolean default false,
						comments text default '');""".format(schema)

			# service address
			sql_statement += """
				CREATE table if not exists {0}.service_address (
						id serial,
						account_num varchar(7) default '',
						address_num numeric(4,0) default 0,
						branch_num varchar(5) default '',
						ref_num varchar(12) default '',
						street_address_1 text default '',
						street_address_2 text default '',
						city text default '',
						state text default '',
						zip varchar(10) default '',
						country varchar(3) default '',
						latitude numeric(12,8) default 0,
						longitude numeric(12,8) default 0,
						inactive boolean default false,
						comments text default '');""".format(schema)

			return sql_statement
		except Exception as error:
			raise error

	# Returns create table sql statement for log
	def _sql_log(self) -> str:
		try:
			return """
				CREATE table if not exists {0}.log (
						id serial,
						db_schema varchar(63) default '',
						db_table varchar(63) default '',
						description varchar(50) default '',
						start timestamp);""".format(self.__SYS_SCHEMA)
		except Exception as error:
			raise error

	# Returns create table sql statement for sql_data_schema.table_name
	def _sql_schema(self, schema:str, table:str) -> str:
		try:
			return """
				CREATE table if not exists {0}.{1} (
						id serial,
						sorting int default 0,
						data text default '');""".format(self.__SQL_SCHEMA + schema, table)
		except Exception as error:
			raise error

	#Public
	# Closes cursor & connection
	def close(self):
		try:
			# pg
			self.__pg_cursor.close()
			self.__pg_connection.close()

			# psql
			self.__psql_cursor.close()
			self.__psql_connection.close()
		except Exception as error:
			raise error

	# Creates data transfer schema
	def create_data_transfer_schema(self, schema:str='dt'):
		try:
			self.sql_queue_exec(self._sql_data_transfer(schema), exec=True)
		except Exception as error:
			raise error

	# Creates the schema if it doesn't exist
	def create_schema(self, schema:str):
		try:
			if self.sql_count("SELECT schema_name from information_schema.schemata where schema_name = '{0}'".format(str(schema))) == 0:
				# sys.log inserting
				if schema != self.__SYS_SCHEMA:
					description = 'schema created'
					if self.__check_log('{0}~{1}~{2}'.format(schema, '', description)) == False:
						self.__log_queue.append('{0}~{1}~{2}~{3}'.format(schema, '', description, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

				# create schema
				self.__pg_cursor.execute('CREATE SCHEMA if not exists {0};'.format(schema))
		except Exception as error:
			raise error

	# Creates all pgsql functions
	def create_system_defaults(self, schema:str='dt', version:str=''):
		try:
			system_defaults_timer = timer()

			# create sys schema
			self.create_schema(self.__SYS_SCHEMA)

			# initialize connect to default database
			self.__init_pg_sql_connection()
			
			print('Creating sys functions in: ', end='', flush=True)
			# get all pgsql functions
			self.__pg_sql_cursor.execute("SELECT * from {0}.functions".format(self.__SYS_SCHEMA))
			# creates function definition from pgsql.functions
			for function in self.__pg_sql_cursor:
				if function[0] == 'sql':
					function_str = "drop function if exists {0}.{1};\ncreate function {0}.{1}({2}) returns {3}\n\tlanguage {4}\n\timmutable\n\t{5}".format(self.__SYS_SCHEMA, function[1], function[2], function[3], function[0], function[5])
				elif function[0] == 'plpgsql':
					function_str = "drop function if exists {0}.{1};\ncreate function {0}.{1}({2})\n\treturns {3} language {6} as $$\ndeclare\n\t{4}\nbegin\n\t{5}\nend;\n$$;".format(self.__SYS_SCHEMA, function[1], function[2], function[3], function[4], function[5], function[0])
				# build function into sys schema
				self.sql_queue_exec(function_str)
			system_defaults_timer.print_time()
		except Exception as error:
			raise error
		finally:
			system_defaults_timer.print_time('Total system defaults created in: ')

	# Processing dataflex data
	def etl_dataflex_data(self, path:str, file:str='', data_schema:str='dflex', remove_empty_tables:bool=True, ignore_tables:array_list=array_list()):
		try:
			# third party library import
			import pyodbc
			
			# total timing
			total_timer = timer()

			print('Extracting, transforming & loading dataflex data...')

			# sql_[data_type] schema
			self.create_schema(self.__SQL_SCHEMA + data_schema)
			# dataflex schema
			self.create_schema(data_schema)

			# set location
			location = data_location(path, file, ['vld'])

			# removes any tables from location list
			for table_name in ignore_tables:
				location.get_file_list().remove(table_name.lower())

			# loop thru file list
			for file in location.get_file_list():
				# table timing
				table_timer = timer()

				table_name = file.split('.')[0]
				print('{0} completed in: '.format(table_name), end='', flush=True)

				# queue drop table statements
				sql_drop = self.__drop_table_statements(data_schema, table_name)
				
				# open table
				odbc_connection = pyodbc.connect('DRIVER={0};DBQ={1}'.format('{DataFlex Driver}', path))
				odbc_cursor = odbc_connection.cursor()
				odbc_cursor.execute("SELECT * from {0}".format(table_name))

				# create table string - data schema
				sql_create = "CREATE table if not exists {0}.{1}(".format(data_schema, table_name)
				# columns in description (name, type_code, display_size, internal_size, precision, scale, null_ok)
				for column in odbc_cursor.description:
					# creates column name
					sql_create += '"{0}" '.format(self.__modify_column_name(column[0].lower()))

					# creates data type & size
					if str(column[1]) == "<class 'str'>":
						sql_create += 'varchar({0}), '.format(column[3])
					elif str(column[1]) == "<class 'int'>":
						sql_create += 'integer, '
					elif str(column[1]) == "<class 'decimal.Decimal'>":
						sql_create += 'numeric({0},{1}), '.format(column[4], column[5])
					elif str(column[1]) == "<class 'datetime.date'>":
						sql_create += 'date NULL, '
				# remove last comma and space and replace with );
				sql_create = sql_create.rstrip(', ') + ');'
				# queue create table statements
				self.__create_table_statements(data_schema, table_name, sql_create)

				# write drop & create statments after sql_[data_type].table is setup
				self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

				# rows from odbc connection
				for row in odbc_cursor:
					sql_row = ''
					# columns from each row
					for column in row:
						# nulls
						if column is None:
							sql_row += "null, "
						# numbers & boolean
						elif isinstance(column, (bool, int, float, decimal.Decimal)):	#
							sql_row += "{0}, ".format(column)
						# bytes
						elif isinstance(column, bytes):
							sql_row += "{0}, ".format(self.__transform_byte_data(column))
						# strings & dates
						else:
							sql_row += "E'{0}', ".format(self.__transform_string_data(str(column)))
					# remove last comma and space
					sql_row = sql_row.rstrip(', ')
					# queue insert into statements
					self.__insert_into_statements(data_schema, table_name, sql_row)
					
				# execute anything left in the queue
				self.sql_queue_exec(exec=True)

				# close table
				odbc_cursor.close()

				# print table timer
				table_timer.print_time()

			# vacuums
			self.sql_vacuum()
			# only run when multiple files
			if len(location.get_file_list()) > 1:
				# removes 0 record tables
				self.sql_remove_empty_tables(data_schema, remove_empty_tables)
			
			# close connection
			if len(location.get_file_list()) > 0:
				odbc_connection.close()

			if len(location.get_file_list()) > 1:
				# print total timer
				total_timer.print_time('Total conversion of dataflex data completed in: ')
		except Exception as error:
			raise error

	# Processing fox pro data
	def etl_fox_pro_data(self, path:str, file:str='', data_schema:str='vfp', remove_empty_tables:bool=True, ignore_tables:array_list=array_list()):
		try:
			# third party library import
			import dbfread

			# constants
			DEFAULT_ENCODING = 'cp1252'

			# total timing
			total_timer = timer()

			if normal_fox_pro_operation:
				print('Extracting, transforming & loading fox pro data...')

			# sql_[data_type] schema
			self.create_schema(self.__SQL_SCHEMA + data_schema)
			# fox pro schema
			self.create_schema(data_schema)

			# set location
			location = data_location(path, file, ['dbf'])

			# removes any tables from location list
			for table_name in ignore_tables:
				location.get_file_list().remove(table_name.lower())

			# loop thru file list
			for file in location.get_file_list():
				# table timing
				table_timer = timer()

				table_name = file.split('.')[0]
				print('{0} completed in: '.format(table_name), end='', flush=True)

				# queue drop table statements
				sql_drop = self.__drop_table_statements(data_schema, table_name, normal_fox_pro_operation)

				# open table
				dbf_cursor = dbfread.DBF(filename=os.path.join(path, file), encoding=DEFAULT_ENCODING, lowernames=True, load=True)
				
				# create table string - data schema
				sql_create = "CREATE table if not exists {0}.{1}(".format(data_schema, table_name)
				
				table_fields = array_list()

				# get field definitions
				for field in dbf_cursor.fields:
					if field.type == 'C' or field.type == 'V':
						sql_create += '"{0}" varchar({1})'.format(self.__modify_column_name(field.name), field.length) + " not NULL default '', "
						table_fields.append('string')
					elif field.type == 'L':
						sql_create += '"{0}" boolean'.format(self.__modify_column_name(field.name)) + " not NULL default false, "
						table_fields.append('boolean')
					elif field.type == 'D':
						sql_create += '"{0}" date NULL, '.format(self.__modify_column_name(field.name))
						table_fields.append('date')
					elif field.type == 'T' or field.type == '@':
						sql_create += '"{0}" timestamp NULL, '.format(self.__modify_column_name(field.name))
						table_fields.append('date')
					elif field.type == 'I' or field.type == '+':
						sql_create += '"{0}" integer not NULL default 0, '.format(self.__modify_column_name(field.name))
						table_fields.append('number')
					elif field.type == 'N':
						sql_create += '"{0}" numeric({1},{2}) not NULL default 0, '.format(self.__modify_column_name(field.name), field.length, field.decimal_count)
						table_fields.append('decimal')
					elif field.type == 'F':
						sql_create += '"{0}" float4 not NULL default 0, '.format(self.__modify_column_name(field.name))
						table_fields.append('number')
					elif field.type == 'B':
						sql_create += '"{0}" float8 not NULL default 0, '.format(self.__modify_column_name(field.name))
						table_fields.append('number')
					elif field.type == 'Y':
						sql_create += '"{0}" decimal({1},{2}) not NULL default 0, '.format(self.__modify_column_name(field.name), field.length+field.decimal_count, field.decimal_count)
						table_fields.append('decimal')
					elif field.type == 'M':
						sql_create += '"{0}" text'.format(self.__modify_column_name(field.name)) + " not NULL default '', "
						table_fields.append('string')
					else:
						table_fields.append('invalid')
				
				# remove last comma and space and replace with );
				sql_create = sql_create.rstrip(', ') + ');'
				# queue create table statements
				self.__create_table_statements(data_schema, table_name, sql_create, normal_fox_pro_operation)
				
				# write drop & create statments after sql_[data_type].table is setup
				self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)
				
				record_number = 1
				
				# loop thru data rows
				for row in dbf_cursor.records:
					sql_row = ''
					
					column_count = 0
					# loop thru each column
					for column in array_list(row.items()):
						if table_fields[column_count] != 'invalid':
							# boolean
							if table_fields[column_count] == 'boolean':
								sql_row += "{0}, ".format('true' if str(column[1]) == 'True' else 'false')
							# int / float
							elif table_fields[column_count] == 'number':
								sql_row += "{0}, ".format(str(column[1]).strip() if column[1] is not None else str(0))
							# numeric / decimal
							elif table_fields[column_count] == 'decimal':
								if column[1] is None:
									sql_row += "{0}, ".format(0)
								# vfp can do 1,000,000.000 is valid in numeric(10, 4) but psql can only have 999,999.9999
								elif len(str(column[1]).split('.')[0]) > dbf_cursor.fields[column_count].length - dbf_cursor.fields[column_count].decimal_count:
									sql_row += '{0}, '.format(float('9' * (dbf_cursor.fields[column_count].length - dbf_cursor.fields[column_count].decimal_count) + '.' + '9' * dbf_cursor.fields[column_count].decimal_count))
								else:
									sql_row += "{0}, ".format(str(column[1]).strip()[0:dbf_cursor.fields[column_count].length])
							# string
							elif table_fields[column_count] == 'string':
								sql_row += "E'{0}', ".format(self.__transform_string_data(str(column[1])) if column[1] is not None else '')
							# nulls
							elif column[1] is None:
								sql_row += "null, "
							# date / time
							elif table_fields[column_count] == 'date':
								sql_row += "'{0}', ".format(column[1])
						column_count += 1
					# special exception - adding record_number to the end of sql_row
					if file == 'fields.dbf':
						sql_row += '{0}'.format(record_number)
						record_number += 1
					else:
						# remove last comma and space
						sql_row = sql_row.rstrip(', ')
					# queue insert into statements
					self.__insert_into_statements(data_schema, table_name, sql_row, normal_fox_pro_operation)
					
				# execute anything left in the queue
				self.sql_queue_exec(exec=True)

				# close table
				dbf_cursor = ''

				# print table timer
				table_timer.print_time()

			# vacuums
			self.sql_vacuum()
			# only run when multiple files
			if len(location.get_file_list()) > 1:
				# removes 0 record tables
				self.sql_remove_empty_tables(data_schema, remove_empty_tables)
				# print total timer
				total_timer.print_time('Total conversion of fox pro data completed in: ')
		except Exception as error:
			raise error

	# Processing mysql data - needs fixing
	def etl_mysql_data(self, my_database:str, table:str='', data_schema:str='mysql', remove_empty_tables:bool=True, ignore_tables:array_list=array_list()):
		try:
			# third party library import
			import pymysql

			# total timing
			total_timer = timer()

			print('Extracting, transforming & loading mysql data...')

			# sql_[data_type] schema
			self.create_schema(self.__SQL_SCHEMA + data_schema)
			# mysql schema
			self.create_schema(data_schema)

			# open database - change
			mysql_connection = pymsyql.connect(
						host = self.__login_info['local_mysql']['host'],
						database = my_database,
						user = self.__login_info['local_mysql']['user'],
						password = self.__login_info['local_mysql']['password'])
			mysql_cursor = mssql_connection.cursor()
			
			# get all tables from mssql - change sql statement
			tables_sql_statement = """
				SELECT schema_name(tables.schema_id) as schema_name,
					lower(tables.[name]) as table_name,
					sum(partitions.rows) as row_count
				FROM sys.tables
					left outer join sys.partitions on tables.object_id = partitions.object_id
				GROUP by tables.schema_id, lower(tables.[name])
				--HAVING sum(partitions.rows) != 0
				ORDER by tables.schema_id, lower(tables.[name]);"""
			mysql_cursor.execute(tables_sql_statement)

			# add to my_tables from mysql - change
			my_tables = array_list()
			for table in mysql_cursor:
				my_tables.append(table[1] if table[0] == 'dbo' else '{0}.{1}'.format(table[0], table[1]))

			# removes any tables from tables list
			for table_name in ignore_tables:
				my_tables.remove(table_name.lower())
			
			# when table variable is used
			if table != '':
				my_tables.clear()
				if my_tables.exists(table):
					my_tables.append(table)
				else:
					print("Table doesn't exist in database: {0}".format(table))
			
			# loop thru table list
			for table_name in my_tables:
				# table timing
				table_timer = timer()

				# get schema_name & table_name - change
				if table_name.count('.') != 0:
					schema_name = table_name.split('.')[0]
					table_name = table_name.split('.')[1]
				else:
					schema_name = 'dbo'

				print('{0} completed in: '.format(table_name), end='', flush=True)

				# queue drop table statements
				sql_drop = self.__drop_table_statements(data_schema, table_name)

				# get table schema - change
				mysql_cursor.execute("""
					SELECT --lower(tables.[name]) as table_name,
						columns.column_id,
						lower(columns.[name]) as column_name,
						case
							when lower(types.[name]) in ('binary', 'image', 'rowversion', 'timestamp', 'varbinary') then 'bytea'
							when lower(types.[name]) = 'bit' then 'boolean'
							when lower(types.[name]) = 'datetime' then 'timestamp(3) without time zone'
							when lower(types.[name]) = 'datetime2' then 'timestamp without time zone'
							when lower(types.[name]) = 'datetimeoffset' then 'timestamp'
							when lower(types.[name]) = 'decimal' then 'decimal(' + cast(columns.precision as varchar) + ',' + cast(columns.scale as varchar) + ')'
							when lower(types.[name]) = 'float' then 'float8'
							when lower(types.[name]) in ('nchar', 'char') then 'varchar' + case when columns.max_length is not null then '(' + cast(columns.max_length as varchar) + ')' else '' end
							when lower(types.[name]) in ('ntext', 'text') or lower(types.[name]) in ('nvarchar', 'varchar') and columns.max_length = -1 then 'text'
							when lower(types.[name]) in ('nvarchar', 'varchar') then 'varchar' + case when columns.max_length is not null then '(' + cast(columns.max_length as varchar) + ')' else '' end
							when lower(types.[name]) = 'smalldatetime' then 'timestamp(0) without time zone'
							when lower(types.[name]) = 'smallmoney' then 'money'
							when lower(types.[name]) = 'time' then 'time without time zone'
							when lower(types.[name]) = 'tinyint' then 'smallint'
							when lower(types.[name]) = 'uniqueidentifier' then 'varchar(16)'	--'uuid'
							when lower(types.[name]) in ('cursor', 'hiearchyid', 'sql_variant', 'table') then ''
							else lower(types.[name]) end as data_type,
						case when columns.is_nullable = 1 then 'True' else 'False' end as is_nullable
					FROM sys.tables
						inner join sys.columns on tables.object_id = columns.object_id
						inner join sys.types on columns.system_type_id = types.system_type_id and lower(types.[name]) != 'sysname'
					WHERE tables.[name] = '{0}'
					ORDER by tables.[name], columns.column_id;""".format(table_name))

				# create table string - data schema
				sql_create = "CREATE table if not exists {0}.{1}(".format(data_schema, table_name)

				# columns (column_id, column_name, data_type, is_nullable) - change
				for column in mysql_cursor:
					sql_create += '{0} {1}{2}, '.format(self.__modify_column_name(column[1]), column[2], '' if column[3] == 'True' else ' NOT NULL')
				# remove last comma and space and replace with );
				sql_create = sql_create.rstrip(', ') + ');'
				# queue create table statements
				self.__create_table_statements(data_schema, table_name, sql_create)

				# write drop & create statments after sql_[data_type].table is setup
				self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

				# get data from table_name
				mysql_cursor.execute("SELECT * from {0}.{1};".format(schema_name, table_name))

				# row from mssql connection
				for row in mysql_cursor:
					sql_row = ''
					# column from each row
					for column in row:
						# nulls
						if column is None:
							sql_row += "null, "
						# numbers & boolean
						elif isinstance(column, (bool, int, float, decimal.Decimal)):	#
							sql_row += "{0}, ".format(column)
						# bytes
						elif isinstance(column, bytes):
							sql_row += "{0}, ".format(self.__transform_byte_data(column))
						# strings & dates
						else:
							sql_row += "E'{0}', ".format(self.__transform_string_data(str(column)))
					# remove last comma and space
					sql_row = sql_row.rstrip(', ')
					# queue insert into statements
					self.__insert_into_statements(data_schema, table_name, sql_row)
			
				# execute anything left in the queue
				self.sql_queue_exec(exec=True)

				# close table
				mysql_cursor.close()

				# print table timer
				table_timer.print_time()

			# vacuums
			self.sql_vacuum()
			if len(my_tables) > 1:
				# removes 0 record tables
				self.sql_remove_empty_tables(data_schema, remove_empty_tables)

			# close cursor & connection
			#mysql_cursor.close()
			mysql_connection.close()

			if len(my_tables) > 1:
				# print total timer
				total_timer.print_time('Total conversion of mysql data completed in: ')
		except Exception as error:
			raise error
		
	# Processing spreadsheet data
	def etl_spreadsheet_data(self, path:str, file:str='', data_schema:str='excel', file_table_name:bool=False, sheet_table_name:bool=True, header_line:int=1, bottom_lines_skipped:int=0, delimiter_char=',', ignore_files:array_list=array_list(), ignore_tables:array_list=array_list()):
		try:
			# third party library import
			import csv
			import pyexcel
			
			# total timing
			total_timer = timer()

			print('Extracting, transforming & loading spreadsheet data...')

			# sql_[data_type] schema
			self.create_schema(self.__SQL_SCHEMA + data_schema)
			# spreadsheet schema
			self.create_schema(data_schema)

			# set location
			location = data_location(path, file, ['csv', 'tsv', 'xlsx', 'xlsm', 'xls'])
			
			# removes any files from location list
			for file_name in ignore_files:
				location.get_file_list().remove(file_name.lower())

			# loop thru file list
			for file in location.get_file_list():
				# table timing
				table_timer = timer()

				# open file
				spreadsheet_cursor = pyexcel.get_book(file_name=os.path.join(path, file), encoding=self.__ENCODING, delimiter=delimiter_char)
				
				# get list of work_sheets and remove any from ignore_tables
				work_sheets = array_list(spreadsheet_cursor.sheet_names())
				if isinstance(ignore_tables, list):
					for table_name in ignore_tables:
						work_sheets.remove(table_name)
				else:
					work_sheets.remove(ignore_tables)
				
				# loop thru file (ie workbook)
				for work_sheet in work_sheets:
					number_of_columns = spreadsheet_cursor[work_sheet].number_of_columns()
					number_of_rows = spreadsheet_cursor[work_sheet].number_of_rows()

					# get table_name from file and/or sheet
					table_name = ''
					if file_table_name:
						table_name = re.sub('\\W', '_', os.path.splitext(file)[0].lower() + '{0}'.format('_' if sheet_table_name else ''))
					# set table_name from sheet_name regardless
					table_name += re.sub('\\W', '_', work_sheet.lower())

					# get column names in work_sheet
					column_names = array_list()
					for column_index in range(number_of_columns):
						# uses column_# for names if no header_line used
						if header_line == 0:
							column_names.append('column_' + str(column_index + 1))
						else:
							# trims column names and replace whitespace characters with underscore
							new_column_name = re.sub('\\W' , '_', str(spreadsheet_cursor[work_sheet].cell_value(header_line - 1, column_index)).strip())
							# uses column_# if empty column name
							if new_column_name == '':
								column_names.append('column_' + str(column_index + 1))
							# uses column name plus _# if column name is already used once
							elif column_names.exists(new_column_name):
								column_names.append(new_column_name + '_' + str(column_index + 1))
							# uses column name in spreadsheet
							else:
								column_names.append(new_column_name)

					# setup blank array_lists
					column_types = array_list([''] * number_of_columns)
					column_sizes = array_list([0] * number_of_columns)
						
					# column types & sizes checking
					for row_index in range(header_line, number_of_rows - bottom_lines_skipped):
						# column specifications
						for column_index in range(number_of_columns):
							testing_value = spreadsheet_cursor[work_sheet].cell_value(row_index, column_index)
							
							# boolean
							if isinstance(testing_value, bool) or str(testing_value).lower() == 'false' or str(testing_value).lower() == 'true':
								column_types[column_index] = 'boolean'
							# date
							elif isinstance(testing_value, datetime.date) and column_types[column_index] != 'boolean':
								column_types[column_index] = 'date'
							# string
							elif isinstance(testing_value, str) and column_types[column_index] != 'boolean' and column_types[column_index] != 'date' and testing_value != '':
								column_types[column_index] = 'varchar'
							# numeric
							elif isinstance(testing_value, float) and column_types[column_index] != 'boolean' and column_types[column_index] != 'date' and column_types[column_index] != 'varchar':
								column_types[column_index] = 'numeric'
							# integer or bigint
							elif isinstance(testing_value, int) and column_types[column_index] != 'boolean' and column_types[column_index] != 'date' and column_types[column_index] != 'varchar' and column_types[column_index] != 'numeric':
								if abs(int(testing_value)) <= 2147483647 and column_types[column_index] != 'bigint':
									column_types[column_index] = 'integer'
								else:
									column_types[column_index] = 'bigint'
							
							# set the maximum length size for each column
							if column_sizes[column_index] < len(str(testing_value)):
								column_sizes[column_index] = len(str(testing_value))

					# queue drop table statements
					sql_drop = self.__drop_table_statements(data_schema, table_name)

					# create table string - data schema
					sql_create = "CREATE table if not exists {0}.{1}(".format(data_schema, table_name)

					# column statements for table creation
					for column_index in range(number_of_columns):
						# column name
						sql_create += '"{0}" '.format(str(column_names[column_index]))

						# column types & size
						# string
						if column_types[column_index] == 'varchar':
							sql_create += "varchar({0}) default ''".format(column_sizes[column_index])
						# integer or bigint
						elif column_types[column_index] == 'integer' or column_types[column_index] == 'bigint':
							sql_create += '{0} default 0'.format(column_types[column_index])
						# numeric
						elif column_types[column_index] == 'numeric':
							sql_create += 'float default 0'
						# boolean
						elif column_types[column_index] == 'boolean':
							sql_create += "boolean default 'false'"
						# date
						elif column_types[column_index] == 'date':
							sql_create += 'timestamp'
						
						# separator or ending
						if column_index + 1 != number_of_columns:
							sql_create += ', '
						else:
							sql_create += ');'

					# queue create table statements
					self.__create_table_statements(data_schema, table_name, sql_create)

					# write drop & create statments after sql_[data_type].table is setup
					self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

					# process data rows
					for row_index in range(header_line, number_of_rows - bottom_lines_skipped):
						sql_row = ''

						# column from each row
						for column_index in range(number_of_columns):
							cell_value = spreadsheet_cursor[work_sheet].cell_value(row_index, column_index)

							# boolean
							if column_types[column_index] == 'boolean':
								if str(cell_value).lower() == 'true':
									sql_row += str(True) + ', '
								else:
									sql_row += str(False) + ', '
							# integer, bigint & numeric
							elif column_types[column_index] == 'integer' or column_types[column_index] == 'bigint' or column_types[column_index] == 'numeric':
								if str(cell_value) == '':
									sql_row += str(0) + ', '
								else:
									sql_row += str(cell_value) + ', '
							# dates
							elif column_types[column_index] == 'date':
								if str(cell_value).strip() == '':
									sql_row += 'null, '
								else:
									sql_row += "'{0}', ".format(str(cell_value).strip())	
							# strings
							elif column_types[column_index] == 'varchar':
								sql_row += "E'{0}', ".format(self.__transform_string_data(str(cell_value)))
						# remove last comma and space
						sql_row = sql_row.rstrip(', ')
						# queue insert into statements
						self.__insert_into_statements(data_schema, table_name, sql_row)

					# execute anything left in the queue
					self.sql_queue_exec(exec=True)
						
			# vacuums
			self.sql_vacuum()

			# print total timer
			total_timer.print_time('Total conversion of spreadsheet data completed in: ')
		except Exception as error:
			raise error
	
	# Processing sql server data
	def etl_sql_server_data(self, ms_database:str, table:str='', data_schema:str='mssql', remove_empty_tables:bool=True, ignore_tables:array_list=array_list()):
		try:
			# third party library import
			import pymssql
			
			# constants
			self.__DEFAULT_ENCODING = 'utf-16'

			# total timing
			total_timer = timer()

			print('Extracting, transforming & loading sql server data...')

			# sql_[data_type] schema
			self.create_schema(self.__SQL_SCHEMA + data_schema)
			# sql server schema
			self.create_schema(data_schema)

			# open database
			mssql_connection = pymssql.connect(
						host = self.__login_info['local_mssql']['host'],
						server = self.__login_info['local_mssql']['server'],
						database = ms_database,
						user = self.__login_info['local_mssql']['user'],
						password = self.__login_info['local_mssql']['password'],
						#autocommit = True,
						as_dict = False)
			mssql_cursor = mssql_connection.cursor()
			
			# get all tables from mssql
			tables_sql_statement = """
				SELECT schema_name(tables.schema_id) as schema_name,
					lower(tables.[name]) as table_name,
					sum(partitions.rows) as row_count
				FROM sys.tables
					left outer join sys.partitions on tables.object_id = partitions.object_id
				GROUP by tables.schema_id, lower(tables.[name])
				--HAVING sum(partitions.rows) != 0
				ORDER by tables.schema_id, lower(tables.[name]);"""
			mssql_cursor.execute(tables_sql_statement)

			# add to ms_tables from mssql
			ms_tables = array_list()
			for ms_table in mssql_cursor:
				ms_tables.append(ms_table[1] if ms_table[0] == 'dbo' else '{0}.{1}'.format(ms_table[0], ms_table[1]))

			# removes any tables from tables list
			for table_name in ignore_tables:
				ms_tables.remove(table_name.lower())
			
			# when table variable is used
			if table != '':
				if ms_tables.exists(table):
					ms_tables.clear()
					ms_tables.append(table)
				else:
					print("Table doesn't exist in database: {0}".format(table))

			# loop thru table list
			for table_name in ms_tables:
				# table timing
				table_timer = timer()

				# get schema_name & table_name
				if table_name.count('.') != 0:
					schema_name = table_name.split('.')[0]
					table_name = table_name.split('.')[1]
				else:
					schema_name = 'dbo'

				print('{0} completed in: '.format(table_name), end='', flush=True)

				# queue drop table statements
				sql_drop = self.__drop_table_statements(data_schema, table_name)

				# get table schema
				mssql_cursor.execute("""
					SELECT --lower(tables.[name]) as table_name,
						columns.column_id,
						lower(columns.[name]) as column_name,
						case
							when lower(types.[name]) in ('binary', 'image', 'rowversion', 'timestamp', 'varbinary') then 'bytea'
							when lower(types.[name]) = 'bit' then 'boolean'
							when lower(types.[name]) = 'datetime' then 'timestamp(3) without time zone'
							when lower(types.[name]) = 'datetime2' then 'timestamp without time zone'
							when lower(types.[name]) = 'datetimeoffset' then 'timestamp'
							when lower(types.[name]) = 'decimal' then 'decimal(' + cast(columns.precision as varchar) + ',' + cast(columns.scale as varchar) + ')'
							when lower(types.[name]) = 'float' then 'float8'
							when lower(types.[name]) in ('nchar', 'char') then 'varchar' + case when columns.max_length is not null then '(' + cast(columns.max_length as varchar) + ')' else '' end
							when lower(types.[name]) in ('ntext', 'text') or lower(types.[name]) in ('nvarchar', 'varchar') and columns.max_length = -1 then 'text'
							when lower(types.[name]) in ('nvarchar', 'varchar') then 'varchar' + case when columns.max_length is not null then '(' + cast(columns.max_length as varchar) + ')' else '' end
							when lower(types.[name]) = 'smalldatetime' then 'timestamp(0) without time zone'
							when lower(types.[name]) = 'smallmoney' then 'money'
							when lower(types.[name]) = 'time' then 'time without time zone'
							when lower(types.[name]) = 'tinyint' then 'smallint'
							when lower(types.[name]) = 'uniqueidentifier' then 'uuid'
							when lower(types.[name]) in ('cursor', 'hiearchyid', 'sql_variant', 'table') then ''
							else lower(types.[name]) end as data_type,
						case when columns.is_nullable = 1 then 'True' else 'False' end as is_nullable
					FROM sys.tables
						inner join sys.columns on tables.object_id = columns.object_id
						inner join sys.types on columns.system_type_id = types.system_type_id and lower(types.[name]) != 'sysname'
					WHERE tables.[name] = '{0}'
					ORDER by tables.[name], columns.column_id;""".format(table_name))

				# create table string - data schema
				sql_create = "CREATE table if not exists {0}.{1}(".format(data_schema, table_name)

				# columns (column_id, column_name, data_type, is_nullable)
				for column in mssql_cursor:
					sql_create += '{0} {1}{2}, '.format(self.__modify_column_name(column[1]), column[2], '' if column[3] == 'True' else ' NOT NULL')
				# remove last comma and space and replace with );
				sql_create = sql_create.rstrip(', ') + ');'
				# queue create table statements
				self.__create_table_statements(data_schema, table_name, sql_create)

				# write drop & create statments after sql_[data_type].table is setup
				self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

				# get data from table_name
				mssql_cursor.execute("SELECT * from {0}.{1};".format(schema_name, table_name))

				# row from mssql connection
				for row in mssql_cursor:
					sql_row = ''
					# column from each row
					for column in row:
						# nulls
						if column is None:
							sql_row += "null, "
						# numbers & boolean
						elif isinstance(column, (bool, int, float, decimal.Decimal)):	#
							sql_row += "{0}, ".format(column)
						# bytes
						elif isinstance(column, bytes):
							sql_row += "{0}, ".format(self.__transform_byte_data(column))
						# strings & dates
						else:
							sql_row += "E'{0}', ".format(self.__transform_string_data(str(column)))
					# remove last comma and space
					sql_row = sql_row.rstrip(', ')
					# queue insert into statements
					self.__insert_into_statements(data_schema, table_name, sql_row)
			
				# execute anything left in the queue
				self.sql_queue_exec(exec=True)

				# print table timer
				table_timer.print_time()

			# vacuums
			self.sql_vacuum()
			if len(ms_tables) > 1:
				# removes 0 record tables
				self.sql_remove_empty_tables(data_schema, remove_empty_tables)

			# close cursor & connection
			mssql_cursor.close()
			mssql_connection.close()

			if len(ms_tables) > 1:
				# print total timer
				total_timer.print_time('Total conversion of sql server data completed in: ')
		except Exception as error:
			raise error			

	# Read sql file data
	def read_sql_file(self, path:str, file:str='', ignore_tables:array_list=array_list()):
		try:
			# total timing
			total_timer = timer()

			# set location
			location = data_location(path, file, ['sql'])

			# removes any tables from location list
			for table_name in ignore_tables:
				location.get_file_list().remove(table_name.lower())

			read_sql_files_normal_operation = file.startswith('default_') == False

			if read_sql_files_normal_operation:
				print('Reading sql file{0}...'.format('' if len(location.get_file_list()) == 1 else 's'))
			
			# loop thru file list
			for file in location.get_file_list():
				# table timer
				table_timer = timer()
				print('  {0} completed in: '.format(file.split('.')[0]), end='', flush=True)

				# open current sql file
				with open(os.path.join(path, file), 'r', encoding=self.__ENCODING) as sql_file:
					build_sql_statement = ''

					# reads file line by line
					for read_line in sql_file:
						# incomplete sql statement - add to build_sql_statement
						if read_line.count(';') == 0 and len(read_line) > 0:
							build_sql_statement += read_line
						# complete sql statement - send to queue
						elif read_line.count(';') >= 1:
							self.sql_queue_exec(build_sql_statement + read_line)
							build_sql_statement = ''
					
					# execute anything left in the queue
					self.sql_queue_exec(exec=True)

					# print table timer
					table_timer.print_time()
			
			# print total timer
			if read_sql_files_normal_operation and len(location.get_file_list()) > 1:
				total_timer.print_time('SQL files completed in: ')
		except Exception as error:
			raise error

	# Renames current schema to new schema name
	def rename_schema(self, new_schema:str):
		try:
			# sys.log inserting
			#self.__insert_log(self.__pg_schema, str(new_schema), 'schema renamed')

			sql = 'ALTER SCHEMA {0} rename to {1};'.format(self.__pg_schema, str(new_schema))
			self.__queue_log(sql)
			self.__insert_log()
			self.__pg_cursor.execute(sql)
		except Exception as error:
			raise error

	# Return or print to screen the count in sql query
	def sql_count(self, sql_statement:str='', print:bool=False, print_str:str='') -> int:
		try:
			# execute query
			if sql_statement != '':
				self.__pg_cursor.execute(str(sql_statement))

			# print to screen the count
			if print and self.__pg_cursor.rowcount > 0:
				print('\t{0} row{1}: {2}'.format(self.__pg_cursor.rowcount, '' if self.__pg_cursor.rowcount == 1 else 's', str(print_str)))
			# return count
			elif print == False:
				return self.__pg_cursor.rowcount
		except Exception as error:
			raise error

	# Returns all records
	def sql_all_records(self, sql_statement:str='') -> list:
		try:
			# execute query
			if sql_statement != '':
				self.__pg_cursor.execute(str(sql_statement))

			# returns all records
			return self.__pg_cursor.fetchall()
		except Exception as error:
			raise error

	# Refreshes complete schema or single table from sql_[data_type].table
	def sql_refresh_data(self, schema:str='', table:str='', remove_empty_tables:bool=True, ignore_tables:array_list=array_list()):
		try:
			# total timing
			refresh_timer = timer()

			print('Refreshing data...')

			# add single table to tables list
			tables = array_list([table])
			
			# initialize connect to current database for sql_[data_type].table data
			self.__init_pg_sql_connection(self.__pg_database)

			# complete schema refresh
			if table == '':
				# get list of table names
				self.__pg_sql_cursor.execute("SELECT relname from pg_stat_user_tables where schemaname = '{0}' order by relname;".format(schema))
				# load all tuples into tables
				for records in self.__pg_sql_cursor.fetchall():
					tables.append(records[0])

				# removes any tables from tables list
				for table_name in ignore_tables:
					tables.remove(table_name.lower())
			
			# loop thru table list
			for table_name in tables:
				# table timing
				table_timer = timer()
				print('  {0} completed in: '.format(table_name), end='', flush=True)

				# get data from sql_[data_type].table
				self.__pg_sql_cursor.execute("SELECT data from {0}.{1} order by sorting, id;".format(self.__SQL_SCHEMA + schema, table_name))
				
				record_count = 1
				while record_count <= self.__pg_sql_cursor.rowcount:
					# gets next table.data record and adds it to the queue
					self.sql_queue_exec(self.__pg_sql_cursor.fetchone()[0])
					record_count += 1

				# execute last queued statements
				self.sql_queue_exec(exec=True)

				# print table timer
				table_timer.print_time()

			# vacuums
			self.sql_vacuum()

			# print total timer
			refresh_timer.print_time('Data refreshed in: ')
		except Exception as error:
			raise error

	# Removes any table with 0 records
	def sql_remove_empty_tables(self, schema:str, remove_empty_tables:bool=True):
		try:
			# remove empty tables
			if remove_empty_tables:
				remove_timer = timer()
				
				# get list of tables to remove
				remove_tables = self.sql_all_records("SELECT 'DROP TABLE if exists '||schemaname||'.'||relname||' cascade;' from pg_stat_user_tables where n_live_tup = 0 and schemaname = '{0}' order by relname;".format(schema))

				# check not empty
				if remove_tables != []:
					# load each command into the queue
					for remove_table in remove_tables:
						self.sql_queue_exec(remove_table[0])
					
					# execute queue
					self.sql_queue_exec(exec=True)

					remove_timer.print_time('Removed empty tables in: ')
		except Exception as error:
			raise error

	# Returns next record
	def sql_next_record(self) -> list:
		try:
			return self.__pg_cursor.fetchone()
		except Exception as error:
			raise error

	# Executes when exec is set or when queued sql statements reaches max semicolon or max string length
	def sql_queue_exec(self, sql_statement:str='', exec:bool=False):
		try:
			if sql_statement != '':
				# add semicolon to end of sql_statements
				if not (sql_statement.endswith(';') or sql_statement.endswith(';/n')):
					sql_statement += ';'
				
				# checking semicolon count & max string length
				self.__execute_sql_when_maximum(sql_statement)
				
			# execute all sql_statements
			if exec:
				# execute the remainder of sql strings - less than semicolon_maximum
				if self.__sql_statements.count(';') > 0:
					self.__insert_log()
					self.__pg_cursor.execute(self.__sql_statements)
					self.__sql_statements = ''
		except Exception as error:
			raise error

	# Vacuums with full, analyze
	def sql_vacuum(self, full:bool=False, analyze:bool=True, printing:bool=True):
		try:
			if printing:
				vacuum_timer = timer()
				print('Vacuum completed in: ', end='', flush=True)

			# building sql_statement string from parameters
			sql_statement = 'VACUUM'
			if full:
				sql_statement += ' full'
			if analyze:
				sql_statement += ' analyze'

			# execute
			self.__pg_cursor.execute(sql_statement)

			if printing:
				vacuum_timer.print_time()
		except Exception as error:
			raise error
