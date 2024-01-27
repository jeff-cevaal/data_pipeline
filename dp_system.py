# standard library imports
import os
import re
import tomllib

# local library imports
from data_pipeline import array_list
from data_pipeline import pg_sql

# Constants
DATABASE = 'postgres'
SYSTEM_SCHEMA = 'sys'
SETTINGS = 'settings'

# Global variables
cloud_db = False
# read login file
with open(os.path.join(os.path.dirname(__file__), SETTINGS, 'login.toml'), mode='rb') as toml_file:
	login_info = tomllib.load(toml_file)

# path
pgsql_files = os.path.join(os.path.dirname(__file__), SETTINGS)

# data connection
pg = pg_sql(DATABASE)

# Public
# Main function that listens for commands
def start():
	try:
		_clear()
		print()
		while True:
			command = input('Enter selection (cloud): ' if cloud_db else 'Enter selection (local): ')
			if command.lower() == 'clear':
				_clear()
			elif command.lower() == 'cloud':
				_pg_switch(True)
			elif command.lower() == 'exit':
				break
			elif command.lower() == 'local':
				_pg_switch(False)
			elif command.lower() == 'new':
				pass
			elif command.lower() == 'sys':
				_sys()
			elif command.lower() == 'vacuum':
				pg.sql_vacuum(full=True)
			else:
				print('Incorrect syntax')

			print()
	except Exception as error:
		raise error

#Protected
# Clears the cmd windows and lists valid commands
def _clear():
	try:
		if os.name == 'nt':
			os.system('cls')
		else:
			os.system('clear')
		print('local              switches database destination' if cloud_db else 'cloud              switches database destination')
		print('vacuum             reclaims disk space & updates statistics')
		print()
		print('sys                inserts functions in the sys schema')
		print()
		print('clear              clears the screen')
		print('exit               exits the program')
	except Exception as error:
		raise error

# Switches database connection, cloud boolean & refreshes command list
def _pg_switch(switch:bool=False):
	try:
		global pg
		global cloud_db

		if switch:
			cloud_db = True
			pg = pg_sql(DATABASE, cloud=True)
		else:
			cloud_db = False
			pg = pg_sql(DATABASE)

		_clear()
	except Exception as error:
		raise error

# Processes, creates & inserts sql files under sys schema
def _sys():
	try:
		pg.create_schema(SYSTEM_SCHEMA)

		# pgsql/system
		pg.read_sql_file(pgsql_files, 'functions.sys')

		# vacuum
		pg.sql_vacuum()
	except Exception as error:
		raise error

# runs the start function - must be at the bottom
if __name__ == '__main__':
	start()