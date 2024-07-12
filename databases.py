""" PG SQL """

# standard library imports
import os
import re
import tomllib
from datetime import datetime

# third party library imports
import psycopg2

# local library imports
from utilities import ArrayList
from utilities import Timer

class PgSql:
    """ PostgreSQL database access """

    #def __init__(self, database: str, cloud: bool = False, sql_queue_count: int = 500):
    def __init__(self, database: str, connection: str = 'local_pg_5433', sql_queue_count: int = 500):
        # sql_queue_count to small ~< 100 and to big ~> 800 it becomes slower

        self.__toml_settings_file = os.path.join(os.path.dirname(__file__), 'info', 'logins.toml')
        # read login file
        with open(self.__toml_settings_file, 'rb') as toml_file:
            self._login_info = tomllib.load(toml_file)

        # get login credentials
        #self.__pg_server = self._login_info['cloud_pg' if cloud else 'local_pg']['host']
        #self.__pg_port = self._login_info['cloud_pg' if cloud else 'local_pg']['port']
        #self.__pg_username = self._login_info['cloud_pg' if cloud else 'local_pg']['user']
        #self.__pg_password = self._login_info['cloud_pg' if cloud else 'local_pg']['password']
        #self.__ssl = self._login_info['cloud_pg' if cloud else 'local_pg']['ssl']

        # constants
        # protected
        self._sql_schema = 'sql_'
        self._sys_schema = 'sys'
        # private
        #self.__pg_default_db = 'postgres'
        self.__sql_string_maximum = 100000000  # larger than 230,000,000 creates a string failure
        self.__sql_variables = {}

        self.__semicolon_maximum = sql_queue_count
        self._reserved_keywords = ArrayList()

        # variables
        self.__pg_database = database
        self.__login_credentials = self._login_info.get(connection)
        self.__sql_statements = ''
        self.__semicolon_count = 0
        self.__log_queue = ArrayList()
        self.__log_written = ArrayList()

        if self.__login_credentials is not None:

            # initalize connection to postgres database
            self.__init_pg_connection('postgres', self.__login_credentials, main_connection=False)

            """
            # check if database exists
            self.__pg_sql_connection = psycopg2.connect(
                host=self.__pg_server,
                port=self.__pg_port,
                user=self.__pg_username,
                password=self.__pg_password,
                database=self.__pg_default_db,
                sslmode=self.__ssl,
            )
            self.__pg_sql_connection.autocommit = True

            self.__pg_sql_cursor = self.__pg_sql_connection.cursor()
            """

            # check if database exists or not
            self.__pg_sql_cursor.execute(f"select datname from pg_catalog.pg_database where datname = '{database}';")
            # create database if empty rowcount
            if self.__pg_sql_cursor.rowcount == 0:
                self.__pg_sql_cursor.execute(f'create database "{database}";')

            # get reserved keywords - constant
            self.__pg_sql_cursor.execute("select word from pg_get_keywords() where catcode in ('R', 'T');")
            for keyword in self.__pg_sql_cursor.fetchall():
                self._reserved_keywords.append(keyword[0])

            # initalize connection to specified database
            self.__init_pg_connection(self.__pg_database, self.__login_credentials)

            """
            # connect to database
            self.__pg_connection = psycopg2.connect(
                host=self.__pg_server,
                port=self.__pg_port,
                user=self.__pg_username,
                password=self.__pg_password,
                database=self.__pg_database,
                sslmode=self.__ssl,
            )
            self.__pg_connection.autocommit = True
            self.__pg_cursor = self.__pg_connection.cursor()
            """

            # create sys.log if not exists
            self.__create_log()

        else:
            print(f'{connection}: not in {self.__toml_settings_file} file')

    # Public
    def close(self):
        """ Closes cursor & connection """

        # pg
        self.__pg_cursor.close()
        self.__pg_connection.close()

        # psql
        self.__pg_sql_cursor.close()
        self.__pg_sql_connection.close()

    def copy(self, dest_database: str, dest_schema: str = 'wt', dest_cloud: bool = False):
        """ Copy from source table to destination table """

        """
        # get login credentials
        pg_server = self._login_info['cloud_pg' if dest_cloud else 'local_pg']['host']
        pg_port = self._login_info['cloud_pg' if dest_cloud else 'local_pg']['port']
        pg_username = self._login_info['cloud_pg' if dest_cloud else 'local_pg']['user']
        pg_password = self._login_info['cloud_pg' if dest_cloud else 'local_pg']['password']
        ssl = self._login_info['cloud_pg' if dest_cloud else 'local_pg']['ssl']

        # create destination connection
        self.__pg_sql_connection = psycopg2.connect(
            host=pg_server,
            port=pg_port,
            user=pg_username,
            password=pg_password,
            database=self.__pg_default_db,
            sslmode=ssl,
        )
        """

        self.__init_pg_connection('postgres', credentials, False)

        # create table for destination

        # copy from source pg connection - using pg copy
        with self.__pg_cursor().copy('COPY source TO STDOUT (FORMAT BINARY)') as copy1:
            # copy to destination connection - using pg copy
            with dest_connection.cursor().copy('COPY target FROM STDIN (FORMAT BINARY)') as copy2:
                # loop thru source table data
                for data in copy1:
                    # write in destination table
                    copy2.write(data)

    def create_data_transfer_schema(self, schema: str = 'dt'):
        """ Creates data transfer schema """

        self.sql_queue_exec(self.__sql_data_transfer(schema), execute=True)

    def create_schema(self, schema: str):
        """ Creates the schema if it doesn't exist """

        schema_string = f"""
            select schema_name
            from information_schema.schemata
            where schema_name = '{schema}'"""

        if self.sql_count(schema_string) == 0:
            # sys.log inserting
            if schema != self._sys_schema:

                description = 'schema created'
                if not self.__check_log(f'{schema}~~{description}'):
                    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.__log_queue.append(f'{schema}~~{description}~{datetime_now}')

            # create schema
            self.__pg_cursor.execute(f'create schema if not exists {schema};')

    def drop_schema(self, schema: str):
        """ Drop the schema """

        self.__pg_cursor.execute(f"drop schema if exists {schema} cascade;")

    def rename_schema(self, old_schema: str, new_schema: str):
        """ Renames current schema to new schema name """

        sql = f'alter schema {old_schema} rename to {new_schema};'
        self.__queue_log(sql)
        self.__insert_log()
        self.__pg_cursor.execute(sql)

    def set_sql_variables(self, new_variables: dict):
        """ Sets SQL variables into dictionary """

        self.__sql_variables.update(new_variables)

    def sql_alter_table(self, schema: str, table: str):
        """ Alter pg table from: sql copy command or vfp tables (files, fields, indexes, relation) """

        pass

    def sql_count(self, sql_statement: str = '', printing: bool = False, print_str: str = '') -> int:
        """ Return or print to screen the count in sql query """

        # execute query
        if sql_statement != '':
            self.__pg_cursor.execute(self.__replace_sql_variables(sql_statement))

        # print to screen the count
        if printing and self.__pg_cursor.rowcount > 0:

            s = '' if self.__pg_cursor.rowcount == 1 else 's'
            print(f'\t{self.__pg_cursor.rowcount} row{s}: {print_str}')

        # elif not printing:
        return self.__pg_cursor.rowcount

    def sql_all_records(self, sql_statement: str = '') -> list:
        """ Returns all records """

        # execute query
        if sql_statement != '':
            self.__pg_cursor.execute(str(sql_statement))

        # returns all records
        return self.__pg_cursor.fetchall()

    def sql_refresh_data(self, schema: str = '', table: str = '', ignore_tables: ArrayList = ArrayList()):
        """ Refreshes complete schema or single table from sql_[data_type].table """

        # total timing
        refresh_timer = Timer()

        print('Refreshing data...')

        # add single table to tables list
        tables = ArrayList([table])

        # initialize connect to current database for sql_[data_type].table data
        #self.__init_pg_sql_connection(self.__pg_database)
        self.__init_pg_connection(self.__pg_database, self.__login_credentials)

        # complete schema refresh
        if table == '':
            # get list of table names
            self.__pg_sql_cursor.execute(
                f"""select relname
                    from pg_stat_user_tables 
                    where schemaname = '{schema}'
                    order by relname;"""
            )
            # load all tuples into tables
            for records in self.__pg_sql_cursor.fetchall():
                tables.append(records[0])

            # removes any tables from tables list
            for table_name in ignore_tables:
                tables.remove(table_name.lower())

        # loop thru table list
        for table_name in tables:
            # table timing
            table_timer = Timer()
            print(f'  {table_name} completed in: ', end='', flush=True)

            # get data from sql_[data_type].table
            sql_statement = f'select data from {self._sql_schema + schema}.{table_name} order by sorting, id;'
            self.__pg_sql_cursor.execute(sql_statement)                

            record_count = 1
            while record_count <= self.__pg_sql_cursor.rowcount:
                # gets next table.data record and adds it to the queue
                self.sql_queue_exec(self.__pg_sql_cursor.fetchone()[0])
                record_count += 1

            # execute last queued statements
            self.sql_queue_exec(execute=True)

            # print table timer
            table_timer.print_time()

        # vacuums
        self.sql_vacuum()

        # print total timer
        refresh_timer.print_time('Data refreshed in: ')

    def sql_remove_empty_tables(self, schema: str, remove_empty_tables: bool = True):
        """ Removes any table with 0 records """

        # remove empty tables
        if remove_empty_tables:
            remove_timer = Timer()

            # get list of tables to remove
            remove_tables = self.sql_all_records(
                f"""select 'drop table if exists '||schemaname||'.'||relname||' cascade;'
                    from pg_stat_user_tables
                    where n_live_tup = 0 and schemaname = '{schema}'
                    order by relname;"""
            )

            # check not empty
            if remove_tables != []:
                # load each command into the queue
                for remove_table in remove_tables:
                    self.sql_queue_exec(remove_table[0])

                # execute queue
                self.sql_queue_exec(execute=True)

                remove_timer.print_time('Removed empty tables in: ')

    def sql_next_record(self) -> list:
        """ Returns next record """

        return self.__pg_cursor.fetchone()

    def sql_queue_exec(self, sql_statement: str = '', execute: bool = False):
        """ Executes when execute is set or when queued sql statements reaches max semicolon or max string length """

        sql_statement = self.__remove_sql_comments(sql_statement)
        sql_statement = self.__replace_sql_variables(sql_statement)

        if sql_statement != '':
            # add semicolon to end of sql_statements
            if not (sql_statement.endswith(';') or sql_statement.endswith(';/n')):
                sql_statement += ';'

            # checking semicolon count & max string length
            self.__execute_sql_when_maximum(sql_statement)

        # execute all sql_statements
        if execute:
            # execute the remainder of sql strings - less than semicolon_maximum
            if self.__sql_statements.count(';') > 0:
                self.__insert_log()

                try:
                    self.__pg_cursor.execute(self.__sql_statements)
                except Exception as error:
                    print(self.__sql_statements)
                    raise error

                self.__sql_statements = ''

    def sql_vacuum(self, full: bool = False, analyze: bool = True, printing: bool = True):
        """ Vacuums with full, analyze """

        if printing:
            vacuum_timer = Timer()
            print('Vacuum completed in: ', end='', flush=True)

        # building sql_statement string from parameters
        sql_statement = 'vacuum'
        if full:
            sql_statement += ' full'
        if analyze:
            sql_statement += ' analyze'

        # execute
        self.__pg_cursor.execute(sql_statement)

        if printing:
            vacuum_timer.print_time()

    # Private
    def __check_log(self, check_string: str) -> bool:
        """ Checks if check_string is in log ArrayLists """

        # check log_written
        for element in self.__log_written:
            if element.startswith(check_string):
                return True

        # check log_queue
        for element in self.__log_queue:
            if element.startswith(check_string):
                return True

        return False

    def __create_log(self):
        """ Creates the schema & table: sys.log """

        sys_created = False

        # create sys schema
        self.__pg_cursor.execute(
            f"""
                select schema_name
                from information_schema.schemata
                where schema_name = '{self._sys_schema}';"""
        )
        if self.__pg_cursor.rowcount == 0:
            self.__pg_cursor.execute(f'create schema if not exists {self._sys_schema};')
            sys_created = True

        #log_created = False
        # create log table
        self.__pg_cursor.execute(
            f"""
                select schemaname, relname, n_live_tup
                from pg_stat_user_tables
                where schemaname = '{self._sys_schema}' and relname = 'log'"""
        )

        if self.__pg_cursor.rowcount == 0:
            self.__pg_cursor.execute(self.__sql_log())
            self.__pg_cursor.execute('drop schema if exists public cascade;')

        if sys_created:
            datetime_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.__log_queue.append(f'{self._sys_schema}~~schema created~{datetime_now}')
            self.__log_queue.append(f'{self._sys_schema}~log~table created~{datetime_now}')
            #log_created = True

    def __execute_sql_when_maximum(self, sql_line: str):
        """ Executes the sql statements if maximum semicolons or string length """

        # queue in log ArrayList
        self.__queue_log(sql_line)

        # set sql_statements and semicolon_count if under string maximum
        #   or current sql_line is greater than string maximum
        if len(self.__sql_statements) + len(sql_line) <= self.__sql_string_maximum or (
                self.__sql_statements == '' and len(sql_line) > self.__sql_string_maximum
        ):
            self.__sql_statements += sql_line
            self.__semicolon_count += 1
            sql_line = ''

        # semicolon count reaches max constant or string length max then execute sql string
        if (
                self.__semicolon_count == self.__semicolon_maximum
                or len(self.__sql_statements) + len(sql_line) > self.__sql_string_maximum
        ):
            self.__insert_log()
            self.__pg_cursor.execute(self.__sql_statements)
            self.__sql_statements = sql_line
            self.__semicolon_count = 1 if sql_line.endswith(';') else 0

    def __init_pg_sql_connection(self, database: str = 'postgres'):
        """ Initialize another connection for independent data usage """

        # if already connected to the database
        if str(self.__pg_sql_cursor.connection).count(database) == 0:
            # new connection for getting sql.table data
            self.__pg_sql_connection = psycopg2.connect(
                host=self.__pg_server,
                port=self.__pg_port,
                user=self.__pg_username,
                password=self.__pg_password,
                database=database,
                sslmode=self.__ssl,
            )
            self.__pg_sql_connection.autocommit = True

            # connection -> cursor
            self.__pg_sql_cursor = self.__pg_sql_connection.cursor()

    def __init_pg_connection(self, database: str, credentials: str, main_connection : bool = True):
        """ Initialize pg connection for data usage """

        if main_connection:
            # if already connected to the database
            if str(self.__pg_cursor.connection).count(database) == 0:
                # new connection for getting sql.table data
                self.__pg_connection = psycopg2.connect(
                    host=credentials['host'],
                    port=credentials['port'],
                    user=credentials['username'],
                    password=credentials['password'],
                    database=database,
                    sslmode=credentials['ssl']
                )
                self.__pg_connection.autocommit = True

                # connection -> cursor
                self.__pg_cursor = self.__pg_connection.cursor()

        else:
            # if already connected to the database
            if str(self.__pg_sql_cursor.connection).count(database) == 0:
                # new connection for getting sql.table data
                self.__pg_sql_connection = psycopg2.connect(
                    host=credentials['host'],
                    port=credentials['port'],
                    user=credentials['username'],
                    password=credentials['password'],
                    database=database,
                    sslmode=credentials['ssl']
                )
                self.__pg_sql_connection.autocommit = True

                # connection -> cursor
                self.__pg_sql_cursor = self.__pg_sql_connection.cursor()

    def __insert_log(self):
        """ Inserts db_schema, db_table, description & current date/time in the log table """

        # loop thru log_queue and insert into pg sql
        while len(self.__log_queue) >= 1:
            # pop first record
            log_record = self.__log_queue.pop(0)

            # write popped record into self.__log_written
            self.__log_written.append(log_record)

            # log[0] = schema, log[1] = table, log[2] = description
            log = log_record.split('~')

            # inserts into log table from self.__log_queue
            self.__pg_cursor.execute(
                f"""insert into {self._sys_schema}.log
                    (db_schema, db_table, description, start)
                    values ('{log[0]}', '{log[1]}', '{log[2]}', '{log[3]}')"""
            )

    def __queue_log(self, sql_statement: str):
        """ Adds log statements if it doesn't exist in ArrayList """

        # creates time_stamp in yyyy-mm-dd hh:mm:ss format
        time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # only log sql queries that use a schema.table syntax
        sql_schema_table = re.findall('from \\w+\\.\\w+', str(sql_statement), flags=re.IGNORECASE)

        if len(sql_schema_table) > 1:
            sql_schema = re.sub('from ', '', sql_schema_table[0].split('.')[0], flags=re.IGNORECASE)
            sql_table = sql_schema_table[0].split('.')[1]

            # add to log_queue ArrayList when sql_schema & sql_table are not empty
            if sql_schema != '' and sql_table != '':

                # create table
                if str(sql_statement).lower().startswith('create table'):
                    description = 'table created'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

                # delete from
                elif str(sql_statement).lower().startswith('delete from'):
                    description = 'values deleted'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

                # drop table
                elif str(sql_statement).lower().startswith('drop table'):
                    description = 'table dropped'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

                # function
                elif str(sql_statement).lower().startswith('drop function'):
                    description = 'function dropped'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~function created~{time_stamp}')

                # insert
                elif str(sql_statement).lower().startswith('insert into'):
                    description = 'values inserted'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

                # update
                elif str(sql_statement).lower().startswith('update'):
                    description = 'values updated'
                    if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                        self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

        # renaming schema
        elif len(re.findall('alter schema \\w+ rename to \\w;', str(sql_statement), flags=re.IGNORECASE)) > 0:
            sql_schema = re.sub(
                'alter schema ',
                '',
                str(sql_statement).split(' rename', maxsplit=1)[0],
                flags=re.IGNORECASE
            )
            
            sql_table = ''
            description = 'schema renamed'
            if not self.__check_log(f'{sql_schema}~{sql_table}~{description}'):
                self.__log_queue.append(f'{sql_schema}~{sql_table}~{description}~{time_stamp}')

    def __remove_sql_comments(self, sql_statement: str):
        """ Removes all comments from sql string """

        # remove single line comment --
        sql_statement = re.sub('(?:--.*\\s)', '', sql_statement)

        # remove comment block /* */
        sql_statement = re.sub('(?:/\\*(?:[\\s\\S]*)\\*/)', '', sql_statement)

        # remove any whitespace from the beginning or the end of the statement
        sql_statement = sql_statement.strip()

        # remove comments -- or /* */
        #sql_statement = re.sub('(?:--.*\\s|/\\*(?:[\\s\\S]*)\\*/)', '', sql_statement)

        return sql_statement

    def __replace_sql_variables(self, sql_statement: str):
        """ Replaces SQL variables in sql query with the variable from the sql dictionary """

        variables = set()
        variables.update(re.findall('\\${\\w+}', sql_statement))

        for dict_key in variables:
            sql_statement = sql_statement.replace(dict_key, str(self.__sql_variables[dict_key[2:][:-1]]))

        return str(sql_statement)

    def __sql_data_transfer(self, schema: str) -> str:
        """ Returns create table sql statement for data transfer """

        # billing address
        sql_statement = f"""
            create table if not exists {schema}.billing_address (
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
                comments text default '');"""

        # contact
        sql_statement += f"""
            create table if not exists {schema}.contact (
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
                comments text default '');"""

        # email
        sql_statement += f"""
            create table if not exists {schema}.email (
                id serial,
                account_num varchar(7) default '',
                address_num numeric(4,0) default 0,
                branch_num varchar(5) default '',
                email text default '',
                billing boolean default false,
                receiving boolean default false,
                inactive boolean default false,
                comments text default '');"""

        # phone
        sql_statement += f"""
            create table if not exists {schema}.phone (
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
                comments text default '');"""

        # service address
        sql_statement += f"""
            create table if not exists {schema}.service_address (
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
                comments text default '');"""

        return sql_statement

    def __sql_log(self) -> str:
        """ Returns create table sql statement for log """

        return f"""
            create table if not exists {self._sys_schema}.log (
                id serial,
                db_schema varchar(63) default '',
                db_table varchar(63) default '',
                description varchar(50) default '',
                start timestamp);"""
