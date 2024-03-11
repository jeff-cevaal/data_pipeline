""" PG SQL """

# standard library imports
import decimal
import json
import os
import re
import tomllib
from binascii import hexlify
from datetime import datetime, date

# third party library imports
import dbfread
import psycopg2
import pyexcel
import pyodbc
import pymssql
import pymysql

# local library imports
from utilities import ArrayList
from utilities import DataLocation
from utilities import Timer


class PgSql:
    """PostgreSQL database access"""

    def __init__(
        self,
        database: str,
        schema: str = "sys",
        cloud: bool = False,
        toml: bool = True,
        single_exec: bool = False,
        etl_debugger: bool = False,
    ):
        try:
            if toml:
                toml_settings_file = os.path.join(os.path.dirname(__file__), "settings", "login.toml")
                # read login file
                with open(toml_settings_file, "rb") as toml_file:
                    self.__login_info = tomllib.load(toml_file)
            else:
                json_settings_file = os.path.join(os.path.dirname(__file__), "settings", "login.json")
                # read login file
                with open(json_settings_file, "r", encoding="utf-8") as json_file:
                    self.__login_info = json.load(json_file)

            # get login credentials
            self.__pg_server = self.__login_info["cloud_pg" if cloud else "local_pg"]["host"]
            self.__pg_port = self.__login_info["cloud_pg" if cloud else "local_pg"]["port"]
            self.__pg_username = self.__login_info["cloud_pg" if cloud else "local_pg"]["user"]
            self.__pg_password = self.__login_info["cloud_pg" if cloud else "local_pg"]["password"]
            self.__ssl = self.__login_info["cloud_pg" if cloud else "local_pg"]["ssl"]
            # constants
            self.__encoding = "utf-8"
            self.__dt_schema = "dt"
            self.__sys_schema = "sys"
            self.__sql_schema = "sql_"
            self.__pg_default_db = "postgres"
            self.__sql_string_maximum = 100000000  # larger than 230,000,000 creates a string failure

            # used for translation step
            if single_exec:
                self.__semicolon_maximum = 1
            # used for debugging on processing - single sql statement from sql_[data_type] & data_source
            elif etl_debugger:
                self.__semicolon_maximum = 2
            # else normal operation
            else:
                self.__semicolon_maximum = 500  # to small ~< 100 and to big ~> 800 it becomes slower
            self.__reserved_keywords = ArrayList()

            # variables
            self.__pg_database = database
            self.__pg_schema = schema
            self.__sql_statements = ""
            self.__semicolon_count = 0
            self.__log_queue = ArrayList()
            self.__log_written = ArrayList()

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

            # check if database exists or not
            self.__pg_sql_cursor = self.__pg_sql_connection.cursor()
            self.__pg_sql_cursor.execute(f"select datname from pg_catalog.pg_database where datname = '{database}';")
            # create database if empty rowcount
            if self.__pg_sql_cursor.rowcount == 0:
                self.__pg_sql_cursor.execute(f'create database "{database}";')

            # get reserved keywords - constant
            self.__pg_sql_cursor.execute("select word from pg_get_keywords() where catcode in ('R', 'T');")
            for keyword in self.__pg_sql_cursor.fetchall():
                self.__reserved_keywords.append(keyword[0])

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

            # create sys.log if not exists
            self.__create_log()
        except Exception as error:
            raise error

    # Private
    def __check_log(self, check_string: str) -> bool:
        """Checks if check_string is in log ArrayLists"""
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

    def __create_log(self):
        """Creates the schema & table: sys.log"""
        try:
            sys_created = False

            # create sys schema
            self.__pg_cursor.execute(
                f"""
                    select schema_name
                    from information_schema.schemata
                    where schema_name = '{self.__sys_schema}';"""
            )
            if self.__pg_cursor.rowcount == 0:
                self.__pg_cursor.execute(f"create schema if not exists {self.__sys_schema};")
                sys_created = True

            log_created = False
            # create log table
            self.__pg_cursor.execute(
                f"""
                    select schemaname, relname, n_live_tup
                    from pg_stat_user_tables
                    where schemaname = '{self.__sys_schema}' and relname = 'log'"""
            )
            if self.__pg_cursor.rowcount == 0:
                self.__pg_cursor.execute(self.__sql_log())
                self.__pg_cursor.execute("drop schema if exists public cascade;")

            if sys_created:
                self.__log_queue.append(
                    f'{self.__sys_schema}~~schema created~{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                )
                self.__log_queue.append(
                    f'{self.__sys_schema}~log~table created~{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                )
                log_created = True
        except Exception as error:
            raise error

    def __create_table_statements(
        self, data_schema: str, table_name: str, sql_create: str, create_sql_schema: bool = True
    ):
        """ETL - Makes create table statements"""
        try:
            # sql_[data_type] schema
            if create_sql_schema:
                self.sql_queue_exec(self.__sql_schema_create(data_schema, table_name))

            # data schema
            self.sql_queue_exec(sql_create)
        except Exception as error:
            raise error

    def __drop_schema(self, schema: str):
        """Drop the schema"""
        try:
            self.__pg_cursor.execute(f"drop schema if exists {schema} cascade;")
        except Exception as error:
            raise error

    def __drop_table_statements(self, data_schema: str, table_name: str, create_sql_schema: bool = True) -> str:
        """ETL - Makes drop table statements"""
        try:
            # sql_[data_type] schema
            if create_sql_schema:
                self.sql_queue_exec(f"drop table if exists {self.__sql_schema + data_schema}.{table_name} cascade;")

            # data schema
            sql_drop = f"drop table if exists {data_schema}.{table_name} cascade;"
            self.sql_queue_exec(sql_drop)

            return sql_drop
        except Exception as error:
            raise error

    def __execute_sql_when_maximum(self, sql_line: str):
        """Executes the sql statements if maximum semicolons or string length"""
        try:
            # queue in log ArrayList
            self.__queue_log(sql_line)

            # set sql_statements and semicolon_count if under string maximum
            #   or current sql_line is greater than string maximum
            if len(self.__sql_statements) + len(sql_line) <= self.__sql_string_maximum or (
                self.__sql_statements == "" and len(sql_line) > self.__sql_string_maximum
            ):
                self.__sql_statements += sql_line
                self.__semicolon_count += 1
                sql_line = ""

            # semicolon count reaches max constant or string length max then execute sql string
            if (
                self.__semicolon_count == self.__semicolon_maximum
                or len(self.__sql_statements) + len(sql_line) > self.__sql_string_maximum
            ):
                self.__insert_log()
                self.__pg_cursor.execute(self.__sql_statements)
                self.__sql_statements = sql_line
                self.__semicolon_count = 1 if sql_line.endswith(";") else 0
        except Exception as error:
            raise error

    def __init_pg_sql_connection(self, database: str = "postgres"):
        """Initialize another connection for independent data usage"""
        try:
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
        except Exception as error:
            raise error

    def __insert_into_statements(self, data_schema: str, table_name: str, sql_row: str, create_sql_schema: bool = True):
        """ETL - Makes insert into statements"""
        try:
            # sql_[data_type] schema
            sql_insert = f"insert into {data_schema}.{table_name} values ({sql_row});"
            if create_sql_schema:
                self.sql_queue_exec(
                    f"""insert into {self.__sql_schema + data_schema}.{table_name}
                        (sorting, data) VALUES (3, {0});""".format(
                        sql_insert.replace("'", "''")
                    )
                )

            # data schema
            self.sql_queue_exec(sql_insert)
        except Exception as error:
            print(sql_insert)
            raise error

    def __insert_log(self):
        """Inserts db_schema, db_table, description & current date/time in the log table"""
        try:
            # loop thru log_queue and insert into pg sql
            while len(self.__log_queue) >= 1:
                # pop first record
                log_record = self.__log_queue.pop(0)
                # write popped record into self.__log_written
                self.__log_written.append(log_record)
                # log[0] = schema, log[1] = table, log[2] = description
                log = log_record.split("~")

                # inserts into log table from self.__log_queue
                self.__pg_cursor.execute(
                    f"""insert into {self.__sys_schema}.log
                        (db_schema, db_table, description, start)
                        values ('{log[0]}', '{log[1]}', '{log[2]}', '{log[3]}')"""
                )
        except Exception as error:
            raise error

    def __modify_column_name(self, column: str) -> str:
        """Adds an underscore after the column name if it's a reserved keyword in postgresql"""
        try:
            # replaces non-alphanumeric characters with an underscore
            column = re.sub("\\W+", "_", column)

            if self.__reserved_keywords.exists(column):
                column += "_"

            return column
        except Exception as error:
            raise error

    def __queue_log(self, sql_statement: str):
        """Adds log statements if it doesn't exist in ArrayList"""
        try:
            # creates time_stamp in yyyy-mm-dd hh:mm:ss format
            time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # only log sql queries that use a schema.table syntax
            sql_schema_table = re.findall("from \\w+\\.\\w+", str(sql_statement), flags=re.IGNORECASE)
            if len(sql_schema_table) > 1:
                sql_schema = re.sub("from ", "", sql_schema_table[0].split(".")[0], flags=re.IGNORECASE)
                sql_table = sql_schema_table[0].split(".")[1]

                # add to log_queue ArrayList when sql_schema & sql_table are not empty
                if sql_schema != "" and sql_table != "":
                    # create table
                    if str(sql_statement).lower().startswith("create table"):
                        description = "table created"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
                    # delete from
                    elif str(sql_statement).lower().startswith("delete from"):
                        description = "values deleted"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
                    # drop table
                    elif str(sql_statement).lower().startswith("drop table"):
                        description = "table dropped"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
                    # function
                    elif str(sql_statement).lower().startswith("drop function"):
                        description = "function dropped"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~function created~{time_stamp}")
                    # insert
                    elif str(sql_statement).lower().startswith("insert into"):
                        description = "values inserted"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
                    # update
                    elif str(sql_statement).lower().startswith("update"):
                        description = "values updated"
                        if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                            self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
            # renaming schema
            elif len(re.findall("alter schema \\w+ rename to \\w;", str(sql_statement), flags=re.IGNORECASE)) > 0:
                sql_schema = re.sub(
                    "alter schema ", "", str(sql_statement).split(" rename", maxsplit=1)[0], flags=re.IGNORECASE
                )
                sql_table = ""
                description = "schema renamed"
                if not self.__check_log(f"{sql_schema}~{sql_table}~{description}"):
                    self.__log_queue.append(f"{sql_schema}~{sql_table}~{description}~{time_stamp}")
        except Exception as error:
            raise error

    def __sql_data_transfer(self, schema: str) -> str:
        """Returns create table sql statement for data transfer"""
        try:
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
        except Exception as error:
            raise error

    def __sql_log(self) -> str:
        """Returns create table sql statement for log"""
        try:
            return f"""
                create table if not exists {self.__sys_schema}.log (
                    id serial,
                    db_schema varchar(63) default '',
                    db_table varchar(63) default '',
                    description varchar(50) default '',
                    start timestamp);"""
        except Exception as error:
            raise error

    def __sql_schema_create(self, schema: str, table: str) -> str:
        """Returns create table sql statement for sql_data_schema.table_name"""
        try:
            return f"""
                create table if not exists {self.__sql_schema + schema}.{table} (
                    id serial,
                    sorting int default 0,
                    data text default '');"""
        except Exception as error:
            raise error

    def __sql_table_setup(self, data_schema: str, table_name: str, sql_drop: str, sql_create: str):
        """ETL - Insert into sql_[data_schema] drop & create, delayed until sql_[data_type].table is setup"""
        try:
            # sql_[data_type] schema
            self.sql_queue_exec(
                f"""
                    insert into {self.__sql_schema + data_schema}.{table_name}
                    (sorting, data) values (1, '{0}');""".format(
                    sql_drop.replace("'", "''")
                )
            )
            self.sql_queue_exec(
                f"""
                    insert into {self.__sql_schema + data_schema}.{table_name}
                    (sorting, data) values (2, '{0}');""".format(
                    sql_create.replace("'", "''")
                ),
                execute=True,
            )
        except Exception as error:
            raise error

    def __transform_byte_data(self, byte_data: bytes) -> str:
        """ETL - Formats byte data for postgresql"""
        try:
            # converts bytes data into proper hex then into a string
            return f"'\\x{hexlify(byte_data).decode(self.__encoding)}'::bytea"
        except Exception as error:
            raise error

    def __transform_string_data(self, str_data: str) -> str:
        """ETL - Formats string data for postgresql"""
        try:
            # Remove spaces
            str_data = str_data.rstrip()

            # remove hexadecimal values - 20 thru 7E are valid
            str_data = re.sub(
                r"\x00|\x01|\x02|\x03|\x04|\x05|\x06|\x07|\x08|\x0B|\x0C|\x0E|\x0F",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            # str_data = re.sub(r"\x09", '\t', str_data, flags=re.IGNORECASE) # tab
            # str_data = re.sub(r"\x0A", '\n', str_data, flags=re.IGNORECASE) # newline / line feed
            # str_data = re.sub(r"\x0D", '\r', str_data, flags=re.IGNORECASE) # carriage return
            str_data = re.sub(
                r"\x10|\x11|\x12|\x13|\x14|\x15|\x16|\x17|\x18|\x19|\x1A|\x1B|\x1C|\x1D|\x1E|\x1F",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(r"\x5C", "\\\\\\\\", str_data, flags=re.IGNORECASE)  # backslash
            str_data = re.sub(r"\x7F", "", str_data, flags=re.IGNORECASE)  # delete
            str_data = re.sub(
                r"\x80|\x81|\x82|\x83|\x84|\x85|\x86|\x87|\x88|\x89|\x8A|\x8B|\x8C|\x8D|\x8E|\x8F",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\x90|\x91|\x92|\x93|\x94|\x95|\x96|\x97|\x98|\x99|\x9A|\x9B|\x9C|\x9D|\x9E|\x9F",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xA0|\xA1|\xA2|\xA3|\xA4|\xA5|\xA6|\xA7|\xA8|\xA9|\xAA|\xAB|\xAC|\xAD|\xAE|\xAF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xB0|\xB1|\xB2|\xB3|\xB4|\xB5|\xB6|\xB7|\xB8|\xB9|\xBA|\xBB|\xBC|\xBD|\xBE|\xBF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xC0|\xC1|\xC2|\xC3|\xC4|\xC5|\xC6|\xC7|\xC8|\xC9|\xCA|\xCB|\xCC|\xCD|\xCE|\xCF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xD0|\xD1|\xD2|\xD3|\xD4|\xD5|\xD6|\xD7|\xD8|\xD9|\xDA|\xDB|\xDC|\xDD|\xDE|\xDF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xE0|\xE1|\xE2|\xE3|\xE4|\xE5|\xE6|\xE7|\xE8|\xE9|\xEA|\xEB|\xEC|\xED|\xEE|\xEF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )
            str_data = re.sub(
                r"\xF0|\xF1|\xF2|\xF3|\xF4|\xF5|\xF6|\xF7|\xF8|\xF9|\xFA|\xFB|\xFC|\xFD|\xFE|\xFF",
                "",
                str_data,
                flags=re.IGNORECASE,
            )

            # replace single quote with two quotes - quote escaping for postgresql
            str_data = str_data.replace("'", "''")

            return str_data
        except Exception as error:
            raise error

    # Public
    def close(self):
        """Closes cursor & connection"""
        try:
            # pg
            self.__pg_cursor.close()
            self.__pg_connection.close()

            # psql
            self.__pg_sql_cursor.close()
            self.__pg_sql_connection.close()
        except Exception as error:
            raise error

    def create_data_transfer_schema(self, schema: str = "dt"):
        """Creates data transfer schema"""
        try:
            self.sql_queue_exec(self.__sql_data_transfer(schema), execute=True)
        except Exception as error:
            raise error

    def create_schema(self, schema: str):
        """Creates the schema if it doesn't exist"""
        try:
            schema_string = f"""
                select schema_name
                from information_schema.schemata
                where schema_name = '{schema}'"""
            if self.sql_count(schema_string) == 0:
                # sys.log inserting
                if schema != self.__sys_schema:
                    description = "schema created"
                    if not self.__check_log(f"{schema}~~{description}"):
                        self.__log_queue.append(
                            f'{schema}~~{description}~{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                        )

                # create schema
                self.__pg_cursor.execute(f"create schema if not exists {schema};")
        except Exception as error:
            raise error

    def create_system_defaults(self, schema: str = "dt", version: str = ""):
        """Creates all pgsql functions"""
        try:
            system_defaults_timer = Timer()

            # create sys schema
            self.create_schema(self.__sys_schema)

            # initialize connect to default database
            self.__init_pg_sql_connection()

            print("Creating sys functions in: ", end="", flush=True)
            # get all pgsql functions
            self.__pg_sql_cursor.execute(f"select * from {self.__sys_schema}.function")
            # creates function definition from pgsql.function
            for function in self.__pg_sql_cursor:
                if function[0] == "sql":
                    function_str = f"""
                        drop function if exists {self.__sys_schema}.{function[1]};
                            \ncreate function {self.__sys_schema}.{function[1]}({function[2]}) returns {function[3]}
                            \n\tlanguage {function[0]}
                            \n\timmutable
                            \n\t{function[5]}"""
                elif function[0] == "plpgsql":
                    function_str = f"""
                        drop function if exists {self.__sys_schema}.{function[1]};
                            \ncreate function {self.__sys_schema}.{function[1]}({function[2]})
                            \n\treturns {function[3]} language {function[0]} as $$
                            \ndeclare
                            \n\t{function[4]}
                            \nbegin
                            \n\t{function[5]}
                            \nend;
                            \n$$;"""
                # build function into sys schema
                self.sql_queue_exec(function_str)
            system_defaults_timer.print_time()
        except Exception as error:
            raise error
        finally:
            system_defaults_timer.print_time("Total system defaults created in: ")

    def etl_dataflex_data(
        self,
        path: str,
        file_name: str = "",
        data_schema: str = "dflex",
        remove_empty_tables: bool = True,
        ignore_tables: ArrayList = ArrayList(),
    ):
        """Processing dataflex data"""
        try:
            # total timing
            total_timer = Timer()

            print("Extracting, transforming & loading dataflex data...")

            # sql_[data_type] schema
            self.create_schema(self.__sql_schema + data_schema)
            # dataflex schema
            self.create_schema(data_schema)

            # set location
            location = DataLocation(path, file_name, ["vld"])

            # removes any tables from location list
            for table_name in ignore_tables:
                location.get_file_list().remove(table_name.lower())

            # loop thru file list
            for file_name_list in location.get_file_list():
                # table timing
                table_timer = Timer()

                table_name = file_name_list.split(".")[0]
                print(f"{table_name} completed in: ", end="", flush=True)

                # queue drop table statements
                sql_drop = self.__drop_table_statements(data_schema, table_name)

                # open table
                odbc_connection = pyodbc.connect(f"DRIVER={0};DBQ={path}".format("{DataFlex Driver}"))
                odbc_cursor = odbc_connection.cursor()
                odbc_cursor.execute(f"select * from {table_name}")

                # create table string - data schema
                sql_create = f"create table if not exists {data_schema}.{table_name}("
                # columns in description
                #   (name, type_code, display_size, internal_size, precision, scale, null_ok)
                for column in odbc_cursor.description:
                    # creates column name
                    sql_create += f'"{self.__modify_column_name(column[0].lower())}" '

                    # creates data type & size
                    if str(column[1]) == "<class 'str'>":
                        sql_create += f"varchar({column[3]}), "
                    elif str(column[1]) == "<class 'int'>":
                        sql_create += "integer, "
                    elif str(column[1]) == "<class 'decimal.Decimal'>":
                        sql_create += f"numeric({column[4]},{column[5]}), "
                    elif str(column[1]) == "<class 'datetime.date'>":
                        sql_create += "date NULL, "
                # remove last comma and space and replace with );
                sql_create = sql_create.rstrip(", ") + ");"
                # queue create table statements
                self.__create_table_statements(data_schema, table_name, sql_create)

                # write drop & create statments after sql_[data_type].table is setup
                self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

                # rows from odbc connection
                for row in odbc_cursor:
                    sql_row = ""
                    # columns from each row
                    for column in row:
                        # nulls
                        if column is None:
                            sql_row += "null, "
                        # numbers & boolean
                        elif isinstance(column, (bool, int, float, decimal.Decimal)):  #
                            sql_row += f"{column}, "
                        # bytes
                        elif isinstance(column, bytes):
                            sql_row += f"{self.__transform_byte_data(column)}, "
                        # strings & dates
                        else:
                            sql_row += f"E'{self.__transform_string_data(str(column))}', "
                    # remove last comma and space
                    sql_row = sql_row.rstrip(", ")
                    # queue insert into statements
                    self.__insert_into_statements(data_schema, table_name, sql_row)

                # execute anything left in the queue
                self.sql_queue_exec(execute=True)

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
                total_timer.print_time("Total conversion of dataflex data completed in: ")
        except Exception as error:
            raise error

    def etl_fox_pro_data(
        self,
        path: str,
        file_name: str = "",
        data_schema: str = "vfp",
        remove_empty_tables: bool = True,
        ignore_tables: ArrayList = ArrayList(),
    ):
        """Processing fox pro data"""
        try:
            # constants
            default_encoding = "cp1252"

            # total timing
            total_timer = Timer()

            if normal_fox_pro_operation:
                print("Extracting, transforming & loading fox pro data...")

            # sql_[data_type] schema
            self.create_schema(self.__sql_schema + data_schema)
            # fox pro schema
            self.create_schema(data_schema)

            # set location
            location = DataLocation(path, file_name, ["dbf"])

            # removes any tables from location list
            for table_name in ignore_tables:
                location.get_file_list().remove(table_name.lower())

            # loop thru file list
            for file_name_list in location.get_file_list():
                # table timing
                table_timer = Timer()

                table_name = file_name_list.split(".")[0]
                print(f"{table_name} completed in: ", end="", flush=True)

                # queue drop table statements
                sql_drop = self.__drop_table_statements(data_schema, table_name, normal_fox_pro_operation)

                # open table
                dbf_cursor = dbfread.DBF(
                    filename=os.path.join(path, file_name_list),
                    encoding=default_encoding,
                    lowernames=True,
                    load=True,
                )

                # create table string - data schema
                sql_create = f"create table if not exists {data_schema}.{table_name}("

                table_fields = ArrayList()

                # get field definitions
                for field in dbf_cursor.fields:
                    if field.type in ("C", "V"):
                        sql_create += f'"{self.__modify_column_name(field.name)}" '
                        sql_create += f"varchar({field.length})" + " not NULL default '', "
                        table_fields.append("string")
                    elif field.type == "L":
                        sql_create += f'"{self.__modify_column_name(field.name)}" boolean not NULL default false, '
                        table_fields.append("boolean")
                    elif field.type == "D":
                        sql_create += f'"{self.__modify_column_name(field.name)}" date NULL, '
                        table_fields.append("date")
                    elif field.type in ("T", "@"):
                        sql_create += f'"{self.__modify_column_name(field.name)}" timestamp NULL, '
                        table_fields.append("date")
                    elif field.type in ("I", "+"):
                        sql_create += f'"{self.__modify_column_name(field.name)}" integer not NULL default 0, '
                        table_fields.append("number")
                    elif field.type == "N":
                        sql_create += f'"{self.__modify_column_name(field.name)}" '
                        sql_create += f"numeric({field.length},{field.decimal_count}) not NULL default 0, "
                        table_fields.append("decimal")
                    elif field.type == "F":
                        sql_create += f'"{self.__modify_column_name(field.name)}" float4 not NULL default 0, '
                        table_fields.append("number")
                    elif field.type == "B":
                        sql_create += f'"{self.__modify_column_name(field.name)}" float8 not NULL default 0, '
                        table_fields.append("number")
                    elif field.type == "Y":
                        sql_create += f'"{self.__modify_column_name(field.name)}" '
                        sql_create += f"decimal({field.length + field.decimal_count},{field.decimal_count}) "
                        sql_create += "not NULL default 0, "
                        table_fields.append("decimal")
                    elif field.type == "M":
                        sql_create += f'"{self.__modify_column_name(field.name)}" ' + "text not NULL default '', "
                        table_fields.append("string")
                    else:
                        table_fields.append("invalid")

                # remove last comma and space and replace with );
                sql_create = sql_create.rstrip(", ") + ");"
                # queue create table statements
                self.__create_table_statements(data_schema, table_name, sql_create, normal_fox_pro_operation)

                # write drop & create statments after sql_[data_type].table is setup
                self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

                record_number = 1

                # loop thru data rows
                for row in dbf_cursor.records:
                    sql_row = ""

                    column_count = 0
                    # loop thru each column
                    for column in ArrayList(row.items()):
                        if table_fields[column_count] != "invalid":
                            # boolean
                            if table_fields[column_count] == "boolean":
                                if str(column[1]) == "True":
                                    sql_row += f'{"true"}, '
                                else:
                                    sql_row += f'{"false"}, '
                            # int / float
                            elif table_fields[column_count] == "number":
                                if column[1] is not None:
                                    sql_row += f"{str(column[1]).strip()}, "
                                else:
                                    sql_row += f"{str(0)}, "
                            # numeric / decimal
                            elif table_fields[column_count] == "decimal":
                                if column[1] is None:
                                    sql_row += f"{0}, "
                                # vfp can do 1,000,000.000 is valid in numeric(10, 4)
                                #   but pgsql can only have 999,999.9999
                                elif (
                                    len(str(column[1]).split(".", maxsplit=1)[0])
                                    > dbf_cursor.fields[column_count].length
                                    - dbf_cursor.fields[column_count].decimal_count
                                ):
                                    sql_row += f"""
                                            {float('9' * (dbf_cursor.fields[column_count].length
                                                - dbf_cursor.fields[column_count].decimal_count)
                                            + '.' + '9' * dbf_cursor.fields[column_count].decimal_count)}, """
                                else:
                                    sql_row += f"""
                                            {str(column[1]).strip()[0 : dbf_cursor.fields[column_count].length]}, """
                            # string
                            elif table_fields[column_count] == "string":
                                if column[1] is not None:
                                    sql_row += f"E'{self.__transform_string_data(str(column[1]))}', "
                                else:
                                    sql_row += "'', "
                            # nulls
                            elif column[1] is None:
                                sql_row += "null, "
                            # date / time
                            elif table_fields[column_count] == "date":
                                sql_row += f"'{column[1]}', "
                        column_count += 1

                    # remove last comma and space
                    sql_row = sql_row.rstrip(", ")
                    # queue insert into statements
                    self.__insert_into_statements(data_schema, table_name, sql_row, normal_fox_pro_operation)

                # execute anything left in the queue
                self.sql_queue_exec(execute=True)

                # close table
                dbf_cursor = ""

                # print table timer
                table_timer.print_time()

            # vacuums
            self.sql_vacuum()
            # only run when multiple files
            if len(location.get_file_list()) > 1:
                # removes 0 record tables
                self.sql_remove_empty_tables(data_schema, remove_empty_tables)
                # print total timer
                total_timer.print_time("Total conversion of fox pro data completed in: ")
        except Exception as error:
            raise error

    def etl_mysql_data(
        self,
        my_database: str,
        table: str = "",
        data_schema: str = "mysql",
        remove_empty_tables: bool = True,
        ignore_tables: ArrayList = ArrayList(),
    ):
        """Processing mysql data"""
        try:
            # total timing
            total_timer = Timer()

            print("Extracting, transforming & loading mysql data...")

            # sql_[data_type] schema
            self.create_schema(self.__sql_schema + data_schema)
            # mysql schema
            self.create_schema(data_schema)

            # open database
            mysql_connection = pymysql.connect(
                host=self.__login_info["local_mysql"]["host"],
                database=my_database,
                user=self.__login_info["local_mysql"]["user"],
                password=self.__login_info["local_mysql"]["password"],
            )
            mysql_cursor = mysql_connection.cursor()

            # get all tables from mssql
            tables_sql_statement = f"""
                select
                    lower(table_schema) as table_schema,
                    lower(table_name) as table_name,
                    table_rows
                from
                    information_schema.tables
                where
                    table_schema = '{my_database}' /*and table_type = 'BASE TABLE'*/
                order by
                    lower(table_schema),
                    lower(table_name);"""
            mysql_cursor.execute(tables_sql_statement)

            # add to my_tables from mysql
            my_tables = ArrayList()
            for my_table in mysql_cursor:
                my_tables.append(f"{my_table[0]}.{my_table[1]}")

            # removes any tables from tables list
            for table_name in ignore_tables:
                my_tables.remove(table_name.lower())

            # when table variable is used
            if table != "":
                if my_tables.exists(table):
                    my_tables.clear()
                    my_tables.append(table)
                else:
                    print(f"Table doesn't exist in database: {table}")

            # loop thru table list
            for table_name in my_tables:
                # table timing
                table_timer = Timer()

                # get schema_name & table_name
                if table_name.count(".") != 0:
                    schema_name = table_name.split(".")[0]
                    table_name = table_name.split(".")[1]

                print(f"{table_name} completed in: ", end="", flush=True)

                # queue drop table statements
                sql_drop = self.__drop_table_statements(data_schema, table_name)

                # get table schema
                mysql_cursor.execute(
                    f"""
                        SELECT
                            ordinal_position,
                        lower(column_name) as column_name,
                            case
                                when data_type = 'bool' then 'boolean'
                                when data_type = 'tinyint' then 'smallint'
                                when data_type = 'mediumint' then 'int'
                                when data_type = 'float' then 'real'
                                when data_type = 'double' then 'double precision'
                                when data_type = 'time' then 'interval hour to second'
                                when data_type = 'datetime' then 'timestamp without time zone'
                                when data_type = 'timestamp' then 'timestamp with time zone'
                                when data_type = 'tinytext' then 'varchar(255)'
                                when left(data_type, 6) = 'binary' or left(data_type, 9) = 'varbinary' then 'bytea'
                                else data_type end as data_type,
                            case when is_nullable = 'YES' then 'True' else 'False' end as is_nullable
                        from
                            information_schema.columns
                        where
                            table_schema = '{schema_name}' and table_name = '{table_name}'
                        order by
                            lower(table_name), ordinal_position;"""
                )

                # create table string - data schema
                sql_create = f"create table if not exists {data_schema}.{table_name}("

                # columns (ordinal_position, column_name, data_type, is_nullable)
                for column in mysql_cursor:
                    sql_create += f"""{self.__modify_column_name(column[1])}
                                        {column[2]}{"" if column[3] == "True" else " NOT NULL"}, """
                # remove last comma and space and replace with );
                sql_create = sql_create.rstrip(", ") + ");"
                # queue create table statements
                self.__create_table_statements(data_schema, table_name, sql_create)

                # write drop & create statments after sql_[data_type].table is setup
                self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

                # get data from table_name
                mysql_cursor.execute(f"select * from {schema_name}.{table_name};")

                # row from mssql connection
                for row in mysql_cursor:
                    sql_row = ""
                    # column from each row
                    for column in row:
                        # nulls
                        if column is None:
                            sql_row += "null, "
                        # numbers & boolean
                        elif isinstance(column, (bool, int, float, decimal.Decimal)):  #
                            sql_row += f"{column}, "
                        # bytes
                        elif isinstance(column, bytes):
                            sql_row += f"{self.__transform_byte_data(column)}, "
                        # strings & dates
                        else:
                            sql_row += f"E'{self.__transform_string_data(str(column))}', "
                    # remove last comma and space
                    sql_row = sql_row.rstrip(", ")
                    # queue insert into statements
                    self.__insert_into_statements(data_schema, table_name, sql_row)

                # execute anything left in the queue
                self.sql_queue_exec(execute=True)

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
            mysql_cursor.close()
            mysql_connection.close()

            if len(my_tables) > 1:
                # print total timer
                total_timer.print_time("Total conversion of mysql data completed in: ")
        except Exception as error:
            raise error

    def etl_spreadsheet_data(
        self,
        path: str,
        file_name: str = "",
        data_schema: str = "excel",
        file_table_name: bool = False,
        sheet_table_name: bool = True,
        header_line: int = 1,
        bottom_lines_skipped: int = 0,
        delimiter_char=",",
        ignore_files: ArrayList = ArrayList(),
        ignore_tables: ArrayList = ArrayList(),
    ):
        """Processing spreadsheet data"""
        try:
            # total timing
            total_timer = Timer()

            print("Extracting, transforming & loading spreadsheet data...")

            # sql_[data_type] schema
            self.create_schema(self.__sql_schema + data_schema)
            # spreadsheet schema
            self.create_schema(data_schema)

            # set location
            location = DataLocation(path, file_name, ["csv", "tsv", "xlsx", "xlsm", "xls"])

            # removes any files from location list
            for file_name_list in ignore_files:
                location.get_file_list().remove(file_name_list.lower())

            # loop thru file list
            for file_name_list in location.get_file_list():
                # table timing
                table_timer = Timer()

                # open file
                spreadsheet_cursor = pyexcel.get_book(
                    file_name=os.path.join(path, file_name_list),
                    encoding=self.__encoding,
                    delimiter=delimiter_char,
                )

                # get list of work_sheets and remove any from ignore_tables
                work_sheets = ArrayList(spreadsheet_cursor.sheet_names())
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
                    table_name = ""
                    if file_table_name:
                        table_name = re.sub(
                            "\\W",
                            "_",
                            os.path.splitext(file_name_list)[0].lower() + f"{'_' if sheet_table_name else ''}",
                        )
                    # set table_name from sheet_name regardless
                    table_name += re.sub("\\W", "_", work_sheet.lower())

                    # get column names in work_sheet
                    column_names = ArrayList()
                    for column_index in range(number_of_columns):
                        # uses column_# for names if no header_line used
                        if header_line == 0:
                            column_names.append("column_" + str(column_index + 1))
                        else:
                            # trims column names and replace whitespace characters with underscore
                            new_column_name = re.sub(
                                "\\W",
                                "_",
                                str(spreadsheet_cursor[work_sheet].cell_value(header_line - 1, column_index)).strip(),
                            )
                            # uses column_# if empty column name
                            if new_column_name == "":
                                column_names.append("column_" + str(column_index + 1))
                            # uses column name plus _# if column name is already used once
                            elif column_names.exists(new_column_name):
                                column_names.append(new_column_name + "_" + str(column_index + 1))
                            # uses column name in spreadsheet
                            else:
                                column_names.append(new_column_name)

                    # setup blank ArrayLists
                    column_types = ArrayList([""] * number_of_columns)
                    column_sizes = ArrayList([0] * number_of_columns)

                    # column types & sizes checking
                    for row_index in range(header_line, number_of_rows - bottom_lines_skipped):
                        # column specifications
                        for column_index in range(number_of_columns):
                            testing_value = spreadsheet_cursor[work_sheet].cell_value(row_index, column_index)

                            # boolean
                            if (
                                isinstance(testing_value, bool)
                                or str(testing_value).lower() == "false"
                                or str(testing_value).lower() == "true"
                            ):
                                column_types[column_index] = "boolean"
                            # date
                            elif isinstance(testing_value, datetime.date) and column_types[column_index] != "boolean":
                                column_types[column_index] = "date"
                            # string
                            elif (
                                isinstance(testing_value, str)
                                and column_types[column_index] != "boolean"
                                and column_types[column_index] != "date"
                                and testing_value != ""
                            ):
                                column_types[column_index] = "varchar"
                            # numeric
                            elif (
                                isinstance(testing_value, float)
                                and column_types[column_index] != "boolean"
                                and column_types[column_index] != "date"
                                and column_types[column_index] != "varchar"
                            ):
                                column_types[column_index] = "numeric"
                            # integer or bigint
                            elif (
                                isinstance(testing_value, int)
                                and column_types[column_index] != "boolean"
                                and column_types[column_index] != "date"
                                and column_types[column_index] != "varchar"
                                and column_types[column_index] != "numeric"
                            ):
                                if abs(int(testing_value)) <= 2147483647 and column_types[column_index] != "bigint":
                                    column_types[column_index] = "integer"
                                else:
                                    column_types[column_index] = "bigint"

                            # set the maximum length size for each column
                            if column_sizes[column_index] < len(str(testing_value)):
                                column_sizes[column_index] = len(str(testing_value))

                    # queue drop table statements
                    sql_drop = self.__drop_table_statements(data_schema, table_name)

                    # create table string - data schema
                    sql_create = f"create table if not exists {data_schema}.{table_name}("

                    # column statements for table creation
                    for column_index in range(number_of_columns):
                        # column name
                        sql_create += f'"{column_names[column_index]}" '

                        # column types & size
                        # string
                        if column_types[column_index] == "varchar":
                            sql_create += f"varchar({column_sizes[column_index]}) default ''"
                        # integer or bigint
                        elif column_types[column_index] == "integer" or column_types[column_index] == "bigint":
                            sql_create += f"{column_types[column_index]} default 0"
                        # numeric
                        elif column_types[column_index] == "numeric":
                            sql_create += "float default 0"
                        # boolean
                        elif column_types[column_index] == "boolean":
                            sql_create += "boolean default 'false'"
                        # date
                        elif column_types[column_index] == "date":
                            sql_create += "timestamp"

                        # separator or ending
                        if column_index + 1 != number_of_columns:
                            sql_create += ", "
                        else:
                            sql_create += ");"

                    # queue create table statements
                    self.__create_table_statements(data_schema, table_name, sql_create)

                    # write drop & create statments after sql_[data_type].table is setup
                    self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

                    # process data rows
                    for row_index in range(header_line, number_of_rows - bottom_lines_skipped):
                        sql_row = ""

                        # column from each row
                        for column_index in range(number_of_columns):
                            cell_value = spreadsheet_cursor[work_sheet].cell_value(row_index, column_index)

                            # boolean
                            if column_types[column_index] == "boolean":
                                if str(cell_value).lower() == "true":
                                    sql_row += str(True) + ", "
                                else:
                                    sql_row += str(False) + ", "
                            # integer, bigint & numeric
                            elif (
                                column_types[column_index] == "integer"
                                or column_types[column_index] == "bigint"
                                or column_types[column_index] == "numeric"
                            ):
                                if str(cell_value) == "":
                                    sql_row += str(0) + ", "
                                else:
                                    sql_row += str(cell_value) + ", "
                            # dates
                            elif column_types[column_index] == "date":
                                if str(cell_value).strip() == "":
                                    sql_row += "null, "
                                else:
                                    sql_row += f"'{str(cell_value).strip()}', "
                            # strings
                            elif column_types[column_index] == "varchar":
                                sql_row += f"E'{self.__transform_string_data(str(cell_value))}', "
                        # remove last comma and space
                        sql_row = sql_row.rstrip(", ")
                        # queue insert into statements
                        self.__insert_into_statements(data_schema, table_name, sql_row)

                    # execute anything left in the queue
                    self.sql_queue_exec(execute=True)

                    # print table timer
                    table_timer.print_time()

            # vacuums
            self.sql_vacuum()

            # print total timer
            total_timer.print_time("Total conversion of spreadsheet data completed in: ")
        except Exception as error:
            raise error

    def etl_sql_server_data(
        self,
        ms_database: str,
        table: str = "",
        data_schema: str = "mssql",
        remove_empty_tables: bool = True,
        ignore_tables: ArrayList = ArrayList(),
    ):
        """Processing sql server data"""
        try:
            # constants
            # default_encoding = "utf-16"

            # total timing
            total_timer = Timer()

            print("Extracting, transforming & loading sql server data...")

            # sql_[data_type] schema
            self.create_schema(self.__sql_schema + data_schema)
            # sql server schema
            self.create_schema(data_schema)

            # open database
            mssql_connection = pymssql.connect(
                host=self.__login_info["local_mssql"]["host"],
                server=self.__login_info["local_mssql"]["server"],
                database=ms_database,
                user=self.__login_info["local_mssql"]["user"],
                password=self.__login_info["local_mssql"]["password"],
                # autocommit = True,
                as_dict=False,
            )
            mssql_cursor = mssql_connection.cursor()

            # get all tables from mssql
            tables_sql_statement = """
                select
                    schema_name(tables.schema_id) as schema_name,
                    lower(tables.[name]) as table_name,
                    sum(partitions.rows) as row_count
                from
                    sys.tables
                        left outer join sys.partitions on tables.object_id = partitions.object_id
                group by
                    tables.schema_id, lower(tables.[name])
                --having
                    --sum(partitions.rows) != 0
                order by
                    tables.schema_id, lower(tables.[name]);"""
            mssql_cursor.execute(tables_sql_statement)

            # add to ms_tables from mssql
            ms_tables = ArrayList()
            for ms_table in mssql_cursor:
                if ms_table[0] == "dbo":
                    ms_tables.append(ms_table[1])
                else:
                    ms_tables.append(f"{ms_table[0]}.{ms_table[1]}")

            # removes any tables from tables list
            for table_name in ignore_tables:
                ms_tables.remove(table_name.lower())

            # when table variable is used
            if table != "":
                if ms_tables.exists(table):
                    ms_tables.clear()
                    ms_tables.append(table)
                else:
                    print(f"Table doesn't exist in database: {table}")

            # loop thru table list
            for table_name in ms_tables:
                # table timing
                table_timer = Timer()

                # get schema_name & table_name
                if table_name.count(".") != 0:
                    schema_name = table_name.split(".")[0]
                    table_name = table_name.split(".")[1]
                else:
                    schema_name = "dbo"

                print(f"{table_name} completed in: ", end="", flush=True)

                # queue drop table statements
                sql_drop = self.__drop_table_statements(data_schema, table_name)

                # get table schema
                mssql_cursor.execute(
                    f"""
                        select
                            --lower(tables.[name]) as table_name,
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
                        from
                            sys.tables
                                inner join sys.columns on tables.object_id = columns.object_id
                                inner join sys.types on columns.system_type_id = types.system_type_id and lower(types.[name]) != 'sysname'
                        where
                            tables.[name] = '{table_name}'
                        order by
                            tables.[name], columns.column_id;"""
                )

                # create table string - data schema
                sql_create = f"create table if not exists {data_schema}.{table_name}("

                # columns (column_id, column_name, data_type, is_nullable)
                for column in mssql_cursor:
                    sql_create += f"{self.__modify_column_name(column[1])} {column[2]}"
                    if column[3] == "True":
                        sql_create += ", "
                    else:
                        sql_create += " NOT NULL, "
                # remove last comma and space and replace with );
                sql_create = sql_create.rstrip(", ") + ");"
                # queue create table statements
                self.__create_table_statements(data_schema, table_name, sql_create)

                # write drop & create statments after sql_[data_type].table is setup
                self.__sql_table_setup(data_schema, table_name, sql_drop, sql_create)

                # get data from table_name
                mssql_cursor.execute(f"select * from {schema_name}.{table_name};")

                # row from mssql connection
                for row in mssql_cursor:
                    sql_row = ""
                    # column from each row
                    for column in row:
                        # nulls
                        if column is None:
                            sql_row += "null, "
                        # numbers & boolean
                        elif isinstance(column, (bool, int, float, decimal.Decimal)):
                            sql_row += f"{column}, "
                        # bytes
                        elif isinstance(column, bytes):
                            sql_row += f"{self.__transform_byte_data(column)}, "
                        # strings & dates
                        else:
                            sql_row += f"E'{self.__transform_string_data(str(column))}', "
                    # remove last comma and space
                    sql_row = sql_row.rstrip(", ")
                    # queue insert into statements
                    self.__insert_into_statements(data_schema, table_name, sql_row)

                # execute anything left in the queue
                self.sql_queue_exec(execute=True)

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
                total_timer.print_time("Total conversion of sql server data completed in: ")
        except Exception as error:
            raise error

    def read_sql_file(self, path: str, file_name: str = "", ignore_tables: ArrayList = ArrayList()):
        """Read sql file data"""
        try:
            # total timing
            total_timer = Timer()

            # set location
            location = DataLocation(path, file_name, ["sql"])

            # removes any tables from location list
            for table_name in ignore_tables:
                location.get_file_list().remove(table_name.lower())

            read_sql_files_normal_operation = file_name.startswith("default_") is False

            if read_sql_files_normal_operation:
                if len(location.get_file_list()) == 1:
                    print("Reading sql file...")
                else:
                    print("Reading sql files...")

            # loop thru file list
            for file_name_list in location.get_file_list():
                # table timer
                table_timer = Timer()
                print(f"  {file_name_list.split('.')[0]} completed in: ", end="", flush=True)

                # open current sql file
                with open(os.path.join(path, file_name_list), "r", encoding=self.__encoding) as sql_file:
                    build_sql_statement = ""

                    # reads file line by line
                    for read_line in sql_file:
                        # incomplete sql statement - add to build_sql_statement
                        if read_line.count(";") == 0 and len(read_line) > 0:
                            build_sql_statement += read_line
                        # complete sql statement - send to queue
                        elif read_line.count(";") >= 1:
                            self.sql_queue_exec(build_sql_statement + read_line)
                            build_sql_statement = ""

                    # execute anything left in the queue
                    self.sql_queue_exec(execute=True)

                    # print table timer
                    table_timer.print_time()

            # print total timer
            if read_sql_files_normal_operation and len(location.get_file_list()) > 1:
                total_timer.print_time("SQL files completed in: ")
        except Exception as error:
            raise error

    def rename_schema(self, new_schema: str):
        """Renames current schema to new schema name"""
        try:
            # sys.log inserting
            # self.__insert_log(self.__pg_schema, str(new_schema), 'schema renamed')

            sql = f"alter schema {self.__pg_schema} rename to {new_schema};"
            self.__queue_log(sql)
            self.__insert_log()
            self.__pg_cursor.execute(sql)
        except Exception as error:
            raise error

    def sql_count(self, sql_statement: str = "", printing: bool = False, print_str: str = "") -> int:
        """Return or print to screen the count in sql query"""
        try:
            # execute query
            if sql_statement != "":
                self.__pg_cursor.execute(str(sql_statement))

            # print to screen the count
            if printing and self.__pg_cursor.rowcount > 0:
                if self.__pg_cursor.rowcount == 1:
                    print(f"\t{self.__pg_cursor.rowcount} row: {print_str}")
                else:
                    print(f"\t{self.__pg_cursor.rowcount} rows: {print_str}")
            # elif not printing:
            return self.__pg_cursor.rowcount
        except Exception as error:
            raise error

    def sql_all_records(self, sql_statement: str = "") -> list:
        """Returns all records"""
        try:
            # execute query
            if sql_statement != "":
                self.__pg_cursor.execute(str(sql_statement))

            # returns all records
            return self.__pg_cursor.fetchall()
        except Exception as error:
            raise error

    def sql_refresh_data(self, schema: str = "", table: str = "", ignore_tables: ArrayList = ArrayList()):
        """Refreshes complete schema or single table from sql_[data_type].table"""
        try:
            # total timing
            refresh_timer = Timer()

            print("Refreshing data...")

            # add single table to tables list
            tables = ArrayList([table])

            # initialize connect to current database for sql_[data_type].table data
            self.__init_pg_sql_connection(self.__pg_database)

            # complete schema refresh
            if table == "":
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
                print(f"  {table_name} completed in: ", end="", flush=True)

                # get data from sql_[data_type].table
                self.__pg_sql_cursor.execute(
                    f"select data from {self.__sql_schema + schema}.{table_name} order by sorting, id;"
                )

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
            refresh_timer.print_time("Data refreshed in: ")
        except Exception as error:
            raise error

    def sql_remove_empty_tables(self, schema: str, remove_empty_tables: bool = True):
        """Removes any table with 0 records"""
        try:
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

                    remove_timer.print_time("Removed empty tables in: ")
        except Exception as error:
            raise error

    def sql_next_record(self) -> list:
        """Returns next record"""
        try:
            return self.__pg_cursor.fetchone()
        except Exception as error:
            raise error

    def sql_queue_exec(self, sql_statement: str = "", execute: bool = False):
        """Executes when execute is set or when queued sql statements reaches max semicolon or max string length"""
        try:
            if sql_statement != "":
                # add semicolon to end of sql_statements
                if not (sql_statement.endswith(";") or sql_statement.endswith(";/n")):
                    sql_statement += ";"

                # checking semicolon count & max string length
                self.__execute_sql_when_maximum(sql_statement)

            # execute all sql_statements
            if execute:
                # execute the remainder of sql strings - less than semicolon_maximum
                if self.__sql_statements.count(";") > 0:
                    self.__insert_log()
                    self.__pg_cursor.execute(self.__sql_statements)
                    self.__sql_statements = ""
        except Exception as error:
            raise error

    def sql_vacuum(self, full: bool = False, analyze: bool = True, printing: bool = True):
        """Vacuums with full, analyze"""
        try:
            if printing:
                vacuum_timer = Timer()
                print("Vacuum completed in: ", end="", flush=True)

            # building sql_statement string from parameters
            sql_statement = "vacuum"
            if full:
                sql_statement += " full"
            if analyze:
                sql_statement += " analyze"

            # execute
            self.__pg_cursor.execute(sql_statement)

            if printing:
                vacuum_timer.print_time()
        except Exception as error:
            raise error
