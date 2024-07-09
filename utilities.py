""" Utilities """

# standard library imports
import os
import time
import logging

class ArrayList(list):
    """ Extends list base class """

    def __init__(self, iterable=None):

        if iterable is None:
            iterable = []

        super().__init__(item for item in iterable)

    def exists(self, item) -> bool:
        """ Returns True if value is in the list """

        for element in super().__iter__():
            if element == item:
                return True

        return False

    def string_list(self) -> str:
        """ Returns a string of every element in list as a comma separated list """

        string_list = ''
        index = 0
        for element in super().__iter__():
            if index != 0:
                string_list += '', ''

            if isinstance(element, (float, int)):
                string_list += f'{element:,}'
            else:
                string_list += f'{element}'

            index += 1

        return string_list

    def remove(self, item) -> bool:
        """ Overrides removes if item doesn't exists no error """

        if self.exists(item):
            super().remove(item)
            return True

        return False

    def sort(self, reverse: bool = False, key: callable = None):
        """ Overrides sort so parameter names don't need to be passed """

        if isinstance(reverse, bool) and key is not None:
            super().sort(reverse=reverse, key=key)

        elif isinstance(reverse, bool):
            super().sort(reverse=reverse)


class DataLocation:
    """ Data Location """

    def __init__(self, path: str, file: str = '', file_extension: ArrayList = ArrayList()):

        # create empty variables
        self.__path = ''
        self.__file_list = ArrayList()
        self.__file_extension = ArrayList(file_extension)

        # valid path
        if os.path.isdir(path):
            self.__path = path

            # single valid file
            if str(file) != '':
                if os.path.exists(os.path.join(path, file)):
                    self.__file_list.append(file.lower())
                # invalid file
                else:
                    print(f'Invalid file: {path}')

            # multiple valid files
            else:
                for file_list in os.listdir(path):
                    # skip directories
                    if len(file_list.split('.')) > 1:
                        # check if file extension is in the list
                        if self.__check_file_extension(file_list.split('.')[-1]):
                            self.__file_list.append(file_list.lower())
                self.__file_list.sort()
        # invalid path
        else:
            print(f'Invalid path: {path}')

    def __check_file_extension(self, file_ext: str) -> bool:
        """ Returns True if extension is in extension list """

        # loop thru file extension list
        for file_extension in self.__file_extension:

            # returns True for match
            if file_extension.lower() == file_ext.lower():
                return True

        # else returns False
        return False

    def get_file_list(self) -> ArrayList:
        """ Returns file list """

        return self.__file_list

    def get_path(self) -> str:
        """ Returns path string """

        return self.__path


class Logger(logging.Logger):
    """ Extends logging class """

    def __init__(self, file_name: str, message_format: str = '%(asctime)s - %(message)s', 
                date_format: str = '%Y-%m-%d %H:%M:%S', log_level: enumerate = logging.INFO):

        super().__init__(file_name, logging.DEBUG)

        self.__formatter = logging.Formatter(message_format, datefmt=date_format)
        
        self.__handler = logging.FileHandler(file_name)
        self.__handler.setFormatter(self.__formatter)
        
        self.__log_file = logging.getLogger(file_name)
        self.__log_file.setLevel(log_level)
        self.__log_file.addHandler(self.__handler)

    def get_log(self):
        """ Send log file instance """

        return self.__log_file


class Timer:
    """ Print the time between beginning and end of the timer """

    def __init__(self):
        self.__start_time = time.time()

    # Private
    def __build_time_string(self, complete_time_string: str, time_integer: int, time_string: str) -> str:
        """ Builds the hours, minutes & seconds piece of the string """

        return (
            f'{complete_time_string}'
            # adds a space to the end if complete_time_string not empty
            + ('' if str(complete_time_string) == '' else ' ')
            + f'{time_integer} {time_string}'
            # adds 's' to time_string if time_integer != 1
            + ('' if time_integer == 1 else 's')
        )

    def __calculate_time_string(self) -> str:
        """ Calcuates the hours, minutes & seconds """

        end_time = time.time()
        total_time = round(end_time - self.__start_time)
        hours = 0
        minutes = 0
        seconds = 0
        time_string = ''

        # calculate hours, minutes & seconds
        # hours
        if total_time > 3600:
            hours = int(total_time / 3600)
            total_time = total_time - 3600 * hours
            time_string = self.__build_time_string(time_string, hours, 'hour')

        # minutes
        if total_time > 60:
            minutes = int(total_time / 60)
            total_time = total_time - 60 * minutes
            time_string = self.__build_time_string(time_string, minutes, 'minute')

        # seconds
        seconds = total_time
        time_string = self.__build_time_string(time_string, seconds, 'second')

        return time_string

    # Public
    def return_time(self, print_string: str = '') -> str:
        """ Returns the time from start for this timer """

        if print_string != '':
            print_string += ' '

        return str(print_string) + self.__calculate_time_string()

    def print_time(self, print_string: str = ''):
        """ Prints the time from start for this timer """

        if print_string != '':
            print_string += ' '

        print(f'{print_string}{self.__calculate_time_string()}')
