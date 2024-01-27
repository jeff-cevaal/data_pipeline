# standard library imports
import os
import time

# Extends list base class
class array_list(list):
	# Init Constructor
	def __init__(self, iterable=[]):
		try:
			super().__init__(item for item in iterable)
		except Exception as error:
			raise error

	# Returns True if value is in the list
	def exists(self, item) -> bool:
		try:
			for element in super().__iter__():
				if element == item:
					return True
			return False
		except Exception as error:
			raise error

	# Returns a string of every element in list as a comma separated list
	def string_list(self) -> str:
		try:
			string_list = ''
			index = 0
			for element in super().__iter__():
				if index != 0:
					string_list += ' ~ '
				if isinstance(element, int) or isinstance(element, float):
					string_list += '{:,}'.format(element)
				else:
					string_list += '{}'.format(element)
				index += 1
			return string_list
		except Exception as error:
			raise error
	
	# Overrides removes if item doesn't exists no error
	def remove(self, item) -> bool:
		try:
			if self.exists(item):
				super().remove(item)
				return True
			else:
				return False
		except Exception as error:
			raise error

	# Overrides sort so parameter names don't need to be passed
	def sort(self, reverse:bool=False, key:callable=None):
		try:
			if isinstance(reverse, bool) and key is not None:
				super().sort(reverse=reverse, key=key)
			elif isinstance(reverse, bool):
				super().sort(reverse=reverse)
		except Exception as error:
			raise error


# Data Location
class data_location:
	# Init Constructor
	def __init__(self, path:str, file:str='', file_extension:array_list=array_list()):
		try:
			# create empty variables
			self.__path = ''
			self.__file_list = array_list()
			self.__file_extension = array_list(file_extension)

			# valid path
			if os.path.isdir(path):
				self.__path = path

				# single valid file
				if str(file) != '':
					if os.path.exists(os.path.join(path, file)):
						self.__file_list.append(file.lower())
					# invalid file
					else:
						print('Invalid file: {0}'.format(path))
				
				# multiple valid files
				else:
					for file_list in os.listdir(path):
						# skip directories
						if len(file_list.split('.')) > 1:
							# check if file extension is in the list
							if self.__check_file_extension(file_list.split('.')[1]):
								self.__file_list.append(file_list.lower())
					self.__file_list.sort()

			# invalid path
			else:
				print('Invalid path: {0}'.format(path))
		except Exception as error:
			raise error

	# Returns True if extension is in extension list
	def __check_file_extension(self, file_ext:str) -> bool:
		try:
			# loop thru file extension list
			for file_extension in self.__file_extension:
		
				# returns True for match
				if file_extension.lower() == file_ext.lower():
					return True
		
			# else returns False
			return False
		except Exception as error:
			raise error

	# Returns self.__file
	def get_file_list(self) -> array_list:
		try:
			return self.__file_list
		except Exception as error:
			raise error

	# Returns self.__path
	def get_path(self) -> str:
		try:
			return self.__path
		except Exception as error:
			raise error


# Writes a _debug.txt file in directory passed to it
class debugger:
	# Init Constructor
	def __init__(self, path:str):
		try:
			self.__debug_file = open(os.path.join(str(path), '_debug.txt'), 'w', encoding='utf-8')
		except Exception as error:
			raise error

	#Public
	# Writes
	def write(self, data:str):
		try:
			self.__debug_file.write('{0}\n'.format(str(data)))
		except Exception as error:
			raise error


# Print the time between beginning and end of the timer
class timer:
	# Init Constructor
	def __init__(self):
		try:
			self.__start_time = time.time()
		except Exception as error:
			raise error

	#Private
	# Builds the hours, minutes & seconds piece of the string
	def __build_time_string(self, complete_time_string:str, time_int:int, time_string:str) -> str:
		try:
			# removes the s from the end of hours, minutes or seconds passed in the time_string
			if time_int == 1:
				return str(complete_time_string) + ('' if str(complete_time_string) == '' else ' ') + '{0} {1}'.format(str(time_int), str(time_string)[0:len(str(time_string))-1])
			else:
				return str(complete_time_string) + ('' if str(complete_time_string) == '' else ' ') + '{0} {1}'.format(str(time_int), str(time_string))
		except Exception as error:
			raise error
		
	#Public
	# Prints the time from start for this timer
	def print_time(self, print_string:str='', return_data:bool=False) -> str:
		try:
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
				time_string = self.__build_time_string(time_string, hours, 'hours')

			# minutes
			if total_time > 60:
				minutes = int(total_time / 60)
				total_time = total_time -  60 * minutes
				time_string = self.__build_time_string(time_string, minutes, 'minutes')

			# seconds
			seconds = total_time
			time_string = self.__build_time_string(time_string, seconds, 'seconds')

			# returns data string or prints to the screen (default)
			if return_data:
				return str(print_string) + time_string
			else:
				print(str(print_string) + time_string)
		except Exception as error:
			raise error