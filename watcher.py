""" Data Pipeline System - SQL File Watcher """

# standard library imports
import os
import time

# third party library imports
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent, FileSystemEventHandler

# local library imports
from watertight import Watertight

# Constants
CORE_INFO_DB = 'CoreInfo'
CLOUD_CREDENTIAL = 'cloud_pg'
PGSQL_FILES = os.path.join(os.path.dirname(__file__), 'info', 'core_info', 'system')

class Watcher:
    """ Watcher setup """

    def __init__(self, path: str, event_handler: FileSystemEvent):
        self.event_handler = event_handler
        self.observer = Observer()
        self.path = path

    def run(self):
        """ Run an observer from Watchdog """

        #self.observer.schedule(self.event_handler, self.path, recursive=True)
        self.observer.schedule(self.event_handler, self.path)
        self.observer.start()
        print(f'Watching: {self.path}\n(Ctrl-C to stop)\n')

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()

        self.observer.join()
        print('Watcher has stopped.')

class MyEventHandler(FileSystemEventHandler):
    """ Handle File Changes """

    def __init__(self):
        self.pg = Watertight(CORE_INFO_DB, CLOUD_CREDENTIAL)
        self.log = []

    def on_modified(self, event: FileSystemEvent):
        """ Activates when a file is modified """

        path = os.path.split(event.src_path)[0]
        file_name = os.path.split(event.src_path)[1]
        event_tuple = (path, file_name, int(time.time()))

        # to fix the duplicating event
        if self.log.count(event_tuple) == 0:
            self.log.append(event_tuple)

            self.pg.cloud_sys_schema(path, file_name)

        for l in self.log:
            if l[2] != int(time.time()):
                self.log.remove(l)

if __name__ == "__main__":
    if os.path.exists(PGSQL_FILES):
        w = Watcher(PGSQL_FILES, MyEventHandler())
        w.run()
    else:
        print(f"Path is invalid: {PGSQL_FILES}")
