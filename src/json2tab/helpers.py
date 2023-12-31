from collections import defaultdict
import gzip
from queue import Queue


def open_file(filepath: str, **kwargs):
    """
    Open .json or .json.gz file depending on given filename extension

    :param filepath: str specifying file path
    """
    if filepath.endswith(".gz"):
        return gzip.open(filepath, **kwargs)
    else:
        return open(filepath, **kwargs)


class Row:
    """
    """

    def __init__(self, size: int, col_lookup: dict):
        self.row = [None] * size
        self.col_lookup = col_lookup
        self.size = size

    def set_value(self, column, value):
        index = self.col_lookup[column]
        self.row[index] = value

    def get_value(self, column):
        index = self.col_lookup[column]
        return self.row[index]

    def get_row(self):
        return self.row

    def __len__(self):
        return self.size


class RowBuffer:
    """
    Helper class for accumulating rows from json flattening before writing to file
    A defaultdict mapping tables to a list of parsed rows
    """
    collector = defaultdict(Queue)
    size = 0  # total number of rows being kept in collector

    def append(self, table, row):
        """
        Append a row to its corresponding table
        :param table:
        :param row:
        :return:
        """
        self.collector[table].put_nowait(row)
        self.inc_size()

    def get_rows(self, table):
        """
        Get list of rows corresponding to table
        :param table:
        :return:
        """
        return self.collector[table].queue

    def get_tables(self):
        """
        Get list of the tables in collector
        :return:
        """
        return self.collector.keys()

    def get_size(self):
        """
        Get size attribute of the RowCollector object
        :return:
        """
        return self.size

    def inc_size(self):
        """
        Increment the size attribute of the RowCollector object
        """
        self.size = self.size + 1

    def reset(self):
        self.collector = defaultdict(Queue)
        self.size = 0


class FileHandler:
    """
    Represents a dict of all CSV files with methods to open and close all
    """

    def __init__(self):
        self.files = defaultdict(dict)
        self.__index = 0

    def open(self, file_key, filepath, **kwargs):
        """
        open file and add to files dict

        :param file_key: target key in files dict
        :param filepath: Path object specifying the file path
        """
        filename = filepath.name
        f = open_file(str(filepath), **kwargs)
        self.files[file_key]['file'] = f
        self.files[file_key]['name'] = filename
        return f

    def close(self):
        """close all open files"""
        for file_key in self.files:
            self.files[file_key]['file'].close()

    def flush(self):
        for file_key in self.files:
            self.files[file_key]['file'].flush()

    def size(self):
        return len(self.files)


class Stack:

    # define the constructor
    def __init__(self):
        # We will initiate the stack with an empty list
        self.items = []

    def is_empty(self):
        """
        Returns whether the stack is empty or not
        Runs in constant time O(1) as does not depend on size
        """
        # return True if empty
        # Return False if not
        return not self.items

    def push(self, item):
        """Adds item to the end of the Stack, returns Nothing
        Runs in constant time O(1) as no indices are change
        as we add to the right of the list
        """
        # use standard list method append
        self.items.append(item)

    def pop(self):
        """Removes the last item from the Stack and returns it
        Runs in constant time O(1) as only an item is removed,
        no indices are changed
        """
        # as long as there are items in the list
        # then return the list item
        if self.items:
            # use standard list method pop()
            # removes the end in pyhton >3.6
            return self.items.pop()
        # otherwise return None
        else:
            return None

    def peek(self):
        """Return the last value from the Stack
        if there is one
        Runs in constant time O(1) as only finding the
        end of the list using the index
        """
        # if there are items in the Stack
        if self.items:
            # then return the item
            return self.items[-1]
        # otherwise return None
        else:
            return None

    def set_last(self, item):
        """
        Modify the last item in the stack
        :param item: value to change last item of stack to
        """
        # if there are items in the Stack
        if self.items:
            # then set the item on top of the stack
            self.items[-1] = item

    def size(self):
        """Return the size of the Stack
        Runs in constant time O(1) as only checks the size"""
        # will return 0 if empty
        # so no need to check
        return len(self.items)

    def __str__(self):
        """Return a string representation of the Stack"""
        return str(self.items)
