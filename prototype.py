from pathlib import Path


import os
import csv
from collections import ChainMap

import time

from utils import parse

JSON_FILE = "data/test_full.json"  # modify this to target JSON file
OUT_DIR = Path("output/output2")  # modify this to desired output directory


# ======= CREATE MAPPINGS ====== #
# only needs to be done once for the entire program
def create_mappings():
    mappings = {}

    with open(JSON_FILE, "r") as f:
        # First pass: add all top-level keys using first json in file
        for (prefix, event, value) in parse(f):
            if prefix == '' and event == 'map_key' and value:
                mappings[value] = {}

            elif prefix == '' and event == 'end_map' and value is None:
                # first pass done, initiate second pass
                f.seek(0)  # read from beginning again
                break

        # Second pass: add all column names to mappings with default values
        # This pass goes through the entire json file to collect all possible columns
        for (prefix, event, value) in parse(f):
            if (event == "string" or event == "number"):
                # get name of relevant table from prefix eg. 'site'
                index_of_sep = prefix.find(".")
                if index_of_sep == -1:
                    base_prefix = prefix
                else:
                    base_prefix = prefix[:index_of_sep]

                # find table that matches the prefix and add value if value is an external node
                if base_prefix in list(mappings.keys()):
                    mappings[base_prefix][prefix] = None



    # for each table turn into ChainMap
    # The ChainMap makes it easy to restore default values to None after every row
    for table in mappings:
        mappings[table] = ChainMap({}, mappings[table])

    return mappings

    # ===== END CREATE MAPPINGS ===== #


# Represents a dict of all CSV files that can be closed simultaneously
class FileCollection:
    def __init__(self):
        self.files = {}
        self.__index = 0

    def open(self, file_key, *open_args):
        '''open file and add to files dict'''
        f = open(*open_args, newline='')
        self.files[file_key] = f
        return f

    def close(self):
        '''close all open files'''
        for file_key in self.files:
            self.files[file_key].close()





def json_flat(mappings, writers):
    count_rows = 0  # track number of rows written
    with open(JSON_FILE, "r", newline='') as jsonfile:
        for writer in writers:
            writer.writeheader()

        for (prefix, event, value) in parse(jsonfile):  # without multiple_values flag our json is invalid
            # get name of relevant table from prefix eg. 'site'
            index_of_sep = prefix.find(".")
            if index_of_sep == -1:
                base_prefix = prefix
            else:
                base_prefix = prefix[:index_of_sep]

            # if leaf reached and the field is not yet populated, set the value
            if (event == "string" or event == "number") and mappings[base_prefix][prefix] is None:

                if mappings[base_prefix][prefix] is None:  # value for key prefix empty
                    mappings[base_prefix][prefix] = value
            # else if leaf reached and field is already populated, append to array
            elif event == "string" or event == "number":  # value for key prefix already populated, add to/create an array

                # Note: growing arrays is an expensive operation, may look into linked lists or other solution
                if type(mappings[base_prefix][prefix]) == list:
                    mappings[base_prefix][prefix] = [*mappings[base_prefix][prefix],
                                                     value]  # unpack existing array into new one
                else:
                    mappings[base_prefix][prefix] = [mappings[base_prefix][prefix], value]

            # if reached end of a top-level json (ie. finished one property)
            elif prefix == '' and event == 'end_map' and value is None:
                count_rows = count_rows + 1

                # write rows to all corresponding csv files
                for writer, mapping in zip(writers, mappings):
                    writer.writerow(mappings[mapping])
                    mappings[mapping].maps[0].clear()
    return count_rows


def main():
    mappings = create_mappings()

    # create output directory if does not exist
    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)

    # open all CSV files, creates them if they don't exist
    out_files = FileCollection()
    for key in mappings.keys():
        out_files.open(key, OUT_DIR / f'{key}.csv', 'w')

    # create array of csv.DictWriter objects to prepare for writing rows
    files = out_files.files
    # note - The order of writers is the same as the order of top-level keys in mappings
    writers = [csv.DictWriter(files[table], fieldnames=list(mappings[table].keys())) for table in mappings]

    start = time.time()  # track overall run time of flattening algorithm

    count_rows = json_flat(mappings, writers)
    out_files.close()  # important - must close output files

    end = time.time()
    total_time = (end - start) * 1000
    print(f"The total number of rows is: {count_rows}")
    print(f"The average time per row is: {total_time / count_rows:.4f} ms")
    print(f"The total time is: {total_time:.4f} ms")


if __name__ == "__main__":
    main()
