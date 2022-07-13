from pathlib import Path

import os
import csv
from collections import ChainMap, defaultdict

import time

from utils import parse
from helpers import FileCollection

JSON_FILE = "data/test_full.json"  # modify this to target JSON file
OUT_DIR = Path("output")  # modify this to desired output directory
IDENTIFIERS = ["factId", "rollNumber"]
CHUNK_SIZE = 10000

def create_mappings():
    """
    - only needs to be done once for the entire program
    - assumes that every json has the same top-level keys
    :return: mappings
    """
    mappings = {}

    with open(JSON_FILE, "r") as f:
        # First pass: add all top-level keys using first json in file
        for (_, prefix, event, value) in parse(f):
            if prefix == '' and event == 'map_key' and value not in IDENTIFIERS:
                mappings[value] = {}

            elif prefix == '' and event == 'end_map' and value is None:
                # first pass done, initiate second pass
                f.seek(0)  # read from beginning again
                break

        # Add factId and rollNumber to each table
        for table in mappings:
            mappings[table]['factId'] = None
            mappings[table]['rollNumber'] = None

        # Second pass: add all column names to mappings with default values
        # This pass goes through the entire json file to collect all possible columns
        for (base_prefix, prefix, event, value) in parse(f):
            if event == "string" or event == "number":
                # find table that matches the prefix and add value if value is an external node
                if base_prefix in list(mappings.keys()):
                    mappings[base_prefix][prefix] = None

    # for each table turn into ChainMap
    # The ChainMap makes it easy to restore default values to None after every row
    for table in mappings:
        mappings[table] = ChainMap({}, mappings[table])

    return mappings


def json_flat(mappings, writers):
    """

    :param mappings:
    :param writers:
    :return:
    """
    count_rows = 0  # track number of rows written
    row_collector = defaultdict(list)

    with open(JSON_FILE, "r", newline='') as jsonfile:
        for writer in writers:
            writer.writeheader()

        roll_number = None
        fact_id = None

        for (base_prefix, prefix, event, value) in parse(jsonfile):  # without multiple_values flag our json is invalid

            if roll_number is None and base_prefix == "rollNumber":
                roll_number = value
            if fact_id is None and base_prefix == "factId":
                fact_id = value

            if base_prefix not in IDENTIFIERS:
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

                    # add roll number and fact id to row
                    for table in mappings:
                        mappings[table]['rollNumber'] = roll_number
                        mappings[table]['factId'] = fact_id
                        row = mappings[table].maps[0].copy() # append copy so that row doesn't get reset
                        row_collector[table].append(row)

                        # reset map
                        mappings[table].maps[0].clear()

                    if len(row_collector) >= CHUNK_SIZE:
                        for writer, rows in zip(writers, row_collector):
                            writer.writerows(row_collector[rows])

                        row_collector = defaultdict(list)

                    # reset variables

                    roll_number = None
                    fact_id = None

    if row_collector:
        for writer, rows in zip(writers, row_collector):
            writer.writerows(row_collector[rows])

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
    print(f"The total time for json_flat is: {total_time:.4f} ms")


if __name__ == "__main__":
    main()
