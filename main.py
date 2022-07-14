from pathlib import Path
import os
import csv
from collections import ChainMap, defaultdict
import time

from utils import parse
from helpers import FileCollection

JSON_FILE = "data/test_full.json"  # modify this to target JSON file
OUT_DIR = Path("output/out2")  # modify this to desired output directory
IDENTIFIERS = ["factId", "rollNumber"]
CHUNK_SIZE = 10000


def create_mappings(select_tables):
    """
    - only needs to be done once for the duration of the program
    - assumes that every json has the same top-level keys

    :param select_tables: tables to output
    :return: mappings
    """
    mappings = {}

    with open(JSON_FILE, "r") as f:
        # First pass: add all top-level keys using first json in file
        for (_, prefix, event, value) in parse(f, multiple_values=True):
            if not select_tables and prefix == '' and event == 'map_key' and value not in IDENTIFIERS:
                mappings[value] = {}
            elif prefix == '' and event == 'map_key' and value not in IDENTIFIERS and value in select_tables:
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
        for (base_prefix, prefix, event, value) in parse(f, multiple_values=True):
            if event == "string" or event == "number":
                # find table that matches the prefix and add value if value is an external node
                if base_prefix in list(mappings.keys()):
                    mappings[base_prefix][prefix] = None

    # for each table turn into ChainMap
    # The ChainMap makes it easy to restore default values to None after every row
    for table in mappings:
        mappings[table] = ChainMap({}, mappings[table])

    return mappings


def json_flat(mappings, writers, select_tables):
    """
    Flatten json and output to csv

    :param select_tables: selected tables to output
    :param mappings: mapping dict specifying structure of output files
    :param writers: list of output writers
    :return: count of json lines
    """
    count_rows = 0  # track number of rows written
    row_collector = defaultdict(list)

    with open(JSON_FILE, "r", newline='') as jsonfile:
        for writer in writers:
            writer.writeheader()

        roll_number = None
        fact_id = None

        for (base_prefix, prefix, event, value) in parse(jsonfile, multiple_values=True):

            if event == "string" or event == "number":
                if roll_number is None and base_prefix == "rollNumber":
                    roll_number = value
                if fact_id is None and base_prefix == "factId":
                    fact_id = value

                if base_prefix in select_tables and base_prefix not in IDENTIFIERS:
                    # if leaf reached and the field is not yet populated, set the value
                    if mappings[base_prefix][prefix] is None:
                        mappings[base_prefix][prefix] = value

                    # else if leaf reached and field is already populated, create or append to array
                    elif type(mappings[base_prefix][prefix]) == list:
                        mappings[base_prefix][prefix] = [*mappings[base_prefix][prefix],
                                                         value]  # unpack existing array into new one
                    else:
                        mappings[base_prefix][prefix] = [mappings[base_prefix][prefix], value]

            # if reached end of a top-level json (i.e. finished one property)
            elif prefix == '' and event == 'end_map' and value is None:
                count_rows = count_rows + 1

                # add roll number and fact id to row
                for table in mappings:
                    mappings[table]['rollNumber'] = roll_number
                    mappings[table]['factId'] = fact_id
                    row = mappings[table].maps[0].copy()  # append copy so that row doesn't get reset
                    row_collector[table].append(row)

                    # reset map
                    mappings[table].maps[0].clear()

                # write all collected rows if num rows exceeds specified size
                if len(row_collector) >= CHUNK_SIZE:
                    for writer, rows in zip(writers, row_collector):
                        writer.writerows(row_collector[rows])

                    row_collector = defaultdict(list)

                # reset variables
                roll_number = None
                fact_id = None

    # write any remaining rows
    if row_collector:
        for writer, rows in zip(writers, row_collector):
            writer.writerows(row_collector[rows])

    return count_rows


def main():
    select_tables = []

    start = time.time()
    mappings = create_mappings(select_tables)
    end = time.time()

    total_time = (end - start)
    print(f"The total time to execute create_mappings is {total_time:.4f} s\n")

    select_tables = list(mappings.keys()) if not select_tables else select_tables

    # create output directory
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
    count_rows = json_flat(mappings, writers, select_tables)
    out_files.close()  # important - must close output files
    end = time.time()

    total_time = (end - start)
    print(f"The total number of rows is: {count_rows}")
    print(f"The average time per row is: {total_time * 1000 / count_rows:.4f} ms")
    print(f"The total time to execute json_flat is: {total_time:.4f} s")


if __name__ == "__main__":
    main()
