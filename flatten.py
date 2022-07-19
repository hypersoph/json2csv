import click

import os
import csv
from collections import ChainMap, defaultdict
import time
from cmd import Cmd

from utils import *
from helpers import *
from config import Config


def create_mappings(select_tables, config):
    """
    Creates the mappings variable determining the headers of each output file

    Assumes that every json has the same top-level keys

    :param config: configured parameters from user input
    :param select_tables: tables to output
    :return: mappings
    """
    mappings = {}

    with open(config.json_file, "r") as f:
        # First pass: add all top-level keys using first json in file
        try:
            for (_, prefix, event, value) in parse(f, multiple_values=True):
                if not select_tables and prefix == '' and event == 'map_key' and value not in config.identifiers:
                    mappings[value] = {}
                elif prefix == '' and event == 'map_key' and value not in config.identifiers and value in select_tables:
                    mappings[value] = {}
                elif prefix == '' and event == 'end_map' and value is None:
                    # first pass done
                    f.seek(0)  # read from beginning again
                    break
        except Exception as e:
            click.echo(e, err=True)
            pass

        # Add identifiers (e.g. factId and rollNumber) to each table
        for table in mappings:
            for identifier in config.identifiers:
                mappings[table][identifier] = None

        # Second pass: add all column names to mappings with default values
        # This pass goes through the entire json file to collect all possible columns
        try:
            for (base_prefix, prefix, event, value) in parse(f, multiple_values=True):
                if event == "string" or event == "number":
                    # find table that matches the prefix and add value if value is an external node
                    if base_prefix in list(mappings.keys()):
                        mappings[base_prefix][prefix] = None
        except Exception as e:
            click.echo(e, err=True)
            pass

    # for each table create ChainMap
    # The ChainMap makes it easy to restore default values to None after every json line
    for table in mappings:
        mappings[table] = ChainMap({}, mappings[table])

    return mappings


def json_flat(mappings, writers, select_tables, config):
    """
    Flatten json and output to csv

    :param config: User specified configuration
    :param select_tables: selected tables to output
    :param mappings: mapping dict specifying structure of output files
    :param writers: list of output writers
    :return: count of json lines
    """

    count_rows = 0  # track number of rows written
    row_collector = RowCollector()

    id_dict = {}  # keep track of specified identifier values e.g. factId and rollNumber
    for identifier in config.identifiers:
        id_dict[identifier] = None

    with open(config.json_file, "r", newline='') as jsonfile:
        for writer in writers:
            writer.writeheader()

        try:
            for (base_prefix, prefix, event, value) in parse(jsonfile, multiple_values=True):

                if event == "string" or event == "number":
                    for id_key in id_dict:
                        if id_dict[id_key] is None and base_prefix == id_key:
                            id_dict[id_key] = value

                    if base_prefix in select_tables and base_prefix not in config.identifiers:
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

                    for table in mappings:
                        # add identifiers to row
                        for id_key in id_dict:
                            mappings[table][id_key] = id_dict[id_key]

                        row = mappings[table].maps[0].copy()  # append copy so that row doesn't get reset with mappings
                        row_collector.append(table, row)

                        # reset map
                        mappings[table].maps[0].clear()

                    count_rows = count_rows + 1

                    # write all collected rows if total num rows exceeds specified size
                    if row_collector.get_size() >= config.chunk_size:
                        for writer, table in zip(writers, row_collector.get_tables()):
                            writer.writerows(row_collector.get_rows(table))

                        row_collector.reset()

                    # reset variables
                    for id_key in id_dict:
                        id_dict[id_key] = None
        except ijson.IncompleteJSONError as e:
            click.echo(e, err=True)
            pass
    # write any remaining rows
    if row_collector.get_size() > 0:
        for writer, table in zip(writers, row_collector.get_tables()):
            writer.writerows(row_collector.get_rows(table))

        row_collector.reset()

    return count_rows


@click.command()
@click.option('--file', '-f', help='Input JSON file', required=True, type=click.Path(exists=True))
@click.option('--out', '-o', help='Output directory', required=True, type=click.Path(file_okay=False))
@click.option('--chunk-size', '-cs', type=int, default=500,
              help='# rows to keep in memory before writing for each file')
def main(file, out, chunk_size):
    """Program that flattens JSON file and converts to CSV"""

    # user input validation
    if not file.endswith(".json"):
        raise click.exceptions.BadOptionUsage('--file', "Input file extension is not .json")

    # create output directory
    if not os.path.exists(out):
        os.mkdir(out)

    config = Config(file, out, chunk_size)

    cli = Cmd()

    print("Running flatten.py")
    print(f"Input file: {file}")
    print(f"Output path: {out}")  # note to self: fix this to show full filesystem path

    print(f"\nTop-level keys:\n=================")
    top_keys = get_top_keys(file)
    cli.columnize(top_keys, displaywidth=80)

    input_valid = 0
    tables = ''
    while input_valid == 0:
        tables = click.prompt(
            f'\nEnter desired keys from the preceding list, separated by spaces (leave empty for all):\n',
            default='', show_default=False)

        tables = tables.split(" ") if tables else top_keys
        # input valid if every table in tables is in top_keys
        if set(tables).issubset(set(top_keys)):
            input_valid = 1
        else:
            click.echo(f"Error: {tables} is not in {top_keys}", err=True)

    input_valid = 0
    identifiers = config.identifiers
    while input_valid == 0:
        identifiers = click.prompt(f'Specify identifier keys separated by spaces (leave empty for defaults):',
                                   default=config.identifiers)  # add error if keys not in tables
        identifiers = identifiers.split(" ")

        if set(identifiers).issubset(set(top_keys)):
            input_valid = 1
        else:
            click.echo(f"Error: {identifiers} is not in {top_keys}", err=True)

    config.identifiers = identifiers

    # remove any identifiers from tables var
    for idt in config.identifiers:
        if idt in tables:
            tables.remove(idt)

    click.clear()

    print("Creating mappings...")
    start = time.time()
    mappings = create_mappings(tables, config)
    end = time.time()

    total_time = (end - start)
    print(f"The total time to execute create_mappings is {total_time:.4f} s\n")

    # open all CSV files, creates them if they don't exist
    out_files = FileCollection()
    for key in mappings.keys():
        out_files.open(key, os.path.join(out, f'{key}.csv'), 'w')

    # create array of csv.DictWriter objects to prepare for writing rows
    files = out_files.files
    # note - The order of writers is the same as the order of top-level keys in mappings
    writers = [csv.DictWriter(files[table], fieldnames=list(mappings[table].keys())) for table in mappings]

    print("Flattening JSON...")
    start = time.time()  # track overall run time of flattening algorithm
    count_rows = json_flat(mappings, writers, tables, config)
    end = time.time()

    print(f"{out_files.size()} files written to {out}\n")
    out_files.close()  # important - must close output files

    total_time = (end - start)
    print(f"Number of json lines written in each file is: {count_rows}")
    print(f"The average time per json line is: {total_time * 1000 / count_rows:.4f} ms")
    print(f"The total time to execute json_flat is: {total_time:.4f} s")


if __name__ == "__main__":
    main()
