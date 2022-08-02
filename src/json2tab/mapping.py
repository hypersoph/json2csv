from json2tab.utils import parse, open_file
from ijson import IncompleteJSONError
import click

from collections import ChainMap
from tqdm import tqdm


class Mapping:
    total_count_json = 0  # total count of json lines in file

    @staticmethod
    def create_mappings(select_tables, config):
        """
        Creates the mappings variable determining the headers of each output file

        Assumes that every json has the same top-level keys

        :param config: configured parameters from user input
        :param select_tables: tables to output
        :return: mappings
        """
        mappings = {}

        with open_file(config.json_file, mode="r") as f:
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
            except IncompleteJSONError as e:
                click.echo(f"ijson.IncompleteJSONError {e}", err=True)
                pass

            # Add identifiers (e.g. factId and rollNumber) to each table
            for table in mappings:
                for identifier in config.identifiers:
                    mappings[table][identifier] = None

            # Second pass: add all column names to mappings with default values
            # This pass goes through the entire json file to collect all possible columns
            try:
                progress = tqdm(desc="Creating mappings", unit=" lines")
                for (base_prefix, prefix, event, value) in parse(f, multiple_values=True):
                    if event == "string" or event == "number":
                        # find table that matches the prefix and add value if value is an external node
                        if base_prefix in select_tables and base_prefix not in config.identifiers:
                            mappings[base_prefix][prefix] = None
                    elif prefix == '' and event == 'end_map' and value is None:
                        progress.update(1)
                        Mapping.total_count_json = Mapping.total_count_json + 1
            except IncompleteJSONError as e:
                click.echo(f"ijson.IncompleteJSONError {e}", err=True)
                pass

        # for each table create ChainMap
        # The ChainMap makes it easy to restore default values to None after every json line
        for table in mappings:
            mappings[table] = ChainMap({}, mappings[table])

        return mappings
