import os
import csv

import time
from cmd import Cmd

from utils import *
from helpers import *
from config import Config
from mapping import *

from tqdm import tqdm


class Flatten:
    count_rows = 0  # track number of rows written

    @staticmethod
    def json_flat(mappings, writers, files, select_tables, config):
        """
        Flatten json and output to csv

        :param files:
        :param config: User specified configuration
        :param select_tables: selected tables to output
        :param mappings: mapping dict specifying structure of output files
        :param writers: list of output writers
        """

        row_buffer = RowBuffer()

        id_dict = {}  # keep track of specified identifier values e.g. factId and rollNumber
        for identifier in config.identifiers:
            id_dict[identifier] = None

        with open(config.json_file, "rb") as jsonfile:
            for writer, table in zip(writers, mappings):
                writer.writerow(list(mappings[table].keys()))

            try:
                pbar = tqdm(total=Mapping.total_count_json, desc='Flattening JSON')
                parser = parse(jsonfile, multiple_values=True)
                for (base_prefix, prefix, event, value) in parser:

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
                            # append copy so that row doesn't get reset with mappings
                            row = blist(id_dict.values()) + blist(mappings[table].maps[0].values())
                            row_buffer.append(table, row)

                            # reset map
                            mappings[table].maps[0].clear()

                        Flatten.count_rows = Flatten.count_rows + 1
                        pbar.update(1)

                        # write all collected rows if total num rows exceeds specified size
                        if row_buffer.get_size() >= config.chunk_size:
                            for writer, table in zip(writers, row_buffer.get_tables()):
                                writer.writerows(row_buffer.get_rows(table))

                            row_buffer.reset()
                            files.flush()

                        # reset variables
                        for id_key in id_dict:
                            id_dict[id_key] = None

            except ijson.IncompleteJSONError as e:
                click.echo(f"ijson.IncompleteJSONError {e}", err=True)
                pass
            finally:
                pbar.close()

                # write any remaining rows
                if row_buffer.get_size() > 0:
                    for writer, table in zip(writers, row_buffer.get_tables()):
                        writer.writerows(row_buffer.get_rows(table))

                    row_buffer.reset()

                files.close()


@click.command()
@click.option('--file', '-f', help='Input JSON file', required=True, type=click.Path(exists=True))
@click.option('--out', '-o', help='Output directory', required=True, type=click.Path(file_okay=False))
@click.option('--chunk-size', '-cs', type=int, default=50000,
              help='# rows to keep in memory before writing for each file')
@click.option('--debug', is_flag=True, help='Enables printing of additional info for debugging')
def main(file, out, chunk_size, debug):
    """Program that flattens JSON file and converts to CSV"""

    # user input validation
    if not file.endswith(".json"):
        raise click.exceptions.BadOptionUsage('--file', "Input file extension is not .json")

    # create output directory
    if not os.path.exists(out):
        os.mkdir(out)

    config = Config(file, out, chunk_size)

    cli = Cmd()

    click.echo("Running flatten.py")
    click.echo(f"Input file: {file}")
    click.echo(f"Output path: {out}")  # note to self: fix this to show full filesystem path

    if debug:
        click.echo("\nDEBUG INFO")
        click.echo(f"Using ijson backend: {ijson.backend}")

    click.echo(f"\nTop-level keys:\n=================")
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

    mappings = Mapping.create_mappings(tables, config)
    click.echo(f"The total number of json lines is: {Mapping.total_count_json}")

    # open all CSV files, creates them if they don't exist
    out_files = FileHandler()
    for key in mappings.keys():
        out_files.open(key, file=os.path.join(out, f'{key}.csv'), mode='w', encoding='utf-8', newline='')

    # create array of csv.DictWriter objects to prepare for writing rows
    files = out_files.files
    # note - The order of writers is the same as the order of top-level keys in mappings
    writers = [csv.writer(files[table]) for table in mappings]

    start = time.time()  # track overall run time of flattening algorithm
    Flatten.json_flat(mappings, writers, out_files, tables, config)
    end = time.time()

    print(f"{out_files.size()} files written to {out}\n")

    total_time = (end - start)
    print(f"Number of json lines written into each file is: {Flatten.count_rows}")
    print(f"The average time per json line is: {total_time * 1000 / Flatten.count_rows:.4f} ms")


if __name__ == "__main__":
    main()
