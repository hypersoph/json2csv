import os
import csv
from pathlib import Path
from cmd import Cmd
import concurrent.futures

import json2tab.utils as utils
from json2tab.utils import parse, get_top_keys
from json2tab.helpers import FileHandler, RowBuffer, open_file
from json2tab.config import Config
from json2tab.mapping import Mapping

from tqdm import tqdm
import ijson
import click


def json_bytes_from_file(f):
    while True:
        chunk = f.read(65536)
        if not chunk:
            break
        yield chunk


def flatten(files, select_tables, mappings, writers, config):
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

    pbar = tqdm(total=Mapping.total_count_json, desc='Flattening JSON', unit=" lines")

    @ijson.coroutine
    def process(exe):
        while True:
            (base_prefix, prefix, event, value) = (yield)

            if event == "string" or event == "number" or event == "boolean":
                for id_key in id_dict:
                    if id_dict[id_key] is None and base_prefix == id_key:
                        id_dict[id_key] = value

                if base_prefix in select_tables and base_prefix not in config.identifiers:
                    # if leaf reached and the field is not yet populated, set the value
                    if mappings[base_prefix][prefix] is None:
                        mappings[base_prefix][prefix] = value

                    # # else if leaf reached and field is already populated, create or append to array
                    # elif type(mappings[base_prefix][prefix]) == list:
                    #     mappings[base_prefix][prefix] = [*mappings[base_prefix][prefix],
                    #                                      value]  # unpack existing array into new one
                    # else:
                    #     #click.echo(f"{base_prefix} {prefix}, {value}", err=True)
                    #     mappings[base_prefix][prefix] = [mappings[base_prefix][prefix], value]

            # if reached end of a top-level json (i.e. finished one property)
            elif prefix == '' and event == 'end_map' and value is None:

                for table in mappings:
                    # add identifiers to row
                    for id_key in id_dict:
                        mappings[table][id_key] = id_dict[id_key]

                    row = list(mappings[table].values())  # append copy so that row doesn't get reset with mappings
                    row_buffer.append(table, row)

                    # reset map
                    for field in mappings[table]:
                        mappings[table][field] = None

                pbar.update(1)

                # write all collected rows if total num rows exceeds specified size
                if row_buffer.get_size() >= config.chunk_size:
                    #click.echo(f"writing {row_buffer.get_size()} rows...")
                    for csvwriter, table in zip(writers, row_buffer.get_tables()):
                        exe.submit(csvwriter.writerows, row_buffer.get_rows(table))

                    row_buffer.reset()

                # reset variables
                for id_key in id_dict:
                    id_dict[id_key] = None

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(select_tables)) as executor:

        with open_file(config.json_file, mode="rb") as json_file:  # read bytes
            for writer, table in zip(writers, mappings):
                writer.writerow(list(mappings[table].keys()))

            process_coro = process(executor)
            # send the events from custom parser to process coroutine
            parse_coro = utils.parse_coro(process_coro)  # note this line has to go before ijson.basic_parse... line
            # send the events and values from basic_parse to custom parser
            coro = ijson.basic_parse_coro(parse_coro, multiple_values=True, use_float=True)

            for chunk in json_bytes_from_file(json_file):
                coro.send(chunk)  # push bytes to parser

        try:
            coro.close()
        except ijson.IncompleteJSONError as e:
            click.echo(f"\nijson.IncompleteJSONError {e}", err=True)
            pass
        finally:
            pbar.close()
            click.echo()
            # write any remaining rows
            if row_buffer.get_size() > 0:
                for w, t in zip(writers, row_buffer.get_tables()):
                    executor.submit(w.writerows, row_buffer.get_rows(t))
                    file = files.files[t]
                    click.echo(f"Wrote {file['name']} with {len(mappings[t])} fields")

                row_buffer.reset()

            files.close()


def prompt_tables(top_keys):
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
    return tables


def prompt_ids(top_keys):
    input_valid = 0
    identifiers = ''
    while input_valid == 0:
        identifiers = click.prompt(f'Specify identifier keys separated by spaces (leave empty for none):',
                                   default='')  # add error if keys not in tables
        if not identifiers:
            click.echo("No identifiers specified.")
            break

        identifiers = identifiers.split(" ")

        if set(identifiers).issubset(set(top_keys)):
            input_valid = 1
        else:
            click.echo(f"Error: {identifiers} is not in {top_keys}", err=True)
    return identifiers


@click.command()
@click.option('--filepath', '-f', help='Input JSON file path', required=True, type=click.Path(exists=True))
@click.option('--out', '-o', help='Output directory', required=True, type=click.Path(file_okay=False))
@click.option('--chunk-size', '-cs', type=int, default=100,
              help='# rows to keep in memory before writing for each file')
@click.option('--identifier', '-id',
              help=
              """
              Top-level key to add as identifier col to every output file. 
              You can add this flag multiple times eg. -id factId -id otherId
              """,
              default=(), multiple=True)
@click.option('--table', '-t',
              help=
              """
              Top-level key whose json values are to be converted to tabular format. 
              You can add this flag multiple times eg. -t topkey1 -t topkey2
              """,
              multiple=True)
@click.option('--compress', '-c', help="Output a compressed csv eg. output_file.csv.gz", is_flag=True)
def main(filepath, out, chunk_size, identifier, table, compress):
    """Program that flattens JSON file and converts to CSV"""

    def validate_inputs():
        """
        Validate the program options specified
        """
        # Specified file has extension .json
        if not filepath.endswith(".json") and not filepath.endswith(".json.gz"):
            raise click.exceptions.BadOptionUsage(option_name='--filepath',
                                                  message=f"Invalid value for '--filepath' / '-f': Input file {filepath} extension is not .json or .json.gz")

        t_keys = get_top_keys(filepath)

        # check identifiers are top level keys
        if identifier:
            if not (set(identifier).issubset(set(t_keys))):
                raise click.exceptions.BadOptionUsage(option_name='--identifier',
                                                      message=f"Invalid value for '--identifier' / '-id': At least one of {identifier} is not a top-level key")

        # create output directory
        if not os.path.exists(out):
            try:
                os.mkdir(out)
            except FileNotFoundError:
                raise click.exceptions.BadOptionUsage(option_name='--out',
                                                      message=f"Invalid value for '--out / -o': Path '{out}' cannot be created")
        if table:
            if not (set(table).issubset(set(t_keys))):
                raise click.exceptions.BadOptionUsage(option_name='--table',
                                                      message=f"Invalid value for '--table' / '-t': At least one of {table} is not a top-level key")

    def remove_empty_tables():
        """
        remove empty tables from `mappings` and `tables`
        """
        empty_tables = []
        for t in mappings:
            if len(mappings[t]) == len(config.identifiers):
                empty_tables.append(t)
        if empty_tables:
            click.echo(f"\nNote: No output file will be created for the following keys because they have no values:\n")
        for t in empty_tables:
            click.echo(f"\t{t}")
            tables.remove(t)
            mappings.pop(t)
        click.echo()

    validate_inputs()

    click.echo("Starting program")
    config = Config(filepath, out, chunk_size)
    cli = Cmd()

    click.echo(f"Input file: {filepath}")
    click.echo(f"Output path: {out}")  # note to self: fix this to show full filesystem path

    print(f"\nTop-level keys:\n=================")
    top_keys = get_top_keys(filepath)
    cli.columnize(top_keys, displaywidth=80)

    if not table:
        table = prompt_tables(top_keys)
    tables = table

    if not identifier:
        identifier = prompt_ids(top_keys)
    config.identifiers = identifier

    # remove any identifiers from tables var
    for idt in config.identifiers:
        if idt in tables:
            tables.remove(idt)

    mappings = Mapping.create_mappings(tables, config)

    remove_empty_tables()

    if compress:
        extension = '.csv.gz'
    else:
        extension = '.csv'

    # open all CSV files, creates them if they don't exist
    out_files = FileHandler()
    filename = Path(filepath).stem.strip(".json")
    for key in mappings.keys():
        out_files.open(key, Path(out)/f'{filename}_{key}{extension}', mode='wt', encoding='utf-8', newline='')

    # Create list of writers
    files = out_files.files
    # note - The order of writers is the same as the order of top-level keys in mappings
    writers = [csv.writer(files[table]['file']) for table in mappings]

    flatten(out_files, tables, mappings, writers, config)

    click.echo(f"\n{out_files.size()} files written to {out}\n")

    #click.echo(f"Number of json lines written into each file is: {Flatten.count_rows}")

