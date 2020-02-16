import ast
import gzip
import os
import re

SCHEMAS = {}


def is_create_statement(line):
    return line.upper().startswith('CREATE TABLE')


def is_field_definition(line):
    return line.strip().startswith('`')


def is_insert_statement(line):
    return line.upper().startswith('INSERT INTO')


def get_mysql_name_value(line):
    value = None
    result = re.search(r'\`([^\`]*)\`', line)
    if result:
        value = result.groups()[0]
    return value


def get_value_tuples(line):
    values = line.partition(' VALUES ')[-1].strip().replace('NULL', 'None")
    if values[-1] == ';':
        values = values[:-1]

    return ast.literal_eval(values)


def parse_file(file_name: str, table: str):
"""
Given a gzipped mysql dump file, yield a dict for each record.
My use case for this is when performing ETLs where this step needs to yield
a line separated JSON file, particularly useful when targeting Google BigQuery
as the target storage technology.

:param: str file_name Path to gz file containing the MySQL dump file
:param: str table The name of the table to parse, in case there are multiple tables in the same file
"""
    current_table_name = None

    with gzip.open(file_name, 'rb') as reader:
        for line in reader:
            line = line.decode()
            if is_create_statement(line):
                current_table_name = get_mysql_name_value(line)
                SCHEMAS[current_table_name] = []
            elif current_table_name and is_field_definition(line):
                field_name = get_mysql_name_value(line)
                SCHEMAS[current_table_name].append(field_name)
            elif is_insert_statement(line):
                current_table_name = get_mysql_name_value(line)
                if current_table_name != table:
                    continue
                current_schema = SCHEMAS[current_table_name]
                values = get_value_tuples(line)
                for row in values:
                    yield dict(zip(current_schema, row))
