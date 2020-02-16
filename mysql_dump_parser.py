class MysqlDumpParser:
    def __init__(self, table: str):
        """
        :param: str table The name of the table to parse, in case there are multiple tables in the same file
        """
        self.table = table
        self.columns = []

    @staticmethod
    def is_create_statement(line):
        return line.upper().startswith('CREATE TABLE')


    @staticmethod
    def is_field_definition(line):
        return line.strip().startswith('`')

    @staticmethod
    def is_insert_statement(line):
        return line.upper().startswith('INSERT INTO')

    @staticmethod
    def get_mysql_name_value(line):
        value = None
        result = re.search(r'\`([^\`]*)\`', line)
        if result:
            value = result.groups()[0]
        return value

    @staticmethod
    def get_value_tuples(line):
        values = line.partition(' VALUES ')[-1].strip().replace('NULL', 'None')
        if values[-1] == ';':
            values = values[:-1]

        return ast.literal_eval(values)
    
    def to_dict(self, filename: str):
        """
        Given a gzipped mysql dump file, yield a dict for each record.
        My use case for this is when performing ETLs where this step needs to yield
        a line separated JSON file, particularly useful when targeting Google BigQuery
        as the target storage technology.

        :param: str filename Path to gz file containing the MySQL dump file
        """
        current_table = None

        with gzip.open(filename, 'rb') as reader:
            for line in reader:
                line = line.decode()
                if MysqlDumpParser.is_create_statement(line):
                    current_table = MysqlDumpParser.get_mysql_name_value(line)
                    if current_table != self.table:
                        continue
                elif current_table == self.table and MysqlDumpParser.is_field_definition(line):
                    col = MysqlDumpParser.get_mysql_name_value(line)
                    self.columns.append(col)
                elif MysqlDumpParser.is_insert_statement(line):
                    current_table = MysqlDumpParser.get_mysql_name_value(line)
                    if current_table != self.table:
                        continue
                    values = MysqlDumpParser.get_value_tuples(line)
                    schema = self.columns
                    for row in values:
                        yield dict(zip(schema, row))
