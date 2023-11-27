import pymysql

"""Classes used to report metrics of backups to icinga and prometheus"""

DEFAULT_VALID_SECTION_CONFIG_PATH = '/etc/wmfbackups/valid_sections.txt'
DEFAULT_CONFIG_FILE_PATH = '/etc/wmfbackups/backups_check.ini'


class BadConfigException(Exception):
    """Internal exception raised when an error happens while trying to read the
       valid sections configuration file: file is missing, bad file permissions,
       or config doesn't return at least 1 valid section."""
    pass


class DatabaseConnectionException(Exception):
    """Internal exception raised when connecting to the metadata database fails
       (e.g. server is down or unreachable, grant issue, etc.)"""
    pass


class DatabaseQueryException(Exception):
    """Internal exception raised when querying the metadata database fails (invalid query,
       unexpected data structure, etc.)"""
    pass


class WMFMetrics:
    """Class to retrieve metrics from the database backups"""

    def __init__(self, options):
        """Constructor"""
        self.config_file = options.config_file if hasattr(options, 'config_file') else DEFAULT_CONFIG_FILE_PATH
        self.valid_sections_config_path = options.valid_sections_file if hasattr(options, 'valid_sections_file') else DEFAULT_VALID_SECTION_CONFIG_PATH

    def get_valid_sections(self):
        """Reads the list of valid section names/backup job names from a given
        config file and loads it into memory for config validation."""
        valid_sections = list()
        # TODO: Change this into a wmf api call- See conversation at:
        #       https://gerrit.wikimedia.org/r/c/operations/software/wmfbackups/+/767844
        #       (now tracked at T138562) why we cannot do this yet
        try:
            with open(self.valid_sections_config_path, 'r', encoding='utf8') as config_file:
                for line in config_file:
                    if len(line.strip()) >= 1:
                        valid_sections.append(line.strip())
        except OSError as ex:
            raise BadConfigException from ex
        if len(valid_sections) < 1:
            raise BadConfigException
        return valid_sections

    def query_metadata_database(self, options):
        """Connect to and query the metadata database, return the data of the last 2 backups
        for the given options. Return true and the data if successful, false and an error
        message if failed."""
        try:
            db = pymysql.connect(read_default_file=self.config_file)
        except (pymysql.err.OperationalError, pymysql.err.InternalError) as ex:
            raise DatabaseConnectionException from ex
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            query = """SELECT id, name, status, source, host, type, section, start_date,
                              end_date, total_size
                         FROM backups
                        WHERE type = %s and
                              section = %s and
                              host like %s and
                              status = 'finished' and
                              end_date IS NOT NULL
                     ORDER BY start_date DESC
                        LIMIT 2"""
            try:
                cursor.execute(query, (options.type, options.section, f'%.{options.datacenter}.wmnet'))
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError) as ex:
                raise DatabaseQueryException from ex
            data = cursor.fetchall()
        return data
