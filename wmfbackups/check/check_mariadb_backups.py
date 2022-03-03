#!/usr/bin/python3

"""check mariadb backups connects to the metadata database containing failed and successful
   database backups and checks that the given section, type and datacenter has a recent sucessful
   backup, as well as comparing its size and other heuristic checks to make sure it looks good"""

import argparse
import datetime
import sys

import arrow
import pymysql

OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

# TODO: Change this into a wmf api call- See conversation at:
#       https://gerrit.wikimedia.org/r/c/operations/software/wmfbackups/+/767844
#       (now tracked at T138562) why we cannot do this yet
SECTIONS = ['s1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's10'
            'x1', 'pc1', 'pc2', 'pc3',
            'es1', 'es2', 'es3', 'es4', 'es5',
            'm1', 'm2', 'm3', 'm4', 'm5', 'tendril', 'zarcillo',
            'mediabackupstemp', 'matomo', 'analytics_meta']
DATACENTERS = ['eqiad', 'codfw']  # only check dbs on the main dcs
TYPES = ['dump', 'snapshot']
DEFAULT_FRESHNESS = 691200  # 8 days, in seconds
DEFAULT_MIN_SIZE = 300 * 1024  # size smaller than 300K is considered failed
DEFAULT_WARN_SIZE_PERCENTAGE = 5  # size of previous ones minus or plus this percentage is weird
DEFAULT_CRIT_SIZE_PERCENTAGE = 15  # size of previous ones minus or plus this percentage is a fail
DEFAULT_SSL_CA = '/etc/ssl/certs/Puppet_Internal_CA.pem'  # CA path used for mysql TLS connection


class BadSectionException(Exception):
    """Internal exception raised when trying to check a non-valid section (not in list)"""
    pass


class BadDatacenterException(Exception):
    """Internal exception raised when trying to check a non primary datacenter"""
    pass


class BadTypeException(Exception):
    """Internal exception raised when trying to check an unknown backup type (not in list)"""
    pass


class DatabaseConnectionException(Exception):
    """Internal exception raised when connecting to the metadata database fails
       (e.g. server is down or unreachable, grant issue, etc.)"""
    pass


class DatabaseQueryException(Exception):
    """Internal exception raised when querying the metadata database fails (invalid query,
       unexpected data structure, etc.)"""
    pass


def get_options():
    """Parses the commandline options and returns them as an object,
       also return a list of available sections"""
    valid_sections = SECTIONS

    parser = argparse.ArgumentParser(description='Checks if backups for a '
                                                 'specific section are fresh.')
    parser.add_argument('--host', '-o', required=True,
                        help='Host with the database to connect to')
    parser.add_argument('--user', '-u', required=True,
                        help='user used for the mysql connection')
    parser.add_argument('--password', '-w', default='',
                        help='Password used for the mysql connection')
    parser.add_argument('--database', '-D', required=True,
                        help='Database where the backup metadata is stored')
    parser.add_argument('--section', '-s', required=True,
                        choices=valid_sections,
                        help='Database section/shard to check')
    parser.add_argument('--datacenter', '-d', required=True,
                        choices=DATACENTERS,
                        help='Datacenter storage location of the backup to check.')
    parser.add_argument('--type', '-t', required=False,
                        choices=TYPES, default=TYPES[0],
                        help='Type or method of backup, dump or snapshot')
    parser.add_argument('--freshness', '-f', default=DEFAULT_FRESHNESS,
                        type=int,
                        help='Time, in seconds, of how old a backup can be '
                             'before being considered outdated (default: 8 days)')
    parser.add_argument('--min-size', '-c', default=DEFAULT_MIN_SIZE,
                        type=int,
                        help='Size, in bytes, below which the backup is considered '
                             'failed in any case (default: 300 KB)')
    parser.add_argument('--warn-size-percentage', '-p', default=DEFAULT_WARN_SIZE_PERCENTAGE,
                        type=float,
                        help='Percentage of size change compared to previous backups, '
                             'above which a WARNING is produced (default: 5%%)')
    parser.add_argument('--crit-size-percentage', '-P', default=DEFAULT_CRIT_SIZE_PERCENTAGE,
                        type=float,
                        help='Percentage of size change compared to previous backups, '
                             'above which a CRITICAL is produced (default: 15%%)')

    parsed_options = parser.parse_args()
    setattr(parsed_options, 'valid_sections', valid_sections)
    return parsed_options


def query_metadata_database(options):
    '''Connect to and query the metadata database, return the data of the last 2 backups
       for the given options. Return true and the data if successful, false and an error
       message if failed.'''
    try:
        db = pymysql.connect(host=options.host, user=options.user, password=options.password,
                             database=options.database, ssl={'ca': DEFAULT_SSL_CA})
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


def validate_input(options):
    """Check and handle input parameters"""
    if options.section not in options.valid_sections:
        raise BadSectionException
    section = options.section
    if options.datacenter not in DATACENTERS:
        raise BadDatacenterException
    datacenter = options.datacenter
    if options.type not in TYPES:
        raise BadTypeException
    type = options.type
    freshness = int(options.freshness)
    required_backup_date = (datetime.datetime.now()
                            - datetime.timedelta(seconds=freshness))
    present = arrow.utcnow()
    humanized_freshness = present.humanize(present.shift(seconds=freshness))
    crit_size_percentage = options.crit_size_percentage
    warn_size_percentage = options.warn_size_percentage
    min_size = options.min_size
    humanized_min_size = format_size(options.min_size)

    return (section, datacenter, type, required_backup_date, humanized_freshness,
            crit_size_percentage, warn_size_percentage, min_size, humanized_min_size)


def format_size(size):
    """Given an integer size in bytes, return a string with a reasonable representation
       easily readable by a human. Avoid terabytes representation to easily compare large sizes."""
    if size >= (1024 * 1024 * 1024 * 2):
        formatted_size = f'{int(round(size / 1024 / 1024 / 1024))} GiB'
    elif size >= (1024 * 1024 * 1024 * 0.9):
        formatted_size = f'{round(size / 1024 / 1024 / 1024, 1):.1f} GiB'
    elif size >= (1024 * 1024 * 2):
        formatted_size = f'{int(round(size / 1024 / 1024))} MiB'
    elif size >= (1024 * 1024 * 0.9):
        formatted_size = f'{round(size / 1024 / 1024, 1):.1f} MiB'
    elif size >= (1024 * 2):
        formatted_size = f'{int(round(size / 1024))} KiB'
    elif size >= 900:
        formatted_size = f'{round(size / 1024, 1):.1f} KiB'
    else:
        formatted_size = f'{int(round(size))} B'
    return formatted_size


def process_current_backup_data(data):
    """returns the interesting data from the last backup"""
    last_backup_date = data[0]['start_date']
    size = data[0]['total_size']
    if size is None:
        size = 0
    else:
        size = int(size)
    humanized_size = format_size(size)
    source = data[0]['source']
    if isinstance(source, str):
        source = source.split('.')[0]

    return (last_backup_date, size, humanized_size, source)


def format_percentage(percentage):
    """Given a percentage float, return an equivalent string easy to read, including the sign"""
    if percentage >= 0:
        sign = '+'
    else:
        sign = '-'
    return f'{sign}{abs(round(percentage, 1)):.1f} %'  # in this house we observe ISO 31-0 standard


def process_previous_backup_data(size, data):
    """Retrieve the size of the backup before the last one and return its properties
       (mostly the size)"""
    previous_size = data[1]['total_size']
    humanized_previous_size = format_size(previous_size)
    percentage_change = (size - previous_size) / previous_size * 100.0
    humanized_percentage_change = format_percentage(percentage_change)
    return previous_size, humanized_previous_size, percentage_change, humanized_percentage_change


def check_backup_database(options):
    '''
    Connects to the database with the backup metadata and checks for anomalies.
    :param options: structure with a section, datacenter and freshness
    :return: (icinga status code (int), icinga status message)
    '''
    try:
        (section, datacenter, type, required_backup_date, humanized_freshness,
         crit_size_percentage, warn_size_percentage, min_size,
         humanized_min_size) = validate_input(options)
    except BadSectionException:
        return (UNKNOWN, f'Bad or unrecognized section: {options.section}')
    except BadDatacenterException:
        return (UNKNOWN, f'Bad or unrecognized datacenter: {options.datacenter}')
    except BadTypeException:
        return (UNKNOWN, f'Bad or unrecognized type: {options.type}')
    identifier = f'{type} for {section} at {datacenter}'

    try:
        data = query_metadata_database(options)
    except DatabaseConnectionException:
        return (False, f'We could not connect to the backup metadata database: '
                       f'{options.host}/{options.database}')
    except DatabaseQueryException:
        return (False, f'Error while querying the backup metadata database: '
                       f'{options.host}/{options.database}')

    # Did we get at least 1 sucessful backup?
    if len(data) < 1:
        return (CRITICAL, f'We could not find any completed {identifier}')

    (last_backup_date, size, humanized_size, source) = process_current_backup_data(data)

    # check backup is fresh enough
    if last_backup_date < required_backup_date:
        return (CRITICAL, f'{identifier} ({source}) taken more than {humanized_freshness}: '
                          f'Most recent backup {last_backup_date}')
    # Check minimum size
    if size < min_size:
        return(CRITICAL, f'{identifier} ({source}, {last_backup_date}): '
                         f'{humanized_size} is less than {humanized_min_size}')
    # warn in any case if there is only 1 backup (cannot compare sizes)
    if len(data) == 1:
        return(WARNING, f'There is only 1 {identifier} ({source}) '
                        f'taken on {last_backup_date} ({humanized_size})')

    (previous_size, humanized_previous_size, percentage_change,
     humanized_percentage_change) = process_previous_backup_data(size, data)

    # check size change
    if abs(percentage_change) > crit_size_percentage:
        return(CRITICAL, f'Last {identifier} ({source}) '
                         f'taken on {last_backup_date} is {humanized_size}, but '
                         f'the previous one was {humanized_previous_size}, '
                         f'a change of {humanized_percentage_change}')
    if abs(percentage_change) > warn_size_percentage:
        return(WARNING, f'Last {identifier} ({source}) '
                        f'taken on {last_backup_date} is {humanized_size}, but '
                        f'the previous one was {humanized_previous_size}, '
                        f'a change of {humanized_percentage_change}')

    # TODO: check files expected
    return (OK, f'Last {identifier} ({source}) '
                f'taken on {last_backup_date} ({humanized_size}, '
                f'{humanized_percentage_change})')


def main():
    """Parse options, query db and print results in icinga format"""
    options = get_options()
    result = check_backup_database(options)
    print(result[1])
    sys.exit(result[0])


if __name__ == "__main__":
    main()
