#!/usr/bin/python3

# Dependencies: python3 python3-pymysql python3-yaml
#               mydumper at /usr/bin/mydumper (if dumps are used)
#               wmf-mariadb101 (for /opt/wmf-mariadb101/bin/mariabackup, if snapshoting is used)
#               pigz on /usr/bin/pigz (if snapshoting is used)
#               tar at /bin/tar
#               TLS certificate installed at /etc/ssl/certs/Puppet_Internal_CA.pem (if data
#               gathering metrics are used)
from WMFBackup import WMFBackup

import argparse
import logging
from multiprocessing.pool import ThreadPool
import sys
import yaml

DEFAULT_CONFIG_FILE = '/etc/mysql/backups.cnf'
DEFAULT_THREADS = 18
DEFAULT_TYPE = 'dump'
CONCURRENT_BACKUPS = 2
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306
DEFAULT_ROWS = 20000000
DEFAULT_USER = 'root'

DEFAULT_BACKUP_DIR = '/srv/backups'

DUMPNAME_FORMAT = 'dump.{0}.{1}'  # where 0 is the section and 1 the date
SNAPNAME_FORMAT = 'snapshot.{0}.{1}'  # where 0 is the section and 1 the date


def parse_options():
    parser = argparse.ArgumentParser(description=('Create a mysql/mariadb logical backup using '
                                                  'mydumper or a snapshot using mariabackup.'
                                                  'It has 2 modes: Interactive, where '
                                                  'options are received from the command line '
                                                  'and non-interactive, where it reads from a '
                                                  'config file and performs several backups'))
    parser.add_argument('section',
                        help=('Section name of the backup. E.g.: "s3", "tendril". '
                              'If section is set, --config-file is ignored. '
                              'If it is empty, only config-file options will be used '
                              'and other command line options will be ignored.'),
                        nargs='?',
                        default=None)
    parser.add_argument('--config-file',
                        help='Config file to use. By default, {} .'.format(DEFAULT_CONFIG_FILE),
                        default=DEFAULT_CONFIG_FILE)
    parser.add_argument('--host',
                        help='Host to generate the backup from. Default: {}.'.format(DEFAULT_HOST),
                        default=DEFAULT_HOST)
    parser.add_argument('--port',
                        type=int,
                        help='Port to connect to. Default: {}.'.format(DEFAULT_PORT),
                        default=DEFAULT_PORT)
    parser.add_argument('--user',
                        help='User to connect for backup. Default: {}.'.format(DEFAULT_USER),
                        default=DEFAULT_USER)
    parser.add_argument('--password',
                        help='Password used to connect. Default: empty password.',
                        default='')
    parser.add_argument('--threads',
                        type=int,
                        help=('Number of threads to use for exporting. '
                              'Default: {} concurrent threads.').format(DEFAULT_THREADS),
                        default=DEFAULT_THREADS)
    parser.add_argument('--type',
                        choices=['dump', 'snapshot'],
                        help='Backup type: dump or snapshot. Default: {}'.format(DEFAULT_TYPE),
                        default=DEFAULT_TYPE)
    parser.add_argument('--only-postprocess',
                        action='store_true',
                        help=('If present, only postprocess and perform the metadata '
                              'gathering metrics for the given ongoing section backup, '
                              'skipping the actual backup. Default: Do the whole process.'))
    parser.add_argument('--rotate',
                        action='store_true',
                        help=('If present, run the rotation process, by moving it to the standard.'
                              '"latest" backup. Default: Do not rotate.'))
    parser.add_argument('--backup-dir',
                        help=('Directory where the backup will be stored. '
                              'Default: {}.').format(DEFAULT_BACKUP_DIR),
                        default=DEFAULT_BACKUP_DIR)
    parser.add_argument('--rows',
                        type=int,
                        help=('Max number of rows to dump per file. '
                              'Default: {}').format(DEFAULT_ROWS),
                        default=DEFAULT_ROWS)
    parser.add_argument('--archive',
                        action='store_true',
                        help=('If present, archive each db on its own tar file.'))
    parser.add_argument('--compress',
                        action='store_true',
                        help=('If present, compress everything into a tar.gz.'
                              'Default: Do not compress.'))
    parser.add_argument('--regex',
                        help=('Only backup tables matching this regular expression,'
                              'with format: database.table. Default: all tables'),
                        default=None)
    parser.add_argument('--stats-host',
                        help='Host where the statistics database is.',
                        default=None)
    parser.add_argument('--stats-port',
                        type=int,
                        help='Port where the statistics database is. Default: {}'
                        .format(DEFAULT_PORT),
                        default=DEFAULT_PORT)
    parser.add_argument('--stats-user',
                        help='User for the statistics database.',
                        default=None)
    parser.add_argument('--stats-password',
                        help='Password used for the statistics database.',
                        default=None)
    parser.add_argument('--stats-database',
                        help='MySQL schema that contains the statistics database.',
                        default=None)
    options = parser.parse_args().__dict__
    # nest --stats-X option into a hash 'statistics' if --stats-host is set and not null
    if 'stats_host' in options and options['stats_host'] is not None:
        statistics = dict()
        statistics['host'] = options['stats_host']
        del options['stats_host']
        statistics['port'] = options['stats_port']
        del options['stats_port']
        statistics['user'] = options['stats_user']
        del options['stats_user']
        statistics['password'] = options['stats_password']
        del options['stats_password']
        statistics['database'] = options['stats_database']
        del options['stats_database']
        options['statistics'] = statistics
    return options


def parse_config_file(config_path):
    """
    Reads the given config_path absolute path and returns a dictionary
    of dictionaries with section names as keys, config names as subkeys
    and values of that config as final values.
    Threads concurrency is limited based on the number of simultaneous backups.
    The file must be in yaml format, and it allows for default configurations:

    user: 'test'
    password: 'test'
    sections:
      s1:
        host: 's1-master.eqiad.wmnet'
      s2:
        host: 's2-master.eqiad.wmnet'
        archive: True
    """
    logger = logging.getLogger('backup')
    try:
        config_file = yaml.load(open(config_path))
    except yaml.YAMLError:
        logger.error('Error opening or parsing the YAML file {}'.format(config_path))
        return
    except FileNotFoundError:
        logger.error('File {} not found'.format(config_path))
        sys.exit(2)
    if not isinstance(config_file, dict) or 'sections' not in config_file:
        logger.error('Error reading sections from file {}'.format(config_path))
        sys.exit(2)

    default_options = config_file.copy()
    # If individual thread configuration is set for each backup, it could have strange effects
    if 'threads' not in default_options:
        default_options['threads'] = DEFAULT_THREADS

    del default_options['sections']

    manual_config = config_file['sections']
    if len(manual_config) > 1:
        # Limit the threads only if there is more than 1 backup
        default_options['threads'] = int(default_options['threads'] / CONCURRENT_BACKUPS)
    config = dict()
    for section, section_config in manual_config.items():
        # fill up sections with default configurations
        config[section] = section_config.copy()
        for default_key, default_value in default_options.items():
            if default_key not in config[section]:
                config[section][default_key] = default_value
    return config


def main():

    logging.basicConfig(filename='debug.log', level=logging.DEBUG)
    logger = logging.getLogger('backup')

    options = parse_options()
    if options['section'] is None:
        # no section name, read the config file, validate it and
        # execute it, including rotation of old dumps
        config = parse_config_file(options['config_file'])
        backup = dict()
        result = dict()
        backup_types = dict()
        backup_pool = ThreadPool(CONCURRENT_BACKUPS)
        for section, section_config in config.items():
            backup[section] = WMFBackup(section, section_config)
            result[section] = backup_pool.apply_async(backup[section].run)
            # register one backup of each successful type for purging purpuses
            if 0 == result[section] and section_config['type'] not in backup_types.keys:
                backup_types[section_config['type']] = backup[section]

        backup_pool.close()
        backup_pool.join()

        for performed_backup in backup_types:
            performed_backup.purge_backups()

        sys.exit(result[max(result, key=lambda key: result[key].get())].get())

    else:
        # a section name was given, only dump that one
        backup = WMFBackup(options['section'], options)
        result = backup.run()
        if 0 == result:
            if options['rotate']:
                backup.purge_backups()
            logger.info('Backup {} generated correctly.'.format(options['section']))
        else:
            logger.critical('Error while performing backup of {}'.format(options['section']))
        sys.exit(result)


if __name__ == "__main__":
    main()
