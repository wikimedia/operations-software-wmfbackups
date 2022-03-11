#!/usr/bin/python3

"""
Remote backup script, used to orchestrate and execute remote backups
using cumin and transfer.py
"""

import argparse
import datetime
from multiprocessing.pool import Pool
import logging
import os
import subprocess
import sys
import yaml

from wmfmariadbpy.RemoteExecution.CuminExecution import (
    CuminExecution as RemoteExecution,
)
import wmfmariadbpy.dbutil as dbutil

DEFAULT_CONFIG_FILE = '/etc/wmfbackups/remote_backups.cnf'
DEFAULT_THREADS = 16
DEFAULT_BACKUP_TYPE = 'dump'
DEFAULT_PORT = 3306
DEFAULT_TRANSFER_DIR = '/srv/backups/snapshots/ongoing'
DATE_FORMAT = '%Y-%m-%d--%H-%M-%S'
DUMP_USER = 'dump'
DUMP_GROUP = 'dump'


def get_cmd_arguments():
    """
    Require a sections options for 2 reasons:
      * avoid the accidental running of backups just by running the executable with no arguments
      * allow a partial manual run of only certain backups
    Only 1 option is read from command line, "sections", which can be "all", for the regular
    run of all sections on the configured backups on file, or a list of concrete sections (e.g.
    "remote-backup-mariadb s1 s2 x1")
    """
    parser = argparse.ArgumentParser(description=(f'Execute backups on remote hosts as '
                                                  f'configured on {DEFAULT_CONFIG_FILE}, '
                                                  f'mainly thought for xtrabackup automation.'))
    parser.add_argument('sections',
                        help=('list of sections names (space separated) -from the config '
                              'file- to backup. If all sections have to be backed up, '
                              'use the alias "all" (without quotes). Error out if a section '
                              'is given that is not on the config file.'),
                        nargs='+',
                        default=None)
    args = parser.parse_args()
    return args


def parse_config_file(config_file, arguments):
    """
    Reads the given config file and returns a dictionary with section names as keys
    and dictionaries as individual config for its backup, as required by transfer.py/
    backup_mariadb.py.
    == Example file ==
    rotate: True
    retention: 1
    compress: True
    archive: False
    threads: 16
    statistics:
      host: 'db1115.eqiad.wmnet'
      port: 3306
      user: 'a_user'
      password: 'a_password'
      database: 'zarcillo'
    sections:
      s1:
        host: 'db1139.eqiad.wmnet'
        port: 3311
        destination: 'dbprov1001.eqiad.wmnet'
        stop_slave: True
        order: 1
      x1:
        host: 'db1139.eqiad.wmnet'
        port: 3316
        destination: 'dbprov1002.eqiad.wmnet'
        order: 2
    """
    allowed_options = ['host', 'port', 'password', 'destination', 'rotate', 'retention',
                       'compress', 'archive', 'threads', 'statistics', 'only_postprocess',
                       'type', 'stop_slave', 'order', 'stats_file']
    logger = logging.getLogger('backup')
    try:
        read_config = yaml.load(open(config_file), yaml.SafeLoader)
    except yaml.YAMLError:
        logger.error('Error opening or parsing the YAML file %s', config_file)
        sys.exit(2)
    except FileNotFoundError:
        logger.error('File %s not found', config_file)
        sys.exit(2)
    if not isinstance(read_config, dict) or 'sections' not in read_config:
        logger.error('Error reading sections from file %s', config_file)
        sys.exit(2)
    default_options = read_config.copy()
    if 'threads' not in default_options:
        default_options['threads'] = DEFAULT_THREADS
    if 'type' not in default_options:
        default_options['type'] = DEFAULT_BACKUP_TYPE

    del default_options['sections']
    manual_config = read_config['sections']
    if len(manual_config) == 0:
        logger.error('No actual backup was configured to run, please add at least one section')
        sys.exit(2)
    config = dict()

    # fill up sections with default configurations
    for section, section_config in manual_config.items():
        config[section] = section_config.copy()
        for default_key, default_value in default_options.items():
            if default_key not in config[section]:
                config[section][default_key] = default_value

    # take into account command line selection
    if 'all' not in arguments.sections:
        # Hard fail if a section has been given that is not configured
        for section in arguments.sections:
            if section not in config:
                logger.error('Section %s was given on command line, '
                             'but wasn\'t found on config.', section)
                sys.exit(1)
        # remove sections not selected on command line from schedule
        config = {c: config[c] for c in arguments.sections}

    # Check sections don't have unknown parameters
    for section in config.keys():
        for key in config[section].keys():
            if key not in allowed_options:
                logger.error(
                    'Found unknown config option "%s" on section %s',
                    str(key), str(section))
                sys.exit(2)
    return config


def group_config_by_destination(config):
    """
    Modifies read config and groups it on a new dictionary indexed by destination.
    This optimizes for later better parallelization of how backups are scheduled.
    Destinations (keys) are sorted for deterministic results.
    """
    grouped_config = dict()
    for section, section_config in config.items():
        if section_config['destination'] not in grouped_config.keys():
            grouped_config[section_config['destination']] = dict()
        grouped_config[section_config['destination']][section] = section_config
    return grouped_config


def get_transfer_cmd(config, path, using_port=0):
    """
    returns a list with the command to run transfer.py with the given options
    """
    cmd = ['transfer.py']
    cmd.extend(['--type', 'xtrabackup'])
    cmd.extend(['--compress', '--no-encrypt', '--no-checksum'])
    cmd.extend(['--port', str(using_port)])
    if config.get('stop_slave', False):
        cmd.append('--stop-slave')
    port = int(config.get('port', DEFAULT_PORT))
    socket = dbutil.get_socket_from_port(port)
    cmd.extend([config['host'] + ':' + socket])
    cmd.extend([config['destination'] + ':' + path])

    return cmd


def get_chown_cmd(path):
    """
    Returns list with command to run on destination host so files
    transferred had the right owner- chown to the right user and group
    """
    cmd = ['/bin/chown', '--recursive', DUMP_USER + ':' + DUMP_GROUP, path]
    return cmd


def get_prepare_cmd(section, config):
    """
    returns a list with the command to run backup prepare with the given options
    """
    cmd = ['/usr/bin/sudo', '--user', DUMP_USER]
    cmd.extend(['backup-mariadb'])
    cmd.extend([section, '--type', config['type']])
    # snapshots have to be "only_postprocess"ed always
    if (config['type'] == 'snapshot'
            or ('only_postprocess' in config and config['only_postprocess'])):
        cmd.append('--only-postprocess')

    cmd.extend(['--backup-dir', DEFAULT_TRANSFER_DIR])
    cmd.extend(['--host', config['host']])
    if 'port' in config and config['port'] != DEFAULT_PORT:
        cmd.extend(['--port', str(config['port'])])
    cmd.extend(['--threads', str(config['threads'])])
    if 'rotate' in config and config['rotate']:
        cmd.append('--rotate')
    if 'retention' in config:
        cmd.extend(['--retention', str(config['retention'])])
    if 'compress' in config and config['compress']:
        cmd.append('--compress')
    if 'archive' in config and config['archive']:
        cmd.append('--archive')
    if 'stats_file' in config:
        cmd.extend(['--stats-file', config['stats_file']])
    elif 'statistics' in config:
        stats = config['statistics']
        if 'host' in stats:
            cmd.extend(['--stats-host', stats['host']])
        if 'port' in stats:
            cmd.extend(['--stats-port', stats['port']])
        if 'user' in stats:
            cmd.extend(['--stats-user', stats['user']])
        if 'password' in stats:
            cmd.extend(['--stats-password', stats['password']])
        if 'database' in stats:
            cmd.extend(['--stats-database', stats['database']])

    return cmd


def get_backup_name(section, type):
    """
    Returns the name of the backup directory to be created on destination.
    Only the name, not the full path.
    """
    formatted_date = datetime.datetime.now().strftime(DATE_FORMAT)
    backup_name = f'{type}.{section}.{formatted_date}'
    return backup_name


def execute_remotely(host, local_command):
    """
    Executes cmd command remotely on host, and returns the local return code, the standard output
    and the standard error output
    """
    remote_executor = RemoteExecution()
    result = remote_executor.run(host, local_command)
    return result.returncode, result.stdout, result.stderr


def run_transfer(section, config, port=0):
    """
    Executes transfer.py in mode xtrabackup, transfering the contents of a live mysql/mariadb
    server to the provisioning host
    """
    logger = logging.getLogger('backup')
    db_host = config['host']
    db_port = int(config.get('port', DEFAULT_PORT))

    # Create new target dir
    logger.info('Create a new empty directory at %s', config['destination'])
    backup_name = get_backup_name(section, 'snapshot')
    path = os.path.join(DEFAULT_TRANSFER_DIR, backup_name)
    cmd = ['/bin/mkdir', path]
    (returncode, _, err) = execute_remotely(config['destination'], cmd)
    if returncode != 0:
        logger.error(err)
        return (returncode, path)

    # transfer mysql data
    logger.info('Running XtraBackup at %s and sending it to %s',
                db_host + ':' + str(db_port), config['destination'])
    cmd = get_transfer_cmd(config, path, port)
    # ignore stdout, stderr, which can deadlock/overflow the buffer for xtrabackup
    # use asyncio to prevent the busy wait loop that Popen does (we don't need a quick response.
    # this should be a long-running process)
    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    returncode = subprocess.Popen.wait(process)
    if returncode != 0:
        logger.error('Transfer failed for section %s on host %s:%s!',
                     section, db_host, db_port)
        return (returncode, path)

    # chown dir to dump user
    logger.info('Making the resulting dir owned by someone else than root')
    cmd = get_chown_cmd(path)
    returncode, _, _ = execute_remotely(config['destination'], cmd)

    return (returncode, path)


def prepare_backup(section, config):
    """
    Executes remotely backup_mariadb with the only_prepare option, over the files transfered
    with transfer.py so they are prepared, we gather statistics, and compress it according to
    the config
    """
    logger = logging.getLogger('backup')
    logger.info('Preparing backup at %s', config['destination'])
    cmd = get_prepare_cmd(section, config)
    returncode, _, _ = execute_remotely(config['destination'], cmd)
    return returncode


def run(section, config, port=0):
    """
    Executes transfer and prepare (if transfer is correct) on the given section, with the
    given config
    """
    if (('only_postprocess' in config and config['only_postprocess'])
            or config['type'] != 'snapshot'):
        result = prepare_backup(section, config)
    else:
        result, path = run_transfer(section, config, port)
        if result == 0:
            result = prepare_backup(path, config)
    return result


def run_destination(destination, sections):
    """
    Runs all the backups for a particular destination and returns a dictionary
    of return values by section.
    Retry once on fail (to avoid fluke issues- e.g. network errors).
    """
    logger = logging.getLogger('backup')
    result = dict()
    sorted_config = sorted(sections.items(),
                           key=lambda section: section[1].get('order', sys.maxsize))
    # first try
    for section, section_config in sorted_config:
        result[section] = run(section, section_config)
    # do we need to retry once after the first run is completed?
    for section, section_config in sorted_config:
        if result[section] != 0:
            result[section] = run(section, section_config)

    logger.info('All %s backup(s) sent to %s finished', len(result), destination)
    return result


def main():
    """
    main backup logic: setup, argument parsing, config reading,
    execution in parallel by destination and results handling.
    We always return 0 exit code unless there is a parsing error. The reason is
    that it is difficult to judge an error based multiple executions. In the past,
    we returned > 0 if at least one backup run failed, but this wasn't too
    useful, creating too much noise at systemd monitoring.
    Logs (or metadata monitoring) should be used instead to alert (see check
    logic).
    """
    # logging
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format='[%(asctime)s]: %(levelname)s - %(message)s', datefmt='%H:%M:%S'
    )
    logger = logging.getLogger('backup')

    # reading command line arguments
    arguments = get_cmd_arguments()

    # reading configuration
    config = group_config_by_destination(parse_config_file(DEFAULT_CONFIG_FILE, arguments))

    destination_result = dict()
    result = dict()

    # parallel execution
    destination_pool = Pool(len(config))
    for destination, sections in sorted(config.items()):
        destination_result[destination] = destination_pool.apply_async(
            run_destination, (destination, sections)
        )
    destination_pool.close()
    destination_pool.join()

    # results handling
    for destination, result_list in destination_result.items():
        result.update(result_list.get())
    failed_backups = [fb for fb in result if result[fb] > 0]
    if len(failed_backups) == 0:
        logger.info('All %s configured backup(s) run finished correctly', str(len(result)))
    else:
        logger.error('Backup process completed, but some backups finished with error codes: %s',
                     ','.join(failed_backups))
    # we always return success, only fail if there is a parsing error, argument error
    # or other exception
    sys.exit(0)


if __name__ == "__main__":
    main()
