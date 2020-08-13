#!/usr/bin/python3

from wmfmariadbpy.CuminExecution import CuminExecution as RemoteExecution
from wmfmariadbpy.WMFMariaDB import WMFMariaDB

import datetime
from multiprocessing.pool import Pool
import logging
import os
import subprocess
import sys
import yaml

DEFAULT_CONFIG_FILE = '/etc/mysql/backups.cnf'
DEFAULT_THREADS = 16
DEFAULT_BACKUP_TYPE = 'dump'
CONCURRENT_BACKUPS = 2
DEFAULT_PORT = 3306
DEFAULT_TRANSFER_DIR = '/srv/backups/snapshots/ongoing'
DATE_FORMAT = '%Y-%m-%d--%H-%M-%S'
DUMP_USER = 'dump'
DUMP_GROUP = 'dump'


def parse_config_file(config_file):
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
                       'type', 'stop_slave', 'order']
    logger = logging.getLogger('backup')
    try:
        read_config = yaml.load(open(config_file))
    except yaml.YAMLError:
        logger.error('Error opening or parsing the YAML file {}'.format(config_file))
        sys.exit(2)
    except FileNotFoundError:  # noqa: F821
        logger.error('File {} not found'.format(config_file))
        sys.exit(2)
    if not isinstance(read_config, dict) or 'sections' not in read_config:
        logger.error('Error reading sections from file {}'.format(config_file))
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
    elif len(manual_config) > 1:
        # Limit the threads only if there is more than 1 backup
        default_options['threads'] = int(default_options['threads'] / CONCURRENT_BACKUPS)
    config = dict()
    for section, section_config in manual_config.items():
        # fill up sections with default configurations
        config[section] = section_config.copy()
        for default_key, default_value in default_options.items():
            if default_key not in config[section]:
                config[section][default_key] = default_value
    # Check sections don't have unknown parameters
    for section in config.keys():
        for key in config[section].keys():
            if key not in allowed_options:
                logger.error(
                    'Found unknown config option "{}" on section {}'.format(
                        str(key), str(section))
                )
                sys.exit(2)
    return config


def get_transfer_cmd(config, using_port, path):
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
    socket = WMFMariaDB.get_socket_from_port(port)
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
    cmd.extend(['/usr/bin/python3', '/usr/local/bin/backup_mariadb.py'])
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
    if 'statistics' in config:
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
    backup_name = '{}.{}.{}'.format(type, section, formatted_date)
    return backup_name


def execute_remotely(host, local_command):
    """
    Executes cmd command remotely on host, and returns the local return code, the standard output
    and the standard error output
    """
    remote_executor = RemoteExecution()
    result = remote_executor.run(host, local_command)
    return result.returncode, result.stdout, result.stderr


def run_transfer(section, config, port):
    """
    Executes transfer.py in mode xtrabackup, transfering the contents of a live mysql/mariadb
    server to the provisioning host
    """
    logger = logging.getLogger('backup')
    db_host = config['host']
    db_port = int(config.get('port', DEFAULT_PORT))

    # Create new target dir
    logger.info('Create a new empty directory at {}'.format(config['destination']))
    backup_name = get_backup_name(section, 'snapshot')
    path = os.path.join(DEFAULT_TRANSFER_DIR, backup_name)
    cmd = ['/bin/mkdir', path]
    (returncode, out, err) = execute_remotely(config['destination'], cmd)
    if returncode != 0:
        logger.error(err)
        return (returncode, path)

    # transfer mysql data
    logger.info('Running XtraBackup at {} and sending it to {}'.format(
        db_host + ':' + str(db_port), config['destination']))
    cmd = get_transfer_cmd(config, port, path)
    # ignore stdout, stderr, which can deadlock/overflow the buffer for xtrabackup
    # use asyncio to prevent the busy wait loop that Popen does (we don't need a quick response.
    # this should be a long-running process)
    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    returncode = subprocess.Popen.wait(process)
    if returncode != 0:
        logger.error('Transfer failed!')
        return (returncode, path)

    # chown dir to dump user
    logger.info('Making the resulting dir owned by someone else than root')
    cmd = get_chown_cmd(path)
    (returncode, out, err) = execute_remotely(config['destination'], cmd)

    return (returncode, path)


def prepare_backup(section, config):
    """
    Executes remotely backup_mariadb with the only_prepare option, over the files transfered
    with transfer.py so they are prepared, we gather statistics, and compress it according to
    the config
    """
    logger = logging.getLogger('backup')
    logger.info('Preparing backup at {}'.format(config['destination']))
    cmd = get_prepare_cmd(section, config)
    (returncode, out, err) = execute_remotely(config['destination'], cmd)
    return returncode


def run(section, config, port):
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


def main():
    logger = logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format='[%(asctime)s]: %(levelname)s - %(message)s', datefmt='%H:%M:%S'
    )
    logger = logging.getLogger('backup')
    config = parse_config_file(DEFAULT_CONFIG_FILE)
    result = dict()
    backup_pool = Pool(CONCURRENT_BACKUPS)
    port = 4444
    sorted_config = sorted(config.items(),
                           key=lambda section: section[1].get('order', sys.maxsize))
    for section, section_config in sorted_config:
        result[section] = backup_pool.apply_async(run, (section, section_config, port))
        port += 1
    backup_pool.close()
    backup_pool.join()

    logger.info('Backup finished correctly')
    sys.exit(result[max(result, key=lambda key: result[key].get())].get())


if __name__ == "__main__":
    main()
