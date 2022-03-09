import re
import datetime
import logging
import os
import subprocess
import shutil
import sys

from wmfbackups.BackupStatistics import DatabaseBackupStatistics, DisabledBackupStatistics
from wmfbackups.NullBackup import NullBackup, BackupException
from wmfbackups.MariaBackup import MariaBackup
from wmfbackups.MyDumperBackup import MyDumperBackup

DEFAULT_BACKUP_PATH = '/srv/backups'
ONGOING_BACKUP_DIR = 'ongoing'
FINAL_BACKUP_DIR = 'latest'
ARCHIVE_BACKUP_DIR = 'archive'
DATE_FORMAT = '%Y-%m-%d--%H-%M-%S'
DEFAULT_BACKUP_TYPE = 'dump'
DEFAULT_BACKUP_THREADS = 18
DEFAULT_PORT = 3306
DEFAULT_RETENTION_DAYS = 18
SUPPORTED_BACKUP_TYPES = ['dump', 'snapshot', 'null']


class WMFBackup:
    """
    Backup generation and handling class (preparation, rotation, archiving, compression, etc.)
    """
    name = None  # e.g. s1, tendril, s4-test
    config = {}  # dictionary with backup config (type, backup_dir, ...)
    logger = None  # object of clas logging
    dir_name = None  # e.g. dump.s1.2019-01-01--11-34-45
    file_name = None  # e.g. dump.s1.2019-01-01--11-34-45.tar.gz
    log_file = None  # e.g. /srv/backups/dumps/dump_log.s1

    @property
    def default_ongoing_backup_dir(self):
        return os.path.join(DEFAULT_BACKUP_PATH, self.config['type'] + 's', ONGOING_BACKUP_DIR)

    @property
    def default_final_backup_dir(self):
        return os.path.join(DEFAULT_BACKUP_PATH, self.config['type'] + 's', FINAL_BACKUP_DIR)

    @property
    def default_archive_backup_dir(self):
        return os.path.join(DEFAULT_BACKUP_PATH, self.config['type'] + 's', ARCHIVE_BACKUP_DIR)

    @property
    def name_regex(self):
        return self.config['type'] + \
               r'\.([a-z0-9\-_]+)\.(20\d\d-[01]\d-[0123]\d\--\d\d-\d\d-\d\d)(\.[a-z0-9\.]+)?'

    def generate_file_name(self, backup_dir):
        formatted_date = datetime.datetime.now().strftime(DATE_FORMAT)
        self.dir_name = f'{self.config.get("type")}.{self.name}.{formatted_date}'
        if self.config.get('compress', False):
            extension = '.tar.gz'
        else:
            extension = ''
        self.file_name = self.dir_name + extension
        self.log_file = os.path.join(backup_dir, f'{self.config.get("type")}_log.{self.name}')

    def parse_backup_file(self):
        """
        Given a self.name being an absolute path (not a section), generate the actual section name,
        backup_dir, dir_name, file_name, and log file
        """
        type = self.config['type']
        regex = r'.*/(([^/\.]+)\.([^/\.]+)\.\d\d\d\d-\d\d-\d\d--\d\d-\d\d-\d\d)/*$'
        result = re.match(regex, self.name)
        if not result or not os.path.isdir(self.name):
            self.logger.error('%s is not a valid absolute path directory', self.name)
            return None
        if result.group(2) != type:
            msg = 'A %s backup was requested, but a %s dir was provided'
            self.logger.error(msg, type, result.group(2))
            return None
        backup_dir = os.path.normpath(os.path.join(self.name, '..'))  # /backups/ongoing
        self.name = result.group(3)  # section identifier e.g. 's1'
        self.dir_name = result.group(1)  # type.section.date
        if self.config.get('compress', False):
            extension = '.tar.gz'
        else:
            extension = ''
        self.file_name = self.dir_name + extension  # e.g. type.section.date.tar.gz
        self.log_file = os.path.join(backup_dir, '{}_log.{}'.format(type, self.name))
        return 0

    def find_backup_file(self, backup_dir):
        """
        Generates the backup name and returns the log path of the only backup file/dir within
        backup_dir patch of the correct name and type.
        If there is none or more than one, log an error and return None.
        """
        type = self.config['type']
        name = self.name
        # TODO: Use regex instead? Or make compulsory a full name?
        try:
            potential_files = [f for f in os.listdir(backup_dir) if f.startswith('.'.join([type,
                                                                                           name,
                                                                                           '']))]
        except FileNotFoundError:
            self.logger.error('%s directory not found', backup_dir)
            return None
        if len(potential_files) != 1:
            msg = 'Expecting 1 matching %s for %s, found %s'
            self.logger.error(msg, type, name, len(potential_files))
            return None
        self.dir_name = potential_files[0]
        if self.config.get('compress', False):
            extension = '.tar.gz'
        else:
            extension = ''
        self.file_name = self.dir_name + extension
        self.log_file = os.path.join(backup_dir, f'{type}_log.{name}')
        return 0

    def os_rename(self, source, destination):
        """
        os.rename only works between same devices (not physical rename with
        copy + delete. Reimplement it with execution of mv and handle
        the errors.
        """
        cmd = ['/bin/mv', source, destination]
        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        returncode = subprocess.Popen.wait(process)
        return returncode

    def move_backups(self, name, source, destination, regex):
        """
        Move directories (and all its contents) from source to destination
        for all dirs that have the right format (dump.section.date) and
        section matches the given name
        """
        files = os.listdir(source)
        pattern = re.compile(regex)
        for entry in files:
            match = pattern.match(entry)
            if match is None:
                continue
            if name == match.group(1):
                self.logger.debug('Archiving %s', entry)
                path = os.path.join(source, entry)
                result = self.os_rename(path, os.path.join(destination, entry))
                if result != 0:
                    return result
        return 0

    def purge_backups(self, source=None, days=None, regex=None):
        """
        Remove subdirectories in source dir and all its contents for dirs/files that
        have the right format (dump.section.date), its sections matches the current
        section, and are older than the given
        number of days.
        """
        if source is None:
            source = self.default_archive_backup_dir
        if days is None:
            days = self.config['retention']
        if regex is None:
            regex = self.name_regex
        files = os.listdir(source)
        pattern = re.compile(regex)
        for entry in files:
            path = os.path.join(source, entry)
            match = pattern.match(entry)
            if match is None:
                continue
            if self.name != match.group(1):
                continue
            timestamp = datetime.datetime.strptime(match.group(2), DATE_FORMAT)
            if (timestamp < (datetime.datetime.now() - datetime.timedelta(days=days)) and
                    timestamp > datetime.datetime(2018, 1, 1)):
                self.logger.debug('purging backup %s', path)
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                except OSError as e:
                    return e.errno
        return 0

    def tar_and_remove(self, source, name, files, compression=None):
        """Create a tar with the given path and remove the original file or files
           at the same time, to save space"""
        cmd = ['/bin/tar']
        tar_file = os.path.join(source, '{}'.format(name))
        cmd.extend(['--create', '--remove-files', '--file', tar_file, '--directory', source])
        if compression is not None:
            cmd.extend(['--use-compress-program', compression])
        cmd.extend(files)

        self.logger.debug(cmd)
        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        returncode = subprocess.Popen.wait(process)
        return returncode

    def run(self):
        """
        Perform a backup of the given instance,
        with the given config. Once finished successfully, consolidate the
        number of files if asked, and move it to the "latest" dir. Archive
        any previous dump of the same name, if required.
        """
        type = self.config.get('type', DEFAULT_BACKUP_TYPE)
        backup_dir = self.config.get('backup_dir', self.default_ongoing_backup_dir)
        archive = self.config.get('archive', False)
        only_postprocess = self.config.get('only_postprocess', False)
        compress = self.config.get('compress', False)
        rotate = self.config.get('rotate', False)
        threads = self.config.get('threads', DEFAULT_BACKUP_THREADS)

        # find or generate the backup file/dir
        if only_postprocess:
            if self.name.startswith('/'):  # if passed an absolute path as section name
                # basedir doesn't work as intended if passed /a/path/like/this/
                backup_dir = os.path.normpath(os.path.join(self.name, '..'))
                self.parse_backup_file()
            else:
                self.find_backup_file(backup_dir)
            if self.file_name is None:
                msg = 'Problem while trying to find the backup files at %s'
                self.logger.error(msg, backup_dir)
                return 10
        else:
            self.generate_file_name(backup_dir)

        output_dir = os.path.join(backup_dir, self.dir_name)
        if type == 'dump':
            backup = MyDumperBackup(self.config, self)
        elif type == 'snapshot':
            backup = MariaBackup(self.config, self)
        elif type == 'null':
            backup = NullBackup(self.config, self)
        else:
            self.logger.error('Unrecognized backup format: %s', type)
            return 11

        # get the backup command
        if not only_postprocess:
            cmd = backup.get_backup_cmd(backup_dir)

        # start status monitoring
        if 'statistics' in self.config:  # Enable statistics gathering?
            source = self.config.get('host', 'localhost') + \
                     ':' + \
                     str(self.config.get('port', DEFAULT_PORT))
            stats = DatabaseBackupStatistics(dir_name=self.dir_name, section=self.name,
                                             type=type, config=self.config.get('statistics'),
                                             backup_dir=output_dir, source=source)
        else:
            stats = DisabledBackupStatistics()

        stats.start()

        if not only_postprocess:
            # run backup command
            self.logger.debug(cmd)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            if backup.errors_on_output(out, err):
                stats.fail()
                return 3

        # Check log for errors
        if backup.errors_on_log():
            self.logger.error('Error log found at %s', self.log_file)
            stats.fail()
            return 4

        # Check medatada file exists and containg the finish date
        if backup.errors_on_metadata(backup_dir):
            self.logger.error('Incorrect metadata file')
            stats.fail()
            return 5

        # Backups seems ok, prepare it for recovery and cleanup
        try:
            cmd = backup.get_prepare_cmd(backup_dir)
        except BackupException as ex:
            self.logger.error(str(ex))
            stats.fail()
            return 13
        if cmd != '':
            self.logger.debug(cmd)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            if backup.errors_on_prepare(out, err):
                self.logger.error('The mariabackup prepare process did not complete successfully')
                stats.fail()
                return 6

        # get file statistics
        stats.gather_metrics()

        if archive:
            backup.archive_databases(output_dir, threads)

        if compress:
            # no consolidation per-db, just compress the whole thing
            result = self.tar_and_remove(backup_dir, self.file_name, [self.dir_name, ],
                                         compression='/usr/bin/pigz -p {}'.format(threads))
            if result != 0:
                self.logger.error('The compression process failed')
                stats.fail()
                return 11

        if rotate:
            # perform rotations
            # move the old latest one to the archive, and the current as the latest
            # then delete old backups of the same section, according to the retention
            # config
            result = self.move_backups(self.name, self.default_final_backup_dir,
                                       self.default_archive_backup_dir, self.name_regex)
            if result != 0:
                self.logger.warning('Archiving backups failed')
            result = self.os_rename(os.path.join(backup_dir, self.file_name),
                                    os.path.join(self.default_final_backup_dir, self.file_name))
            if result != 0:
                self.logger.error('Moving backup to final dir failed')
                stats.fail()
                return 12
            result = self.purge_backups()
            if result != 0:
                self.logger.warning('Purging old backups failed')

        # we are done
        stats.finish()
        return 0

    def __init__(self, name, config):
        """
        Constructor requires the dictionary with the backup configuration, the name of the backup
        """
        self.name = name
        self.config = config
        self.logger = logging.getLogger(name)
        if 'type' not in config:
            self.config['type'] = DEFAULT_BACKUP_TYPE
        elif config['type'] not in SUPPORTED_BACKUP_TYPES:
            self.logger.error('Unknown dump type: %s', config['type'])
            sys.exit(-1)
        if 'retention' not in config:
            self.config['retention'] = DEFAULT_RETENTION_DAYS
        else:
            self.config['retention'] = int(config['retention'])
