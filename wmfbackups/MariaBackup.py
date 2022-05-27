"""
MariaBackup Procedure

Generates a snapshots of a host by running mariabackup (xtrabackup compiled
with MariaDB libraries) and then checks the metadata file is seen as complete,
and it says it was completed correctly on the log
Note the "backup command" only works for local databases, if the database
is on a remote host, transfer.py should be used first and then use the prepare
command locally
"""

import os
import re
import subprocess

import wmfmariadbpy.dbutil as dbutil
from wmfbackups.NullBackup import NullBackup, BackupException

DEFAULT_PORT = 3306
SERVER_VERSION_REGEX = r'(\d+\.\d+)\.(\d+)(\-([^\s]+))?'


class XtrabackupError(BackupException):
    """Used to raise errors related to xtrabackup execution"""


class MariaBackup(NullBackup):
    """Implements NullBackup by allowing backup generation and preparation
       with mariabackup, while using the default xtrabackup executable on path"""

    xtrabackup_path = 'xtrabackup'
    xtrabackup_prepare_memory = '40G'
    xtrabackup_open_files_limit = '200000'

    def uniformize_vendor_string(self, original_vendor):
        """
        Version string can be faked on config. In particular, it adds a '-log' string
        depending if binary log is enabled or disabled.
        """
        if original_vendor is None or original_vendor in ['', 'log', 'debug', 'valgrind', 'embedded']:
            return 'MySQL'
        if original_vendor.startswith('MariaDB'):
            return 'MariaDB'
        if re.match(r'\d+([\.\-]\d+(\-.+)?)?', original_vendor):
            return 'Percona Server'
        return None

    def _get_xtrabackup_version(self):
        """
        Execute xtrabackup --version and return the server version it was
        compiled against, as major version, minor version and vendor
        """
        cmd = [self.xtrabackup_path, '--version']
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, err = process.communicate()  # xtrabackup returns version to stderr
        if process.returncode != 0:
            raise XtrabackupError('--version failed to execute')
        search = re.search(SERVER_VERSION_REGEX, err.decode())
        if search is None:
            raise XtrabackupError('--version didn\'t provide a recognizable version')
        major_version = search.group(1)
        try:
            minor_version = int(search.group(2))
        except ValueError:
            raise XtrabackupError('--version didn\'t provide a numeric minor version')
        vendor = self.uniformize_vendor_string(search.group(4))
        return {'major': major_version, 'minor': minor_version, 'vendor': vendor}

    def _get_backup_source_server_version(self, backup_dir):
        """
        Open the xtrabackup_info file from the given backup dir, read it and
        extract the server version
        """
        metadata_file = os.path.join(backup_dir, self.backup.dir_name, 'xtrabackup_info')
        with open(metadata_file, 'r', errors='ignore') as metadata_file:
            metadata = metadata_file.read()
        search = re.search(f'server_version\\s*=\\s*({SERVER_VERSION_REGEX})', metadata)
        if search is None or search.group(1) is None:
            raise XtrabackupError('xtrabackup_info file does not exist or '
                                  'it does not contain a server version')
        major_version = search.group(2)
        try:
            minor_version = int(search.group(3))
        except ValueError:
            raise XtrabackupError('xtrabackup_info didn\'t provide a numeric minor version')
        vendor = self.uniformize_vendor_string(search.group(5))
        return {'major': major_version, 'minor': minor_version, 'vendor': vendor}

    def get_backup_cmd(self, backup_dir):
        """
        Given a config, returns a command line for mydumper, the name
        of the expected snapshot, and the log path.
        """
        cmd = [self.xtrabackup_path, '--backup']

        output_dir = os.path.join(backup_dir, self.backup.dir_name)
        cmd.extend(['--target-dir', output_dir])
        port = int(self.config.get('port', DEFAULT_PORT))
        datadir = dbutil.get_datadir_from_port(port)
        socket = dbutil.get_socket_from_port(port)
        cmd.extend(['--datadir', datadir])
        cmd.extend(['--socket', socket])
        if 'regex' in self.config and self.config['regex'] is not None:
            cmd.extend(['--tables', self.config['regex']])

        if 'user' in self.config:
            cmd.extend(['--user', self.config['user']])
        if 'password' in self.config:
            cmd.extend(['--password', self.config['password']])

        return cmd

    def errors_on_metadata(self, backup_dir):
        metadata_file = os.path.join(backup_dir, self.backup.dir_name, 'xtrabackup_info')
        try:
            with open(metadata_file, 'r', errors='ignore') as metadata_file:
                metadata = metadata_file.read()
        except (OSError, IOError):
            return True
        if 'end_time = ' not in metadata:
            return True
        return False

    def _get_xtraback_prepare_cmd(self, backup_dir):
        """
        Returns the command needed to run the backup prepare
        (REDO and UNDO actions to make the backup consistent)
        """
        path = os.path.join(backup_dir, self.backup.dir_name)
        cmd = [self.xtrabackup_path, '--prepare']
        cmd.extend(['--target-dir', path])
        # TODO: Make the amount of memory configurable
        # WARNING: apparently, --innodb-buffer-pool-size fails sometimes
        cmd.extend(['--use-memory', self.xtrabackup_prepare_memory])
        cmd.extend(['--open-files-limit', self.xtrabackup_open_files_limit])

        return cmd

    def errors_on_output(self, stdout, stderr):
        errors = stderr.decode("utf-8")
        if 'completed OK!' not in errors:
            self.logger.error(errors)
            return True
        return False

    def errors_on_log(self):
        return False

    def get_prepare_cmd(self, backup_dir):
        """
        Once an xtrabackup backup has completed, run prepare so it is ready to be copied back
        """
        # Fail hard under certain version missmatches
        xtrabackup_version = self._get_xtrabackup_version()
        backup_version = self._get_backup_source_server_version(backup_dir)
        if (xtrabackup_version['vendor'] != backup_version['vendor'] or
                xtrabackup_version['major'] != backup_version['major'] or
                xtrabackup_version['minor'] < backup_version['minor']):
            raise XtrabackupError(f'xtrabackup version mismatch- '
                                  f'xtrabackup version: {xtrabackup_version}, '
                                  f'backup version: {backup_version}')

        cmd = self._get_xtraback_prepare_cmd(backup_dir)
        return cmd

    def errors_on_prepare(self, stdout, stderr):
        return self.errors_on_output(stdout, stderr)

    def archive_databases(self, source, threads):
        """Archive xtrabackup-generated dbs (generating a tar bundle per database) -
           NOT IMPLEMENTED - configuring it will do nothing"""
        # FIXME: allow archiving databases for xtrabackup backup method
