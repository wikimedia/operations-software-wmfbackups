# MariaBackup Procedure

# Generates a snapshots of a host by running mariabackup (xtrabackup compiled
# with MariaDB libraries) and then checks the metadata file is seen as complete,
# and it says it was completed correctly on the log
# Note the "backup command" only works for local databases, if the database
# is on a remote host, transfer.py should be used first and then use the prepare
# command locally

from wmfbackups.NullBackup import NullBackup
import wmfmariadbpy.dbutil as dbutil
import os
import sys

DEFAULT_PORT = 3306


class MariaBackup(NullBackup):

    xtrabackup_path = 'xtrabackup'
    xtrabackup_prepare_memory = '20G'

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
        except OSError:
            return False
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

        return cmd

    def errors_on_output(self, stdout, stderr):
        errors = stderr.decode("utf-8")
        if 'completed OK!' not in errors:
            sys.stderr.write(errors)
            return True
        return False

    def errors_on_log(self):
        return False

    def get_prepare_cmd(self, backup_dir):
        """
        Once an xtrabackup backup has completed, run prepare so it is ready to be copied back
        """
        cmd = self._get_xtraback_prepare_cmd(backup_dir)
        return cmd

    def errors_on_prepare(self, stdout, stderr):
        return self.errors_on_output(stdout, stderr)

    def archive_databases(self, source, threads):
        # FIXME: Allow database archiving for xtrabackup
        pass
