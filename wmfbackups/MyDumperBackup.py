# MyDumper Procedure
# Generates a logical dump using mydumper, allowing fast parallel and
# compressed copies of the database objects.

from wmfbackups.NullBackup import NullBackup
from multiprocessing.pool import ThreadPool
import os

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306


class MyDumperBackup(NullBackup):

    rows = 20000000

    def get_backup_cmd(self, backup_dir):
        """
        Given a config, returns a command line for mydumper, the name
        of the expected dump, and the log path.
        """
        # FIXME: even if there is not privilege escalation (everybody can run
        # mydumper and parameters are gotten from a localhost file),
        # check parameters better to avoid unintended effects
        cmd = ['/usr/bin/mydumper']
        cmd.extend(['--compress', '--events', '--triggers', '--routines'])

        cmd.extend(['--logfile', self.backup.log_file])
        output_dir = os.path.join(backup_dir, self.backup.dir_name)
        cmd.extend(['--outputdir', output_dir])

        rows = int(self.backup.config.get('rows', self.rows))
        cmd.extend(['--rows', str(rows)])
        cmd.extend(['--threads', str(self.backup.config['threads'])])
        host = self.backup.config.get('host', DEFAULT_HOST)
        cmd.extend(['--host', host])
        port = int(self.backup.config.get('port', DEFAULT_PORT))
        cmd.extend(['--port', str(port)])
        if 'regex' in self.backup.config and self.backup.config['regex'] is not None:
            cmd.extend(['--regex', self.backup.config['regex']])

        if 'user' in self.backup.config:
            cmd.extend(['--user', self.backup.config['user']])
        if 'password' in self.backup.config:
            cmd.extend(['--password', self.backup.config['password']])

        return cmd

    def get_prepare_cmd(self, backup_dir):
        return ''

    def errors_on_metadata(self, backup_dir):
        metadata_file = os.path.join(backup_dir, self.backup.dir_name, 'metadata')
        try:
            with open(metadata_file, 'r', errors='ignore') as metadata_file:
                metadata = metadata_file.read()
        except OSError:
            return True
        if 'Finished dump at: ' not in metadata:
            return True
        return False

    def archive_databases(self, source, threads):
        """
        To avoid too many files per backup output, archive each database file in
        separate tar files for given directory "source". The threads
        parameter allows to control the concurrency (number of threads executing
        tar in parallel).
        """

        # TODO: Ignore already archived databases, so a second run is idempotent
        files = sorted(os.listdir(source))

        schema_files = list()
        name = None
        pool = ThreadPool(threads)
        for item in files:
            if item.endswith('-schema-create.sql.gz') or item == 'metadata':
                if schema_files:
                    pool.apply_async(self.backup.tar_and_remove, (source, name, schema_files))
                    schema_files = list()
                if item != 'metadata':
                    schema_files.append(item)
                    name = item.replace('-schema-create.sql.gz', '.gz.tar')
            else:
                schema_files.append(item)
        if schema_files:
            pool.apply_async(self.backup.tar_and_remove, (source, name, schema_files))

        # TODO: Missing error handling

        pool.close()
        pool.join()

    def errors_on_output(self, stdout, stderr):
        errors = stderr.decode("utf-8")
        if ' CRITICAL ' in errors:
            return 3

    def errors_on_log(self, log_file):
        try:
            with open(log_file, 'r') as output:
                log = output.read()
        except OSError:
            return True
        if ' [ERROR] ' in log:
            return True

    def errors_on_prepare(self, stdout, stderr):
        return False
