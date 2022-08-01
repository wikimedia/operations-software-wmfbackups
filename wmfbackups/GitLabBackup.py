"""
GitLabBackup Procedure

Generates a backup of a GitLab host by running /usr/bin/gitlab-backup
or /usr/bin/gitlab-ctl backup-etc, depending on which gitlab_backup_type
is set (full, partial, config). Backup calls are abstracted by the
gitlab-backup.sh script.
Metadata of the created backup are checked (for full and partial backups).
Config backups are checked for specific files, because no metadata are created.
"""

import os
import subprocess
from wmfbackups.NullBackup import NullBackup


class GitLabBackup(NullBackup):
    """Creates tarballs of gitlab-generated exports for backup purposes"""

    def get_backup_cmd(self, backup_dir):
        """
        Return list with binary and options to execute to generate a new backup at backup_dir.
        gitlab_backup_type is passed to the gitlab-backup.sh script. Possible options
        are full, partial or config.
        """

        cmd = [os.path.join(backup_dir, 'gitlab-backup.sh')]

        # only allow full, partial or config as additional parameters
        if self.config.get('gitlab_backup_type') in ['full', 'partial', 'config']:
            cmd.extend(self.config.get('gitlab_backup_type'))

        return cmd

    def errors_on_output(self, stdout, stderr):
        """
        Returns true if there were errors on the output of the backup command. As parameters,
        a string containing the standard output and standard error ouput of the backup command.
        Return False if there were not detected errors.
        """
        error_strings = ['Error', 'Backup failed', 'rake aborted']

        if any(error_string in stderr for error_string in error_strings):
            return True
        else:
            return False

    def errors_on_metadata(self, backup_dir):
        """
        Checks the metadata file backup_information.yml of a backup, and sees if it has the right
        content. As a parameter, a string containing the full path of the metadata file.
        Returns False if there were no detected errors.
        For config backups a check for gitlab.rb file is done.
        """

        if self.config.get('gitlab_backup_type') in ['partial', 'full']:
            # check data and partial backup metadata in backup_infromation.yml
            latest_backup_path = os.path.join(backup_dir, 'latest/latest-data.tar')
            cmd = ['/usr/bin/tar -axf', latest_backup_path, 'backup_information.yml', '-O']
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            if process.returncode != 0:
                return True
            if ':backup_created_at' in out:
                return False
            else:
                return True
        else:
            # check if config backup contains central gitlab.rb file
            latest_backup_path = '/etc/gitlab/config_backup/latest/latest-config.tar'
            cmd = ['/usr/bin/tar -axf', latest_backup_path, '/etc/gitlab/gitlab.rb', '-O']
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            if process.returncode != 0:
                return True
            if ':backup_created_at' in out:
                return False
            else:
                return True
