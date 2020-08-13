# Null Backup Procedure

# As the title suggests, this backup does nothing, it is a skeleton used
# for debugging and other example purposes that, if run, does absolutely
# nothing (it generates no backup files or reads from anywhere)


class NullBackup:

    config = dict()

    def __init__(self, config, backup):
        """
        Initialize commands
        """
        self.config = config
        self.backup = backup
        self.logger = backup.logger

    def get_backup_cmd(self, backup_dir):
        """
        Return list with binary and options to execute to generate a new backup at backup_dir
        """
        return '/bin/true'

    def get_prepare_cmd(self, backup_dir):
        """
        Return list with binary and options to execute to prepare an existing backup. Return
        none if prepare is not necessary (nothing will be executed in that case).
        """
        return ''

    def errors_on_output(self, stdout, stderr):
        """
        Returns true if there were errors on the output of the backup command. As parameters,
        a string containing the standard output and standard error ouput of the backup command.
        Return False if there were not detected errors.
        """
        return False

    def errors_on_log(self, log_file):
        """
        Returns true if there were errors on the log of the backup command. As a parameter,
        a string containing the full path of the log file.
        Return False if there were not detected errors.
        """
        return False

    def errors_on_metadata(self, backup_dir):
        """
        Checks the metadata file of a backup, and sees if it has the right format and content.
        As a parameter, a string containing the full path of the metadata file.
        Returns False if tehre were no detected errors.
        """
        return False

    def errors_on_prepare(self, stdout, stderr):
        return False
