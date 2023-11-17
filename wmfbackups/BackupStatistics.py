import logging
import os
import pymysql
import socket


DEFAULT_STATS_FILE = '/etc/wmfbackups/statistics.ini'


class BackupStatistics:
    """
    Virtual class that defines the interface to generate
    and store the backup statistics.
    """

    def __init__(self, dir_name, section, type, source, backup_dir, config):
        self.dump_name = dir_name
        self.section = section
        self.type = type
        self.source = source
        self.backup_dir = backup_dir

    def start(self):
        pass

    def gather_metrics(self):
        pass

    def fail(self):
        pass

    def finish(self):
        pass

    def delete(self):
        pass


class DisabledBackupStatistics(BackupStatistics):
    """
    Dummy class that does nothing when statistics are requested to be
    generated and stored.
    """
    def __init__(self):
        pass


class DatabaseBackupStatistics(BackupStatistics):
    """
    Generates statistics and stored them on a MySQL/MariaDB database over TLS
    """

    def __init__(self, dir_name, section, type, source, backup_dir, config):
        self.dump_name = dir_name
        self.section = section
        self.type = type
        if source.endswith(':3306'):
            self.source = source[:-5]
        else:
            self.source = source
        self.backup_dir = backup_dir
        self.config = config

    def find_backup_id(self, db):
        """
        Queries the metadata database to find an ongoing backup in the last
        week with the self properties (name, type, source & destination).
        Returns its metadata backups.id value.
        """
        logger = logging.getLogger('backup')
        host = socket.getfqdn()
        query = ("SELECT id FROM backups WHERE name = %s and "
                 "status = 'ongoing' and type = %s and source = %s and "
                 "host = %s and start_date > now() - INTERVAL 7 DAY")
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            try:
                cursor.execute(query, (self.dump_name, self.type,
                                       self.source, host))
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('A MySQL error occurred while finding the entry for the '
                             'backup')
                return None
        data = cursor.fetchall()
        if len(data) != 1:
            logger.error('We could not find one stat entry for an ongoing backup')
            return None
        else:
            return str(data[0]['id'])

    def set_status(self, status):
        """
        Updates or inserts the backup entry at the backup statistics
        database, with the given status (ongoing, finished, failed).
        If it is ongoing, it is considered a new entry (in which case,
        section and source are required parameters.
        Otherwise, it supposes an existing entry with the given name
        exists, and it tries to update it.
        Returns True if it was successful, False otherwise.
        """
        logger = logging.getLogger('backup')
        stats_file = self.config.get('stats_file', DEFAULT_STATS_FILE)
        try:
            db = pymysql.connect(read_default_file=stats_file)
        except (pymysql.err.OperationalError):
            logger.exception('We could not connect to the stats db with config %s',
                             stats_file)
            return False
        if status == 'ongoing':
            if self.section is None or self.source is None:
                logger.error('A new backup requires a section and a source parameters')
                return False
            host = socket.getfqdn()
            query = "INSERT INTO backups (name, status, section, source, host, type," \
                    "start_date, end_date) " \
                    "VALUES (%s, 'ongoing', %s, %s, %s, %s, now(), NULL)"
            with db.cursor(pymysql.cursors.DictCursor) as cursor:
                try:
                    result = cursor.execute(query, (self.dump_name, self.section, self.source,
                                            host, self.type))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred while trying to insert the entry '
                                 'for the new backup')
                    return False
            if result != 1:
                logger.error('We could not store the information on the database')
                return False
            db.commit()
            return True
        elif status in ('finished', 'failed', 'deleted'):
            backup_id = self.find_backup_id(db)
            if backup_id is None:
                return False
            query = "UPDATE backups SET status = %s, end_date = now() WHERE id = %s"
            with db.cursor(pymysql.cursors.DictCursor) as cursor:
                try:
                    result = cursor.execute(query, (status, backup_id))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred while trying to update the '
                                 'entry for the new backup')
                    return False

            if result != 1:
                logger.error('We could not change the status of the current backup')
                return False
            db.commit()
            return True
        else:
            logger.error('Invalid status: {}'.format(status))
            return False

    def recursive_file_traversal(self, db, backup_id, top_dir, directory):
        """
        Traverses 'directory' and its subdirs (assuming top_dir is the absolute starting path),
        inserts metadata on database 'db',
        and returns the total size of the directory, or None if there was an error.
        """
        logger = logging.getLogger('backup')
        total_size = 0
        # TODO: capture file errors
        for name in sorted(os.listdir(os.path.join(top_dir, directory))):
            path = os.path.join(top_dir, directory, name)
            statinfo = os.stat(path)
            size = statinfo.st_size
            total_size += size
            time = statinfo.st_mtime
            # TODO: Identify which object this files corresponds to and record it on
            #       backup_objects
            with db.cursor(pymysql.cursors.DictCursor) as cursor:
                query = ("INSERT INTO backup_files "
                         "(backup_id, file_path, file_name, size, file_date, backup_object_id) "
                         "VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s), NULL)")
                try:
                    result = cursor.execute(query, (backup_id, directory, name, size, time))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred while inserting the backup '
                                 'file details')
                    return None
            if result != 1:
                logger.error('We could not insert details about file {}'.format(name))
                return None
            # traverse subdir
            # TODO: Check for links to avoid infinite recursivity
            if os.path.isdir(path):
                dir_size = self.recursive_file_traversal(db,
                                                         backup_id,
                                                         top_dir,
                                                         os.path.join(directory, name))
                if dir_size is None:
                    return None
                else:
                    total_size += dir_size
        return total_size

    def gather_metrics(self):
        """
        Gathers the file name list, last modification and sizes for the generated files
        and stores it on the given statistics mysql database.
        """
        logger = logging.getLogger('backup')
        stats_file = self.config.get('stats_file', DEFAULT_STATS_FILE)
        # Find the completed backup db entry
        try:
            db = pymysql.connect(read_default_file=stats_file)
        except (pymysql.err.OperationalError):
            logger.exception('We could not connect to the stats db with the config %s',
                             stats_file)
            return False
        backup_id = self.find_backup_id(db)
        if backup_id is None:
            return False

        # Insert the backup file list
        total_size = self.recursive_file_traversal(db=db, backup_id=backup_id,
                                                   top_dir=self.backup_dir, directory='')
        if total_size is None:
            logger.error('An error occurred while traversing the individual backup files')
            return False

        # Update the total backup size
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            query = "UPDATE backups SET total_size = %s WHERE id = %s"
            try:
                result = cursor.execute(query, (total_size, backup_id))
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('A MySQL error occurred while updating the total backup size')
                return False
        db.commit()
        return result == 1

    def start(self):
        self.set_status('ongoing')

    def fail(self):
        self.set_status('failed')

    def finish(self):
        self.set_status('finished')

    def delete(self):
        self.set_status('deleted')
