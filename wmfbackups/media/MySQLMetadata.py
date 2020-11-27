import logging
import pymysql
from File import File


class MySQLConnectionError(Exception):
    pass


class MySQLQueryError(Exception):
    pass


class MySQLMetadata:
    """Prepare and generate a media backup"""

    def __init__(self, config):
        """Constructor"""
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.socket = config.get('socket', '/run/mysqld/mysqld.sock')
        self.database = config.get('database', 'mediabackups')
        self.user = config.get('user', 'root')
        self.password = config.get('password', '')
        self.ssl = config.get('ssl', None)
        self.batchsize = int(config.get('batchsize', 1000))

    def process_files(self):
        """
        Yields a list of, at each time (at most), batchsize files "pending" to
        process and marks them in state "processing". They are returned in a
        dictionary of Files, keyed by a unique identifier
        """
        logger = logging.getLogger('backup')
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, numeric_backup_status) = self.get_fks()
        string_wiki = {v: k for (k, v) in numeric_wiki.items()}
        string_container = {v: k for (k, v) in numeric_container.items()}
        string_status = {v: k for (k, v) in numeric_status.items()}

        select_query = """SELECT    id, wiki, upload_name,
                                    swift_container, swift_name,
                                    file_type, status, sha1, md5, size,
                                    upload_timestamp, archived_timestamp,
                                    deleted_timestamp
                         FROM       files
                         WHERE      backup_status = %s
                         ORDER BY   id ASC
                         LIMIT      {}
                         FOR UPDATE""".format(self.batchsize)
        while True:
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(select_query, (numeric_backup_status['pending'], ))
            # handle potential loss of connection
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.warning('A MySQL error occurred while selecting the list '
                               'of files, trying to reconnect.')
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    cursor.execute(select_query, (numeric_backup_status['pending'], ))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred again while reconnecting '
                                 'and querying the table, aborting')
                    raise MySQLQueryError
            if cursor.rowcount is None or cursor.rowcount <= 0:
                break
            files = dict()
            rows = cursor.fetchall()
            for row in rows:
                files[row['id']] = File(wiki=string_wiki.get(row['wiki'], None),
                                        upload_name=row['upload_name'].decode('utf-8'),
                                        status=string_status.get(row['status'], None),
                                        swift_container=string_container.get(
                                            row['swift_container'], None
                                        ),
                                        swift_name=row['swift_name'].decode('utf-8'),
                                        sha1=row['sha1'].decode('utf-8'))
            cursor.close()
            update_query = """UPDATE files
                              SET backup_status = %s
                              WHERE id IN ({})""".format(', '.join([str(i) for i in files.keys()]))
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                result = cursor.execute(update_query, (numeric_backup_status['processing'], ))
            # handle potential loss of connection
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.warning('A MySQL error occurred while making the list '
                               'of files "pending", trying to reconnect.')
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    result = cursor.execute(update_query, (numeric_backup_status['processing'], ))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred again while reconnecting '
                                 'and updating the table, aborting')
                    raise MySQLQueryError
                if result != len(files):
                    logger.error('The number of rows updated ({}) was different '
                                 'than expected ({})'.format(result, len(files)))
                    raise MySQLQueryError
            cursor.close()
            self.db.commit()  # unlock the rows

            yield files

    def update_status(self, file_list):
        """
        Updates the status of a file list, an array of dictionaries with the
        following structure:
        {id: numeric row id, file: file object, status: string with the new
        status ('pending', 'processing', etc.)}
        """
        logger = logging.getLogger('backup')
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, numeric_backup_status) = self.get_fks()
        update_query = 'UPDATE files SET backup_status = %s WHERE id = %s'
        insert_query = """INSERT into backups (location, wiki, upload_name, sha1)
                          VALUES (%s, %s, %s, %s)"""
        for file_dictionary in file_list:
            file_id = file_dictionary['id']
            backup_status = file_dictionary['status']
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                result = cursor.execute(update_query,
                                        (numeric_backup_status[backup_status], file_id))
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('A MySQL error occurred again while updating'
                             'the files table for file id {}'.format(file_id))
                return -1
            if result != 1:
                logger.error('Expecting to update 1 row, '
                             'but {} were affected'.format(result))
                raise MySQLQueryError
            cursor.close()
            if backup_status == 'backedup':
                f = file_dictionary['file']
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    result = cursor.execute(insert_query,
                                            ('1', numeric_wiki[f.wiki],  # FIXME multiple locations
                                             f.upload_name, f.sha1))
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error('A MySQL error occurred again while inserting the '
                                 'upload {} on the backups log for id {}'.format(str(f), file_id))
                    return -1
                cursor.close()

        self.db.commit()
        return 0

    def read_dictionary_from_db(self, query):
        """
        Returns a dictionary from the 2-column query given, with the keys from
        the first row, and the values from the second
        """
        logger = logging.getLogger('backup')
        cursor = self.db.cursor()
        try:
            cursor.execute(query)
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning(('A MySQL error occurred while executing: {}, '
                            'trying to reconnect').format(query))
            self.connect_db()
            try:
                cursor.execute(query)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('Query failed again after trying to reconnect')
                raise MySQLQueryError
        rows = cursor.fetchall()
        self.db.commit()
        cursor.close()
        return {row[0].decode('utf-8'): int(row[1]) for row in rows}

    def get_fks(self):
        """
        Queries normalized tables to get the latest values of the foreign keys,
        so a more efficient storage can be achived.
        The following tables are loaded into memory:
        * wikis
        * file_types
        * swift_containers
        * file_status
        The results are returned as a list dictionaries, in the above order.
        """
        logger = logging.getLogger('backup')
        logger.info('Reading foreign key values for the files table from the database')
        wikis = self.read_dictionary_from_db('SELECT wiki_name, id FROM wikis')
        file_types = self.read_dictionary_from_db('SELECT type_name, id FROM file_types')
        swift_containers = self.read_dictionary_from_db(
            'SELECT swift_container_name, id FROM swift_containers'
        )
        file_status = self.read_dictionary_from_db('SELECT status_name, id FROM file_status')
        backup_status = self.read_dictionary_from_db(
            'SELECT backup_status_name, id FROM backup_status'
        )
        return wikis, file_types, swift_containers, file_status, backup_status

    def add(self, files):
        """
        Given a list of files, insert them into the metadata database
        in a single transaction.
        """
        logger = logging.getLogger('backup')
        if len(files) == 0:
            logger.warn('Zero files added, doing nothing')
            return
        # build the optimized insert query
        (numeric_wiki, numeric_type, numeric_container,
         numeric_status, backup_status) = self.get_fks()
        fields = sorted(files[0].properties().keys())
        query = 'INSERT INTO files ({}) VALUES '.format(','.join(fields))
        inserts = list()
        parameters = list()
        for file in files:
            properties = file.properties()
            # override some strings with its numeric value
            properties['wiki'] = numeric_wiki[properties['wiki']]
            properties['file_type'] = numeric_type[properties['file_type']]
            properties['swift_container'] = numeric_container.get(
                properties['swift_container'],
                None
            )
            properties['status'] = numeric_status[properties['status']]
            inserts.append('(' + ', '.join(['%s'] * len(fields)) + ')')
            parameters.extend([properties[key] for key in sorted(properties.keys())])
        query += ', '.join(inserts)

        logger.info('About to insert {} files'.format(len(files)))
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        try:
            result = cursor.execute(query, parameters)
        # handle potential loss of connection
        except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
            logger.warning('A MySQL error occurred while inserting on the files, '
                           'retrying connection')
            self.connect_db()
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(query)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.error('A MySQL error occurred again while reconnecting '
                             'and querying the table, aborting')
                raise MySQLQueryError
        if result != len(files):
            logger.error('Expecting to insert {} rows, '
                         'but {} were affected'.format(len(files), result))
            raise MySQLQueryError
        self.db.commit()
        cursor.close()
        return result

    def connect_db(self):
        """
        Connect to the database to read the file tables
        """
        logger = logging.getLogger('backup')
        try:
            self.db = pymysql.connect(host=self.host,
                                      port=self.port,
                                      unix_socket=self.socket,
                                      database=self.database,
                                      user=self.user,
                                      password=self.password,
                                      ssl=self.ssl)
        except (pymysql.err.OperationalError):
            logger.error('We could not connect to {} to store the stats'.format(self.host))
            raise MySQLConnectionError

    def close_db(self):
        """
        Close db connections
        """
        self.db.close()
