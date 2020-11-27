import logging
import pymysql
from datetime import datetime

from File import File
from SwiftMedia import SwiftMedia

DEFAULT_BATCH_SIZE = 100


class MySQLConnectionError(Exception):
    pass


class MySQLQueryError(Exception):
    pass


class MySQLMedia:
    """Prepare and generate a media backup"""

    def __init__(self, config):
        """Constructor"""
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 3306)
        self.socket = config.get('socket', '/run/mysqld/mysqld.sock')
        self.wiki = config.get('wiki', 'testwiki')
        self.user = config.get('user', 'root')
        self.password = config.get('password', '')
        self.ssl = config.get('ssl', None)
        self.batchsize = config.get('batchsize', DEFAULT_BATCH_SIZE)
        self.swift = SwiftMedia(config=config)

    def _process_row(self, row, status):
        """
        Given the current files list processed, handle the row information
        in the array and return it, if successful
        """
        upload_name = (row['upload_name'].decode('utf-8')
                       if row['upload_name'] else None)
        size = row['size']
        type = (row['type'].decode('utf-8') if row['type'] else None)
        upload_timestamp = row['upload_timestamp']
        deleted_timestamp = row.get('deleted_timestamp', None)
        sha1 = (File.base36tobase16(row['sha1']) if row['sha1'] not in ('', b'')
                else None)
        md5 = None
        archived_date = None
        archived_timestamp = None
        if row.get('archived_name') is not None:
            archived_name = row.get('archived_name').decode('utf-8')
            if archived_name == '':
                archived_date = '19700101000000'
            else:
                archived_date = archived_name.split('!')[0]
            archived_timestamp = datetime.strptime(archived_date, '%Y%m%d%H%M%S')
        swift_container, swift_name = self.swift.name2swift(image_name=upload_name,
                                                            status=status,
                                                            archive_date=archived_date,
                                                            sha1=sha1)
        return File(wiki=self.wiki, upload_name=upload_name,
                    swift_name=swift_name, swift_container=swift_container,
                    size=size, type=type, status=status,
                    upload_timestamp=upload_timestamp,
                    deleted_timestamp=deleted_timestamp,
                    archived_timestamp=archived_timestamp,
                    sha1=sha1, md5=md5)

    def get_image_ranges(self, swift):
        # generate and return the ranges to obtain images in batches for wikis
        # with large number of images, and iterate on them, returning at most
        # self.batchsize images (less could be returned, depending how batches align)
        # None skips the upper or lower bound.
        if swift.isBigWiki():
            ranges = [None,
                      '0', '05',
                      '1', '15', '19',
                      '20', '2013', '2016', '2018', '2019', '2020',
                      '3', '4', '5', '6', '7', '8', '9']
            ranges.extend([x + y for x in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                           for y in list('0chmqt')])
            ranges.extend(['^', 'В', 'Л', 'С', 'Ե', '儀', None])
        else:
            ranges = [None, None]
        return ranges

    def list_files(self, status='public'):
        """
        Reads the list of all files on the given category of a wiki
        and returns an iterator of File objects
        """
        logger = logging.getLogger('backup')
        swift = SwiftMedia(config={'wiki': self.wiki})
        source = {
            'public':   ("""SELECT img_name as upload_name,
                                   img_name as swift_name,
                                   img_size as size,
                                   img_media_type as type,
                                   STR_TO_DATE(img_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                                   NULL as archived_name,
                                   NULL as deleted_timstamp,
                                   img_sha1 as sha1
                            FROM image""",
                         ('img_name', )),
            'archived': ("""SELECT oi_name as upload_name,
                                   oi_archive_name as swift_name,
                                   oi_size as size,
                                   oi_media_type as type,
                                   STR_TO_DATE(oi_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                                   oi_archive_name as archived_name,
                                   NULL as deleted_timestamp,
                                   oi_sha1 as sha1
                            FROM oldimage""",
                         ('oi_name', 'oi_archive_name')),
            'deleted':  ("""SELECT fa_name as upload_name,
                                   fa_storage_key as swift_name,
                                   fa_size as size,
                                   fa_media_type as type,
                                   STR_TO_DATE(fa_timestamp, '%Y%m%d%H%i%s') as upload_timestamp,
                                   fa_archive_name as archived_name,
                                   STR_TO_DATE(fa_deleted_timestamp, '%Y%m%d%H%i%s')
                                     as deleted_timestamp,
                                   fa_sha1 as sha1
                            FROM filearchive""",
                         ('fa_name', 'fa_storage_key'))}

        ranges = self.get_image_ranges(swift)
        general_query = source[status][0] + ' WHERE 1=1'
        ordering_cols = source[status][1]
        paging_col = ordering_cols[0]
        ordering = ' ORDER BY ' + ', '.join(['`' + col + '`' for col in ordering_cols])

        # MySQL query iteration (paging over title)
        for i in range(len(ranges) - 1):
            lower_limit = ranges[i]
            upper_limit = ranges[i + 1]
            lower_filter = (" AND `{}` >= '{}'".format(paging_col, lower_limit)
                            if lower_limit is not None else '')
            upper_filter = (" AND `{}` < '{}'".format(paging_col, upper_limit)
                            if upper_limit is not None else '')
            query = general_query + lower_filter + upper_filter + ordering
            cursor = self.db.cursor(pymysql.cursors.DictCursor)
            try:
                cursor.execute(query)
            # handle unexpected disconnects
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                logger.warning(('A MySQL error occurred while querying the table, '
                                'retrying connection'))
                self.connect_db()
                cursor = self.db.cursor(pymysql.cursors.DictCursor)
                try:
                    cursor.execute(query)
                except (pymysql.err.ProgrammingError, pymysql.err.InternalError):
                    logger.error(('A MySQL error occurred again while reconnecting '
                                 'and querying the table, aborting'))
                    break
            if cursor.rowcount is None or cursor.rowcount <= 0:
                continue
            # return results in batches of (at most) batchsize for processing
            while True:
                rows = cursor.fetchmany(self.batchsize)
                if not rows:
                    break
                files = list()
                for row in rows:
                    files.append(self._process_row(row, status))
                yield files
            cursor.close()

    def connect_db(self):
        """
        Connect to the database to read the file tables
        """
        logger = logging.getLogger('backup')
        try:
            self.db = pymysql.connect(host=self.host,
                                      port=self.port,
                                      unix_socket=self.socket,
                                      database=self.wiki,
                                      user=self.user,
                                      password=self.password,
                                      ssl=self.ssl)
        except (pymysql.err.OperationalError):
            logger.error(('We could not connect to {} to retrieve the media '
                         'metainformation').format(self.host))
            raise MySQLConnectionError

    def close_db(self):
        """
        Close db connections
        """
        self.db.close()
