#!/usr/bin/python3

import File
import SwiftMedia
import MySQLMetadata
import S3

import logging
import os

work_dir = '/home/jynus/enwiki'
swift_config = {'wiki': 'enwiki',
                'batchsize': 1000}
metadata_config = {'host': 'db1133.eqiad.wmnet',
                   'socket': None,
                   'database': 'mediabackups',
                   'user': '',
                   'password': '',
                   'ssl': {'ca': '/etc/ssl/certs/Puppet_Internal_CA.pem'},
                   'batchsize': 10}

logger = logging.getLogger('backup')
swift = SwiftMedia.SwiftMedia(config={})
metadata = MySQLMetadata.MySQLMetadata(metadata_config)
s3api = S3.S3(bucket='mediabackups')

metadata.connect_db()
for batch in metadata.process_files():
    status_list = list()
    for file_id, f in batch.items():
        basename = f.swift_name.split('/')[-1]
        download_path = os.path.join(work_dir, basename)

        if swift.download(f, download_path):
            new_status = 'error'
            logger.error('Download of "{}" failed'.format(str(f)))
            continue
        sha1 = File.File.sha1sum(download_path)
        if f.sha1.zfill(40) != sha1.zfill(40):
            logger.warning('Calculated ({}) and queried ({}) sha1 checksum '
                           'are not the same for "{}"'.format(sha1, f.sha1,
                                                              f.upload_name))
            f.sha1 = sha1
        upload_name = f.upload_name if f.upload_name is not None else 'unknown'
        backup_name = os.path.join(f.wiki, upload_name, sha1)
        if s3api.upload_file(download_path, backup_name):
            logger.error('Upload of "{}" failed'.format(str(f)))
            new_status = 'error'
        else:
            new_status = 'backedup'
            logger.info('Backup of "{}" completed correctly'.format(str(f)))
        status_list.append({'id': file_id, 'file': f, 'status': new_status})
        os.remove(download_path)
    metadata.update_status(status_list)
metadata.close_db()
