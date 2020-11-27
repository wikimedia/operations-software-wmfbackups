#!/usr/bin/python3

import MySQLMedia
import MySQLMetadata

read_config = {'host': 'db1150.eqiad.wmnet',
               'port': 3314,
               'socket': None,
               'wiki': 'commonswiki',
               'user': '',
               'password': '',
               'ssl': {'ca': '/etc/ssl/certs/Puppet_Internal_CA.pem'},
               'batchsize': 1000}
write_config = {'host': 'db1133.eqiad.wmnet',
                'socket': None,
                'database': 'mediabackups',
                'user': '',
                'password': '',
                'ssl': {'ca': '/etc/ssl/certs/Puppet_Internal_CA.pem'}}
backup = MySQLMedia.MySQLMedia(config=read_config)
metadata = MySQLMetadata.MySQLMetadata(write_config)
backup.connect_db()
metadata.connect_db()
for status in ['public', 'archived', 'deleted']:
    print()
    print('=================== {} ==================='.format(status))
    for batch in backup.list_files(status=status):
        for f in batch:
            print(f)
        metadata.add(batch)
backup.close_db()
metadata.close_db()
