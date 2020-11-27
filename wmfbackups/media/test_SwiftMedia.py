#!/usr/bin/python3

import SwiftMedia
import sys

'''
Downloads all files of the given wiki and status to the local dir
'''

wiki = sys.argv[1]
status = sys.argv[2]
config = {'wiki': wiki}
backup = SwiftMedia.SwiftMedia(config)

for batch in backup.list_files(status):
    for f in batch:
        print(f)
        backup.download(f, '')
