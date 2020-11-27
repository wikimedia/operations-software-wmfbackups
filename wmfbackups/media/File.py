from numpy import base_repr
import hashlib


class File:
    def __init__(self, wiki, upload_name, status, size=None, type=None,
                 upload_timestamp=None, deleted_timestamp=None, sha1=None, md5=None,
                 swift_container=None, swift_name=None, archived_timestamp=None):
        self.wiki = wiki
        self.upload_name = upload_name
        self.size = size
        self.type = type if type else 'ERROR'
        self.status = status
        self.upload_timestamp = upload_timestamp
        self.deleted_timestamp = deleted_timestamp
        self.archived_timestamp = archived_timestamp
        self.sha1 = sha1
        self.md5 = md5
        self.swift_container = swift_container
        self.swift_name = swift_name

    @staticmethod
    def sha1sum(path):
        '''Calculates the sha1 sum of a given file'''
        sha1sum = hashlib.sha1()
        with open(path, 'rb') as fd:
            block = fd.read(2**16)
            while len(block) != 0:
                sha1sum.update(block)
                block = fd.read(2**16)
        return sha1sum.hexdigest().zfill(40)

    @staticmethod
    def base16tobase36(number):
        """
        Given a utf-8 string representing a 16-base (hexadecimal)
        number, return the equivalent string representation on
        base36.
        """
        return base_repr(int(number, 16), 36).lower().zfill(31)

    @staticmethod
    def base36tobase16(number):
        """
        Given a utf-8 string representing a 36-base number,
        return the equivalent string representation on base 16
        (hexadecimal).
        """
        return base_repr(int(number, 36), 16).lower().zfill(40)

    def properties(self):
        """
        Returns a list with the file properties, in the expected
        persisting (database) format
        """
        return {'wiki': self.wiki,
                'upload_name': self.upload_name,
                'file_type': self.type,
                'status': self.status,
                'sha1': self.sha1,
                'md5': self.md5,
                'size': self.size,
                'upload_timestamp': self.upload_timestamp,
                'archived_timestamp': self.archived_timestamp,
                'deleted_timestamp': self.deleted_timestamp,
                'swift_container': self.swift_container,
                'swift_name': self.swift_name}

    def __repr__(self):
        return (str(self.wiki or '') + ' ' + str(self.upload_name or '') +
                ' ' + str(self.sha1 or ''))
