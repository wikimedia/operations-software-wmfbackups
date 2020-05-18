#!/usr/bin/python3

import argparse
import sys
from transferpy.Transferer import Transferer


def option_parse():
    """
    Parses the input parameters and returns them as a list.

    :return: sender host, sender path, receiver hosts, receiver paths, other options
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--type", choices=['file', 'xtrabackup', 'decompress'],
                        dest='transfer_type', default='file')
    parser.add_argument("source")
    parser.add_argument("target", nargs='+')

    compress_group = parser.add_mutually_exclusive_group()
    compress_group.add_argument('--compress', action='store_true', dest='compress')
    compress_group.add_argument('--no-compress', action='store_false', dest='compress')
    parser.set_defaults(compress=True)

    encrypt_group = parser.add_mutually_exclusive_group()
    encrypt_group.add_argument('--encrypt', action='store_true', dest='encrypt')
    encrypt_group.add_argument('--no-encrypt', action='store_false', dest='encrypt')
    parser.set_defaults(encrypt=True)

    checksum_group = parser.add_mutually_exclusive_group()
    checksum_group.add_argument('--checksum', action='store_true', dest='checksum')
    checksum_group.add_argument('--no-checksum', action='store_false', dest='checksum')
    parser.set_defaults(checksum=True)

    parser.add_argument('--stop-slave', action='store_true', dest='stop_slave')

    parser.add_argument('--verbose', action='store_true')

    options = parser.parse_args()
    source_host = options.source.split(':', 1)[0]
    source_path = options.source.split(':', 1)[1]
    target_hosts = []
    target_paths = []
    for target in options.target:
        target_hosts.append(target.split(':', 1)[0])
        target_paths.append(target.split(':', 1)[1])
    other_options = {
        'port': options.port,
        'type': options.transfer_type,
        'compress': True if options.transfer_type == 'decompress' else options.compress,
        'encrypt': options.encrypt,
        'checksum': False if not options.transfer_type == 'file' else options.checksum,
        'stop_slave': False if not options.transfer_type == 'xtrabackup' else options.stop_slave,
        'verbose': options.verbose
    }
    return source_host, source_path, target_hosts, target_paths, other_options


def main():
    """
    Main of transfer framework.

    :return: system exit
    """
    (source_host, source_path, target_hosts, target_paths, other_options) = option_parse()
    t = Transferer(source_host, source_path, target_hosts, target_paths, other_options)
    result = t.run()
    sys.exit(max(result))


if __name__ == "__main__":
    main()
