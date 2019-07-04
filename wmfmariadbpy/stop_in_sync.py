#!/usr/bin/python3

from wmfmariadbpy.WMFReplication import WMFReplication
from wmfmariadbpy.WMFMariaDB import WMFMariaDB

import argparse
import sys


def handle_parameters():
    parser = argparse.ArgumentParser(description=('Stops replication on the given 2 sibling '
                                                  'database instances (instances replicating '
                                                  'directly from the same master) on the same coordinate'))
    parser.add_argument('instance1', help=('Instance #1 to be stopped, in hostname:port format'))
    parser.add_argument('instance2', help=('Instance #2 to be stopped, in hostname:port format'))
    parser.add_argument('--timeout', type=float, default=5.0,
                        help=('Timeout in seconds. A lower value will make operations faster, '
                              'but it is more likely to fail if there is more lag than that between instances. '
                              'Default: 5.0 seconds.'))
    options = parser.parse_args()
    return options


def main():
    # Preparatory steps
    options = handle_parameters()
    instance1 = WMFMariaDB(options.instance1)
    instance2 = WMFMariaDB(options.instance2)
    timeout = options.timeout
    instance1_replication = WMFReplication(instance1, timeout)

    result = instance1_replication.stop_in_sync_with_sibling(instance2)
    if result is None:
        print('[ERROR]: {} is not a sibling of {}, or they have too much lag'.format(
              instance1.name(), instance2.name()))
        sys.exit(-1)
    if not result['success']:
        print('[ERROR]: The stop operation failed: {}'.format(result['errmsg']))
        sys.exit(1)
    print('{} and {} stopped both at {}:{}'.format(
        instance1.name(), instance2.name(), result['log_file'], result['log_pos']))
    sys.exit(0)


if __name__ == "__main__":
    main()
