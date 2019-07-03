#!/usr/bin/python3

import WMFReplication
import WMFMariaDB

import argparse
import sys


def handle_parameters():
    parser = argparse.ArgumentParser(description=('Moves the first instance under the second one, '
                                                  'replication-wise. It only works for now for '
                                                  'very simple cases (move under a sibling, move '
                                                  'from child to sibling, etc.)'))
    parser.add_argument('instance', help=('Instance which replication is to be moved, format: hostname:port'))
    parser.add_argument('new_master', help=('New master of the instance, format: hostname:port'))
    parser.add_argument('--timeout', type=float, default=5.0,
                        help=('Timeout in seconds. A lower value will make operations faster, '
                              'but it is more likely to fail if there is more lag than that between instances. '
                              'Default: 5.0 seconds.'))
    parser.add_argument('--start_if_stopped', action='store_true')
    options = parser.parse_args()
    return options


def main():
    # Preparatory steps
    options = handle_parameters()
    instance = WMFMariaDB.WMFMariaDB(options.instance)
    new_master = WMFMariaDB.WMFMariaDB(options.new_master)
    timeout = options.timeout
    start_if_stopped = options.start_if_stopped
    instance_replication = WMFReplication.WMFReplication(instance, timeout)
    new_master_replication = WMFReplication.WMFReplication(new_master, timeout)

    # disable gtid
    instance_gtid = instance_replication.gtid_mode()
    if instance_gtid is not None and instance_gtid.lower() != 'no':
        result = instance_replication.set_gtid_mode('no')
        if not result:
            print('[ERROR] GTID could not be disabled on {}'.format(instance.name()))
            sys.exit(1)
    new_master_gtid = new_master_replication.gtid_mode()
    if new_master_gtid is not None and new_master_gtid.lower() != 'no':
        result = new_master_replication.set_gtid_mode('no')
        if not result:
            print('[ERROR] GTID could not be disabled on {}'.format(new_master.name()))
            sys.exit(1)

    # move
    result = instance_replication.move(new_master, start_if_stopped=start_if_stopped)
    if not result['success']:
        print('[ERROR]: The move operation failed: {}'.format(result['errmsg']))
        sys.exit(1)

    # reenable gtid
    if instance_gtid is not None and instance_gtid.lower() != 'no':
        result = instance_replication.set_gtid_mode(instance_gtid)
        if not result:
            print('[WARNING] GTID could not be reenabled on {}'.format(instance.name()))
    if new_master_gtid is not None and new_master_gtid.lower() != 'no':
        result = new_master_replication.set_gtid_mode(new_master_gtid)
        if not result:
            print('[WARNING] GTID could not be reenabled on {}'.format(new_master.name()))
    sys.exit(0)


if __name__ == "__main__":
    main()
