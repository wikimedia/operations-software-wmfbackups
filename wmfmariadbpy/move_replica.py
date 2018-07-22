#!/usr/bin/python3

import WMFReplication
import WMFMariaDB

import argparse
import sys


def handle_parameters():
    """
    Handles command line parameters:
    * instance: MariaDB instance to move
    * new_master: separate instance to set as the new master for instance
    * --timeout: allowed timeout for replication changes operations, by default, 5.0 seconds (decimal)
    * --start_if_stopped: It restarts replication even if new_master or slave was stopped
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('instance')
    parser.add_argument('new_master')
    parser.add_argument('--timeout', type=float, default=5.0)
    parser.add_argument('--start_if_stopped', type=bool, default=False)
    options = parser.parse_args()
    return WMFMariaDB.WMFMariaDB(host=options.instance), WMFMariaDB.WMFMariaDB(options.new_master), options


def main():
    """
    Reads parameters from command line an performs a topology change, making new_master the new master
    of instance. Communicates on standard output and exit output if it was successful.
    """
    (instance, new_master, options) = handle_parameters()
    instance_replication = WMFReplication.WMFReplication(instance, options.timeout)

    result = instance_replication.move(new_master, options.start_if_stopped)

    if result is not None and result['success']:
        print('SUCCESS: Topology change completed successfully')
        sys.exit(0)
    else:
        if result is not None:
            print(result['errmsg'])
        print('[ERROR]: Topology change was not succesful')
        sys.exit(-1)


if __name__ == "__main__":
    main()
