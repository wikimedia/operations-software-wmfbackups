#!/usr/bin/python3

import WMFReplication
import WMFMariaDB

import argparse
import sys
import time


def handle_parameters():
    parser = argparse.ArgumentParser(description='Performs a master to direct replica switchover in the WMF environment, automating the most error-prone steps. Example usage: switchover.py db1052 db1067')
    parser.add_argument('master', help='Original master host, in hostname:port format, to be switched from')
    parser.add_argument('slave', help='Direct replica host, in hostname:port format, to be switched to, and will become the new master')
    parser.add_argument('--timeout', type=float, default=5.0, help='Timeout in seconds, to wait for several operations before returning an error (STOP SLAVE, etc. It will also mark the maximum amount of lag we can tolerate.')
    parser.add_argument('--skip-slave-move', type=bool, default=False, help='When set, it does not migrate current master replicas to the new host')
    parser.add_argument('--only-slave-move', type=bool, default=False, help='When set, it only migrates current master replicas to the new hosts, but does not perform the rest of the operations (read only, replication inversion, etc.)')

    options = parser.parse_args()
    return options


def do_preflight_checks(master_replication, slave_replication, timeout):
    master = master_replication.connection
    slave = slave_replication.connection
    print('Starting preflight checks...')
    master_result = master.execute('SELECT @@GLOBAL.read_only')
    slave_result = slave.execute('SELECT @@GLOBAL.read_only')
    if not master_result['success'] or not slave_result['success'] or not master_result['rows'][0][0] == 0 or not slave_result['rows'][0][0] == 1:
        print('[ERROR]: Initial read_only status check failed: original master read_only: {} / original slave read_only: {}'.format(master_result['rows'][0][0], slave_result['rows'][0][0]))
        sys.exit(-1)
    print('* Original read only values are as expected (master: read_only=0, slave: read_only=1)')

    if not slave_replication.is_direct_replica_of(master):
        print('[ERROR]: {} is not a direct replica of {}'.format(slave.name(), master.name()))
        sys.exit(-1)
    print('* The host to fail over is a direct replica of the master')

    slave_status = slave_replication.slave_status()
    if slave_status['slave_sql_running'] != 'Yes' or slave_status['slave_io_running'] != 'Yes':
        print('[ERROR]: The replica is not currently running')
        sys.exit(-1)
    print('* Replication is up and running between the 2 hosts')

    lag = slave_replication.lag()
    if lag is None:
        print('[ERROR]: It was impossible to measure the lag between the master and the slave')
        sys.exit(-1)
    elif lag > timeout:
        print('[ERROR]: The replica is too lagged: {} seconds, please allow it to catch up first'.format(str(lag)))
        sys.exit(-1)
    print('* The replication lag is acceptable: {} (lower than the configured or default timeout)'.format(str(lag)))

    # TODO: Allow the master to replicate, just stop it and recover it on the new master
    if not master_replication.slave_status() is None:
        print('[ERROR]: The master is replicating from somewhere, aborting')
        sys.exit(-1)
    print('* The master is not a replica of any other host')


def set_master_in_read_only(master_replication):
    print('Setting up original master as read-only')
    result = master_replication.connection.execute('SET GLOBAL read_only = 1')
    if not result['success']:
        print('[ERROR]: Could not set the master as read only')
        sys.exit(-1)


def wait_for_slave_to_catch_up(master_replication, slave_replication, timeout):
    timeout_start = time.time()
    while not slave_replication.caught_up_to_master(master_replication.connection):
        time.sleep(0.1)
        if time.time() > (timeout_start + timeout):
            break
    if not slave_replication.caught_up_to_master(master_replication.connection):
        print('[ERROR]: We could not wait to catch up replication, trying now to revert read only on the master back to read-write')
        result = master_replication.connection.execute('SET GLOBAL read_only = 0')
        if not result['success']:
            print('[ERROR]: We could not revert the master back to read_only, server may be down or other issues')
        else:
            print('Switchover failed, but we put back the master in read/write again')
        print('Try increasing the timeout parameter, or debuging the current status')
        sys.exit(-1)

    print('Slave caught up to the master after waiting {} seconds'.format(str(time.time() - timeout_start)))


def stop_slave(slave_replication):
    print("Stopping original master->slave replication")
    result = slave_replication.stop_slave()
    if not result['success']:
        print('Could not stop slave: {}'.format(result['errmsg']))
        sys.exit(-1)


def set_replica_in_read_write(master_replication, slave_replication):
    slave = slave_replication.connection
    master = master_replication.connection
    print('Setting up replica as read-write')
    result = slave.execute('SET GLOBAL read_only = 0')
    if not result['success']:
        print('[ERROR]: Could not set the slave as read write, trying to revert read only on the master back to read-write')
        result = master.execute('SET GLOBAL read_only = 0')
        if not result['success']:
            print('We could not revert the master back to read_only, server may be down or other issues')
        else:
            print('Switchover failed, but we put back the master in read/write again')
        sys.exit(-1)

    master_result = master.execute('SELECT @@GLOBAL.read_only')
    slave_result = slave.execute('SELECT @@GLOBAL.read_only')
    if not master_result['success'] or \
       not slave_result['success'] or \
       not master_result['numrows'] == 1 or \
       not master_result['rows'][0][0] == 1 or \
       not slave_result['numrows'] == 1 or \
       not slave_result['rows'][0][0] == 0:
        print('[ERROR]: Post check failed, current status: original master read_only: {} / original slave read_only: {}'.format(master_result['rows'][0][0], slave_result['rows'][0][0]))
        sys.exit(-1)
    print('All commands where successful, current status: original master read_only: {} / original slave read_only: {}'.format(master_result['rows'][0][0], slave_result['rows'][0][0]))


def invert_replication_direction(master_replication, slave_replication, master_status_on_switch):
    slave = slave_replication.connection
    print('Trying to invert replication direction')
    result = master_replication.setup(master_host=slave.host, master_port=slave.port, master_log_file=master_status_on_switch['file'], master_log_pos=master_status_on_switch['position'])
    if not result['success']:
        print('[ERROR]: We could not repoint the original master to the new one')
        sys.exit(-1)
    result = master_replication.start_slave()
    if not result['success']:
        print('[ERROR]: We could not start replicating towards the original master')
        sys.exit(-1)
    result = slave_replication.reset_slave()
    if not result['success']:
        print('[ERROR]: We could not reset replication on the new master')
        sys.exit(-1)


def verify_status_after_switch(master_replication, slave_replication, timeout):
    master = master_replication.connection
    slave = slave_replication.connection
    print('Verifying everything went as expected...')
    master_result = master.execute('SELECT @@GLOBAL.read_only')
    slave_result = slave.execute('SELECT @@GLOBAL.read_only')
    if not master_result['success'] or not slave_result['success'] or not master_result['rows'][0][0] == 1 or not slave_result['rows'][0][0] == 0:
        print('[ERROR]: Read_only status verification failed: original master read_only: {} / original slave read_only: {}'.format(master_result['rows'][0][0], slave_result['rows'][0][0]))
        sys.exit(-1)

    if not master_replication.is_direct_replica_of(slave):
        print('[ERROR]: {} is not a direct replica of {}'.format(master.name(), slave.name()))
        sys.exit(-1)

    master_status = master_replication.slave_status()
    if master_status is None or not master_status['success'] or master_status['slave_sql_running'] != 'Yes' or master_status['slave_io_running'] != 'Yes':
        print('[ERROR]: The original master is not replicating correctly from the switched instance')
        sys.exit(-1)


def move_replicas_to_new_master(master_replication, slave_replication, timeout):
    """
    Migrates all old master direct slaves to the new master, maintaining the consistency.
    """
    for replica in master_replication.slaves():
        print('Testing if to migrate {}...'.format(replica.name()))
        if replica.is_same_instance_as(slave_replication.connection):
            print('Nope')
            continue  # do not move the target replica to itself
        replication = WMFReplication.WMFReplication(replica, timeout)
        result = replication.move(new_master=slave_replication.connection, start_if_stopped=True)
        if not result['success']:
            print('[ERROR]: {} failed to be migrated from master to replica'.format(replica.name()))
            sys.exit(-1)
        print('Migrated {} successfully from master to replica'.format(replica.name()))


def main():
    options = handle_parameters()
    master = WMFMariaDB.WMFMariaDB(options.master)
    slave = WMFMariaDB.WMFMariaDB(options.slave)
    timeout = options.timeout
    slave_replication = WMFReplication.WMFReplication(slave, timeout)
    master_replication = WMFReplication.WMFReplication(master, timeout)

    do_preflight_checks(master_replication, slave_replication, timeout)

    if not options.skip_slave_move:
        move_replicas_to_new_master(master_replication, slave_replication, timeout)

    if options.only_slave_move:
        print('SUCCESS: All slaves moved correctly, but not continuing further because --only-slave-move')
        sys.exit(0)

    set_master_in_read_only(master_replication)

    wait_for_slave_to_catch_up(master_replication, slave_replication, timeout)

    slave_status_on_switch = slave_replication.slave_status()
    master_status_on_switch = slave_replication.master_status()
    print('Servers sync at master: {} slave: {}'.format(slave_status_on_switch['relay_master_log_file'] +
                                                        ':' +
                                                        str(slave_status_on_switch['exec_master_log_pos']),
                                                        master_status_on_switch['file'] +
                                                        ':' +
                                                        str(master_status_on_switch['position'])))
    stop_slave(slave_replication)

    set_replica_in_read_write(master_replication, slave_replication)

    invert_replication_direction(master_replication, slave_replication, master_status_on_switch)

    verify_status_after_switch(master_replication, slave_replication, timeout)

    print('SUCCESS: Master switch completed successfully')

    sys.exit(0)


if __name__ == "__main__":
    main()
