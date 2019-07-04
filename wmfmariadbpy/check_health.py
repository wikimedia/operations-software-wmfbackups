#!/usr/bin/env python3

from WMFMariaDB import WMFMariaDB

import argparse
import json
import math
import sys
import time
from datetime import datetime
import subprocess


def parse_args():
    """
    Performs the parsing of execution parameters, and returns the object
    containing them
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--host', '-h', help="""the hostname or dns to connect
                        to.""", default='localhost')
    parser.add_argument('--port', '-P', type=int, help='the port to connect',
                        default=3306)
    parser.add_argument('--verbose', '-v', action='store_true', dest='debug',
                        help='Enable debug mode for execution trace.')
    parser.add_argument('--slave-status', action='store_true', dest='slave_status',
                        help='Enable SHOW SLAVE STATUS execution (blocking).')
    parser.add_argument('--process', action='store_true', dest='process',
                        help='Check there is a mysqld process (only available for localhost).')
    parser.add_argument('--icinga', action='store_true', dest='icinga',
                        help='Output in icinga format rather than just the status.')
    parser.add_argument('--connect-timeout', type=float, default=1.0, dest='connect_timeout',
                        help='How much time to wait for mysql to connect.')
    parser.add_argument('--query-timeout', type=float, default=1.0, dest='query_timeout',
                        help='Max execution query limit.')
    parser.add_argument('--shard', default=None,
                        help='Only check this replication channel/heartbeat row.')
    parser.add_argument('--primary-dc', dest='primary_dc', default='eqiad',
                        help='Set primary datacenter (by default, eqiad).')
    parser.add_argument('--help', '-?', '-I', action='help',
                        help='show this help message and exit')
    return parser


def get_var(conn, name, scope='GLOBAL', type='VARIABLES'):
    if scope in ['GLOBAL', 'SESSION'] and type in ['VARIABLES', 'STATUS']:
        result = conn.execute("SHOW {} {} like '{}'".format(scope, type, name))
        if result["success"]:
            return result["rows"][0][1]
    return None


def get_replication_status(conn, connection_name=None):
    """
    Provides replication status information to the given host.
    If connection_name is given, and such a named replication channel exists
    (MariaDB only), it returns a dictionary with the specific connection
    information.
    If no connection_name is given, it will return an array of dictionaries,
    one per replication channel.
    None will be returned if no replication channels are found (or none are found
    with the given name).
    """
    if connection_name is None:
        result = conn.execute("SHOW ALL SLAVES STATUS")
    else:
        result = conn.execute("SHOW SLAVE '{}' STATUS".format(connection_name))
    if result["success"] and result["numrows"] > 0:
        status = list()
        for channel in result["rows"]:
            status.append(dict(zip(result["fields"], channel)))
        return status
    else:
        return None


def get_heartbeat_status(conn, shard=None, primary_dc='eqiad', db='heartbeat', table='heartbeat'):
    if primary_dc not in ['eqiad', 'codfw']:
        return None
    if shard is None:
        query = """
        SELECT shard, min(greatest(0, TIMESTAMPDIFF(MICROSECOND, ts, UTC_TIMESTAMP(6)) - 500000)) AS lag
        FROM {}.{}
        WHERE datacenter = '{}'
        GROUP BY shard
        """.format(db, table, primary_dc)
    else:
        query = """
        SELECT shard, min(greatest(0, TIMESTAMPDIFF(MICROSECOND, ts, UTC_TIMESTAMP(6)) - 500000)) AS lag
        FROM {}.{}
        WHERE datacenter = '{}'
        AND shard = '{}'
        """.format(db, table, primary_dc, shard)
    result = conn.execute(query)
    if result["success"] and result["numrows"] > 0:
        status = dict()
        for channel in result["rows"]:
            if channel[1] is not None:
                status[channel[0].decode('utf-8')] = int(channel[1])/1000000
        if len(status) == 0:
            return None
        else:
            return status
    else:
        return None


def get_processes(process_name):
    try:
        return list(map(int, subprocess.check_output(['/bin/pidof', process_name]).split()))
    except subprocess.CalledProcessError:
        return list()


def get_status(options):
    status = dict()

    if options.process and options.host != 'localhost':
        print("ERROR: Checking process is only allowed on localhost")
        sys.exit(-1)
    elif options.process:
        mysqld_processes = get_processes('mysqld')
        status['mysqld_processes'] = mysqld_processes

    time_before_connect = time.time()
    mysql = WMFMariaDB(host=options.host, port=options.port,
                       connect_timeout=options.connect_timeout)
    time_after_connect = time.time()

    wait_timeout = math.ceil(options.query_timeout)
    mysql.execute("SET SESSION innodb_lock_wait_timeout = {0}, SESSION lock_wait_timeout = {0}, SESSION wait_timeout = {0}".format(wait_timeout))

    if mysql.connection is None:
        status['connection'] = None
    else:
        status['connection'] = 'ok'
        version = get_var(mysql, 'version')
        read_only = get_var(mysql, 'read_only')
        uptime = get_var(mysql, 'Uptime', type='STATUS')
        ssl = get_var(mysql, 'Ssl_cipher', type='STATUS')
        ssl_expiration = get_var(mysql, 'Ssl_server_not_after', type='STATUS')
        threads_connected = get_var(mysql, r'Threads\_connected', type='STATUS')
        total_queries = get_var(mysql, 'Queries', type='STATUS')
        now = time.time()  # get the time here for more exact QPS calculations
        if options.slave_status:
            replication = get_replication_status(mysql)

        time_before_heartbeat = time.time()
        heartbeat = get_heartbeat_status(mysql,
                                         primary_dc=options.primary_dc,
                                         shard=options.shard)
        time_after_heartbeat = time.time()
        mysql.disconnect()

        if version is not None:
            status['version'] = version
        if read_only is not None:
            status['read_only'] = read_only == 'ON'

        if uptime is not None:
            status['uptime'] = int(uptime)

        if ssl is None or ssl == '':
            status['ssl'] = False
        else:
            status['ssl'] = True
            if ssl_expiration is not None and ssl_expiration != '':
                try:
                    # We assume we will be always using GMT
                    status['ssl_expiration'] = time.mktime(datetime.strptime(ssl_expiration, '%b %d %H:%M:%S %Y %Z').timetuple())
                except ValueError:
                    status['ssl_expiration'] = None

        if total_queries is not None:
            status['total_queries'] = int(total_queries)

        if threads_connected is not None:
            status['datetime'] = now
            status['threads_connected'] = int(threads_connected)

        if heartbeat is not None and len(heartbeat) > 0:
            status['heartbeat'] = heartbeat
            status['query_latency'] = time_after_heartbeat - time_before_heartbeat

        if options.slave_status and replication is not None and len(replication) > 0:
            status['replication'] = dict()
            for channel in replication:
                replication_status = dict()
                replication_status['Slave_IO_Running'] = channel['Slave_IO_Running']
                replication_status['Slave_SQL_Running'] = channel['Slave_SQL_Running']
                replication_status['Seconds_Behind_Master'] = channel['Seconds_Behind_Master']
                replication_status['Last_IO_Error'] = channel['Last_IO_Error'] if channel['Last_IO_Error'] != '' else None
                # FIXME may contain private data, needs filtering:
                replication_status['Last_SQL_Error'] = channel['Last_SQL_Error'] if channel['Last_SQL_Error'] != '' else None
                status['replication'][channel['Connection_name']] = replication_status

        status['connection_latency'] = time_after_connect - time_before_connect

    return status


def check_health(status, options):
    return None


def main():
    parser = parse_args()
    options = parser.parse_args()

    status = get_status(options)
    if options.icinga:
        health_check = check_health(status, options)
        print(json.dumps(health_check))
    else:
        print(json.dumps(status))


if __name__ == "__main__":
    main()
