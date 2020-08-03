#!/usr/bin/python3
from wmfmariadbpy.WMFMariaDB import WMFMariaDB

import argparse
from datetime import datetime
import sys
from multiprocessing.pool import ThreadPool


def parse_args():
    """
    Performs the parsing of execution parameters, and returns the object
    containing them
    """
    parser = argparse.ArgumentParser(description='Compares the table contents between 2 WMF MySQL/MariaDB servers.')
    parser.add_argument('database', help='Database to connect on both instances')
    parser.add_argument('table', help='Table to compare on both instances')
    parser.add_argument('column', help='Numeric id to loop on, normally an autoincrement field')
    parser.add_argument('--step', type=int, default=10000, help='How many rows to compare each time. The larger number of rows, the faster the full comparison will be, but also more aggressive. Also, there is an upper limit due to group_concat_max_len, depending on the size of the rows. Default: 10000 rows')
    parser.add_argument('--group_concat_max_len', type=int, default=100000000, help='Sets group_concat_max_len. Normally needed when increasing the "step" or the rows are very large. Default: 100MB')
    parser.add_argument('--from-value', type=int, dest='from_value', help='Start from given column value. Useful if only part of the table has to be scanned. Defaults to min column value for the table on the first given instance, at start.')
    parser.add_argument('--to-value', type=int, dest='to_value', help='Start from given column value. Useful if only part of the table has to be scanned. Defaults to max column value for the table on the first given instance, at start.')
    parser.add_argument('--order-by', dest='order_by', help='Ordering when doing the comparions. Useful when the column is not an unique field, and results could be returned out of order. By default, we order by column, which could give false positives if the column is not unique.')
    parser.add_argument('--print-every', type=int, dest='print_every', default=100, help='How often output should print progress report. Default: 100 queries/chunks. Set to 0 to disable progress reporting.')
    parser.add_argument('--threads', type=int, default=1, help='Parallelize requests on the given number of connections per server. By default, only 1 connection is used per server.')
    parser.add_argument('--verbose', dest='verbose', action='store_true', help="Enables verbose logging, where all SQL commands sent to the server are sent to the standard output. Defaults to disabled.")
    parser.add_argument('hosts', nargs='+', help='2 or more database instances, expresed with format host[:port]')

    return parser.parse_args()


def get_host_tuple(host):
    if ':' in host:
        # we do not support ipv6 yet
        host, port = host.split(':')
        port = int(port)
    else:
        port = 3306
    return (host, port)


def connect_in_parallel(hosts, database, threads):
    pool = ThreadPool(processes=len(hosts))
    async_result = dict()
    conn = list()
    for host_string in hosts:
        (host, port) = get_host_tuple(host_string)
        async_result[host + '_' + str(port)] = pool.apply_async(WMFMariaDB, (host, port, database))

    for host_string in hosts:
        (host, port) = get_host_tuple(host_string)
        mysql = async_result[host + '_' + str(port)].get()
        if mysql.connection is None:
            sys.stderr.write("Could not connect to {}\n".format(host))
            sys.exit(-1)
        conn.append(mysql)
    pool.close()
    pool.join()
    return tuple(conn)


def execute_in_parallel(connections, query, verbose):
    pool = ThreadPool(processes=len(connections))
    async_result = dict()
    result = list()
    if verbose:
        print(query)
    for conn in connections:
        async_result[conn.host + '_' + str(conn.port)] = pool.apply_async(conn.execute, (query, False))

    for conn in connections:
        result.append(async_result[conn.host + '_' + str(conn.port)].get())
    pool.close()
    pool.join()
    return result


def main():
    options = parse_args()

    connections = connect_in_parallel(options.hosts, options.database, options.threads)

    # calulate ranges to perform comparison and check if tables are empty
    command = 'SELECT min({0}), max({0}) FROM {1}'.format(options.column, options.table)
    results = execute_in_parallel(connections, command, options.verbose)

    empty_tables = list()
    min_id = 2147483647
    max_id = -2147483648
    for result in results:
        if not result['success']:
            print('ERROR: Minimum/maximum id could not be retrieved for {}, exiting.'.format(result['host'] + ':' + str(result['port'])))
            sys.exit(-1)

        if result['rows'][0][0] is None or result['rows'][0][1] is None:
            empty_tables.append(result['host'] + ':' + str(result['port']))
        else:
            if result['rows'][0][0] < min_id:
                min_id = result['rows'][0][0]
            if result['rows'][0][1] > max_id:
                max_id = result['rows'][0][1]

    if len(empty_tables) >= len(results):
        if options.print_every != 0:
            print('All tables are empty')
        sys.exit(0)
    elif len(empty_tables) > 0 and len(empty_tables) < len(results):
        print('Some tables have rows, but the following are empty: {}'.format(','.join(empty_tables)))
        sys.exit(1)

    # override obtained min_id and max_id if they have been set as parameters
    if options.from_value is not None and options.from_value > min_id:
        min_id = options.from_value

    if options.to_value is not None and options.to_value < max_id:
        max_id = options.to_value

    if min_id > max_id:
        print('starting id is greater than ending id')
        sys.exit(-1)

    if options.print_every != 0:
        print('Starting comparison between id {} and {}'.format(min_id, max_id))
    # setup query for comparison
    # TODO: make column(s) iterator
    command = 'DESCRIBE {}'.format(options.table)
    describe_results = execute_in_parallel(connections, command, options.verbose)
    for describe_result in describe_results:
        if not result['success']:
            print('ERROR: Could not describe the table, exiting.')
            sys.exit(-1)

    all_columns = ','.join({"IFNULL(" + x[0] + ", '\\0'),'|'" for x in describe_results[0]['rows']})
    if options.order_by is None or options.order_by == '':
        order_by = options.column
    else:
        order_by = options.order_by

    # increase group_concat_max_len
    command = 'SET SESSION group_concat_max_len = {}'.format(options.group_concat_max_len)
    execute_in_parallel(connections, command, options.verbose)

    # main comparison loop
    differences = 0
    iterations = 0
    start_time = datetime.now()
    try:
        for lower_limit in range(min_id, max_id + 1, options.step):
            upper_limit = lower_limit + options.step - 1
            if upper_limit > max_id:
                upper_limit = max_id

            if options.print_every != 0:
                iterations += 1
                if iterations % options.print_every == 0:
                    speed = (lower_limit - min_id) / (datetime.now() - start_time).total_seconds()
                    eta = int((max_id - lower_limit) / speed)
                    print('{}: row id {}/{}, ETA: {:02}m{:02}s, {} chunk(s) found different'
                          .format(datetime.now().isoformat(), lower_limit, max_id, eta//60, eta % 60, differences))

            command = 'SELECT crc32(GROUP_CONCAT({4})) FROM {0} WHERE {1} BETWEEN {2} AND {3} ORDER BY {5}'.format(options.table, options.column, lower_limit, upper_limit, all_columns, order_by)
            results = execute_in_parallel(connections, command, options.verbose)
            # only count each chunck once
            difference_detected = False
            for i in range(1, len(results)):
                if not(results[0]['success'] and results[i]['success'] and results[0]['rows'][0][0] == results[i]['rows'][0][0]):
                    # chunk detected as different
                    print('DIFFERENCE on {}: WHERE {} BETWEEN {} AND {}'.format(connections[i].host + ':' + str(connections[i].port), options.column, lower_limit, upper_limit))
                    difference_detected = True
            if difference_detected:
                differences = differences + 1
    except KeyboardInterrupt:
        print('Ctrl-c pressed ...')
        sys.exit(1)

    for connection in connections:
        connection.disconnect()

    if differences == 0:
        if options.print_every != 0:
            print('Execution ended, no differences found.')
        sys.exit(0)
    else:
        if options.print_every != 0:
            print("Execution ended, a total of {} chunk(s) are different.".format(differences))
        sys.exit(1)


if __name__ == "__main__":
    main()
