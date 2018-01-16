#!/usr/bin/env python3
from WMFMariaDB import WMFMariaDB

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
    parser.add_argument('host1', help='First instance, with format host1[:port]')
    parser.add_argument('host2', help='Second instance, with format host2[:port]')
    parser.add_argument('database', help='Database to connect on both instances')
    parser.add_argument('table', help='Table to compare on both instances')
    parser.add_argument('column', help='Numeric id to loop on, normally an autoincrement field')
    parser.add_argument('--step', type=int, default=1000, help='How many rows to compare each time. The larger number of rows, the faster the full comparison will be, but also more aggressive. Also, there is an upper limit due to group_concat_max_len, depending on the size of the rows. Default: 1000 rows')
    parser.add_argument('--group_concat_max_len', type=int, default=10000000, help='Sets group_concat_max_len. Normally needed when increasing the "step" or the rows are very large. Default: 10MB')
    parser.add_argument('--from-value', type=int, dest='from_value', help='Start from given column value. Useful if only part of the table has to be scanned. Defaults to min column value for the table on the first given instance, at start.')
    parser.add_argument('--to-value', type=int, dest='to_value', help='Start from given column value. Useful if only part of the table has to be scanned. Defaults to max column value for the table on the first given instance, at start.')
    parser.add_argument('--order-by', dest='order_by', help='Ordering when doing the comparions. Useful when the column is not an unique field, and results could be returned out of order. By default, we order by column, which could give false positives if the column is not unique.')
    parser.add_argument('--print-every', type=int, dest='print_every', default=100, help='How often output should print progress report. Default: 100 queries/chunks.')
    parser.add_argument('--verbose', dest='verbose', action='store_true', help="Enables verbose logging, where all SQL commands sent to the server are sent to the standard output. Defaults to disabled.")
    return parser.parse_args()


def connect_in_parallel(host1, host2, database):
    pool = ThreadPool(processes=2)
    async_result = dict()
    conn = list()
    for host in (host1, host2):
        if ':' in host:
            # we do not support ipv6 yet
            host, port = host.split(':')
            port = int(port)
        else:
            port = 3306
        async_result[host] = pool.apply_async(WMFMariaDB, (host, port, database))

    for host in (host1, host2):
        mysql = async_result[host].get()
        if mysql.connection is None:
            sys.stderr.write("Could not connect to {}:{}\n".format(host))
            sys.exit(-1)
        conn.append(mysql)
    pool.close()
    pool.join()
    return tuple(conn)

def execute_in_parallel(conn1, conn2, query, verbose):
    if verbose:
        print(query)
    pool = ThreadPool(processes=2)
    async_result1 = pool.apply_async(conn1.execute, (query, False))
    async_result2 = pool.apply_async(conn2.execute, (query, False))

    result = (async_result1.get(), async_result2.get())
    pool.close()
    pool.join()
    return result


def main():
    options = parse_args()

    if options.host1 == options.host2:
        print('ERROR: You are trying to compare {} to itself, exiting.'.format(options.host1))
        sys.exit(-1)
    (conn1, conn2) = connect_in_parallel(options.host1, options.host2, options.database)

    # calulate ranges to perform comparison and check if tables are empty
    command = 'SELECT min({0}), max({0}) FROM {1}'.format(options.column, options.table)
    (result1, result2) = execute_in_parallel(conn1, conn2, command, options.verbose)

    for result in (result1, result2):
        if not result['success']:
            print('ERROR: Minimum/maximum id could not be retrieved, exiting.')
            sys.exit(-1)
    min_id1 = result1['rows'][0][0]
    max_id1 = result1['rows'][0][1]
    min_id2 = result2['rows'][0][0]
    max_id2 = result2['rows'][0][1]

    if min_id1 is None and min_id2 is None and max_id1 is None and max_id2 is None:
        print('No rows found on both tables.')
        sys.exit(0)
    elif min_id1 is None and max_id1 is None:
        print('{} has no rows, but {} has'.format(options.host1, options.host2))
        sys.exit(1)
    elif min_id2 is None and max_id2 is None:
        print('{} has rows, but {} has not'.format(options.host1, options.host2))
        sys.exit(1)

    if options.from_value is None:
        min_id = min(min_id1, min_id2)
    else:
        min_id = options.from_value

    if options.to_value is None:
        max_id = max(max_id1, max_id2)
    else:
        max_id = options.to_value

    if min_id > max_id:
        print('starting id is greater than ending id')
        sys.exit(-1)

    print('Starting comparison between id {} and {}'.format(min_id, max_id))
    # setup query for comparison
    ## TODO: make column(s) iterator
    command = 'DESCRIBE {}'.format(options.table)
    (describe_result1, describe_result2) = execute_in_parallel(conn1, conn2, command, options.verbose)
    if not describe_result1['success'] or not describe_result2['success']:
        print('ERROR: Could not describe the table, exiting.')
        sys.exit(-1)

    all_columns = ','.join({"IFNULL(" + x[0] + ", '\\0'),'|'" for x in describe_result1['rows']})
    if options.order_by is None or options.order_by == '':
        order_by = options.column
    else:
        order_by = options.order_by

    # increase group_concat_max_len
    command = 'SET SESSION group_concat_max_len = {}'.format(options.group_concat_max_len)
    execute_in_parallel(conn1, conn2, command, options.verbose)

    # main comparison loop
    differences = 0
    iterations = 0
    try:
        for lower_limit in range(min_id, max_id, options.step):
            upper_limit = lower_limit + options.step - 1
            if upper_limit > max_id:
                upper_limit = max_id

            iterations += 1
            if iterations % options.print_every == 0:
                print('{}: row id {}/{}, {} chunk(s) found different'.format(datetime.now().isoformat(),
                                                                                    lower_limit,
                                                                                    max_id,
                                                                                    differences))

            command = 'SELECT crc32(GROUP_CONCAT({4})) FROM {0} WHERE {1} BETWEEN {2} AND {3} ORDER BY {5}'.format(options.table, options.column, lower_limit, upper_limit, all_columns, order_by)
            (result1, result2) = execute_in_parallel(conn1, conn2, command, options.verbose)
            if not(result1['success'] and result2['success'] and result1['rows'][0][0] ==   result2['rows'][0][0]):
                # chunk detected as different
                print('DIFFERENCE: WHERE {} BETWEEN {} AND {}'.format(options.column, lower_limit, upper_limit))
                differences = differences + 1
    except KeyboardInterrupt:
        print('Ctrl-c pressed ...')
        sys.exit(1)

    conn1.disconnect()
    conn2.disconnect()

    if differences == 0:
        print('Execution ended, no differences found.')
        sys.exit(0)
    else:
        print("Execution ended, a total of {} chunk(s) are different.".format(differences))
        sys.exit(1)


if __name__ == "__main__":
    main()
