# WMFReplication.py
import WMFMariaDB

import configparser
import ipaddress
import socket
import time


class WMFReplication:
    """
    Class to control replication at WMF MariaDB/MySQL Cluster
    """
    connection = None
    timeout = 5.0  # how much to wait after start slave to determine if it was successful or not

    def __init__(self, connection, timeout=5.0):
        """
        The constructore requires an open connection in the form of a WMFMariaDB object
        """
        self.connection = connection
        self.timeout = timeout

    def slave_status(self):
        """
        Returns a dictionary with the slave status. If the server is not configured as a slave, it will return None.
        """
        result = self.connection.execute('SHOW SLAVE STATUS', timeout=self.timeout)
        if not result['success']:
            return {'success': False, 'errno': result['errno'], 'errmsg': result['errmsg']}
        if result['numrows'] == 0:
            return None
        status = dict(zip([key.lower() for key in result['fields']], result['rows'][0]))
        status['success'] = True
        return status

    def master_status(self):
        """
        Returns a dictionary with the master status. If the server is not configured as a master (binary log is
        disabled), it will return None.
        """
        result = self.connection.execute('SHOW MASTER STATUS')
        if not result['success']:
            return {'success': False, 'errno': result['errno'], 'errmsg': result['errmsg']}
        if result['numrows'] == 0:
            return None
        status = dict(zip([key.lower() for key in result['fields']], result['rows'][0]))
        status['success'] = True
        return status

    def setup(self, master_host=None, master_port=3306, master_log_file=None, master_log_pos=0, master_ssl=1):
        """
        Sets up replication configuration for a server for the first time (but does not start it).
        If replication is already configured (even if the replica is stopped), it fails. Stop and reset replication
        before trying to run it.
        """
        config = configparser.ConfigParser(interpolation=None)
        config.read('/root/.my.cnf')
        master_user = config['clientreplication']['user']
        master_password = config['clientreplication']['password']
        slave_status = self.slave_status()
        if slave_status is not None and slave_status['success'] and 'slave_sql_running' in slave_status:
            return {'success': False, 'errno': -1, 'errmsg': 'Replica is setup, reset it before setting it up again.'}

        query = """CHANGE MASTER TO
                   MASTER_HOST = '{}',
                   MASTER_PORT = {},
                   MASTER_USER = '{}',
                   MASTER_PASSWORD = '{}',
                   MASTER_LOG_FILE = '{}',
                   MASTER_LOG_POS = {},
                   MASTER_SSL = {}
                 """.format(master_host, str(int(master_port)), master_user, master_password, master_log_file, str(int(master_log_pos)), str(int(master_ssl)))
        result = self.connection.execute(query)
        return result

    def is_direct_replica_of(self, master):
        """
        Checks if the current instance is a direct replica of the given master. Only returns true
        if it can be confirmed. If it gives an error or it not a direct replica, it will return false
        """
        # master_replication = WMFReplication(master)
        # master_status = master_replication.master_status()
        query = 'SELECT @@GLOBAL.hostname AS hostname, @@GLOBAL.port AS port'
        result = master.execute(query)
        if not result['success'] or result['numrows'] == 0:
            return False
        master_host = result['rows'][0][0]
        master_port = result['rows'][0][1]
        slave_status = self.slave_status()
        # should the check be based on master_status instead?
        return slave_status is not None and slave_status['success'] and (slave_status['master_host'] == master.host or slave_status['master_host'] == master_host) and (slave_status['master_port'] == master.port or slave_status['master_port'] == master_port)

    def reset_slave(self):
        """
        Resets all replication information for the instance. It only works if both the io thread and the sql thread
        are currently stopped- run stop slave otherwise beforehand.
        """
        slave_status = self.slave_status()
        if not slave_status['slave_sql_running'] == 'No' or not slave_status['slave_io_running'] == 'No':
            return {'success': False, 'errno': -1, 'errmsg': 'Replica is running, stop slave before resetting it'}
        return self.connection.execute('RESET SLAVE ALL')

    def lag(self):
        """
        Returns the current lag of the replication, as provided by SHOW SLAVE STATUS.
        Use heartbeat functions if a more accurate and less locking way is needed (e.g. for
        frequent updates). Returns None on error.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            return None
        else:
            return slave_status['seconds_behind_master']

    def start_slave(self, thread=None):
        """
        Starts the replication thread given (allowed values are 'sql', 'io', or None- the default, which will try
        to start both threads). If all the given threads are already running, it returns with an error.
        For convenience, if the action is succesful, it will return a copy of the replication status.
        """
        slave_status = self.slave_status()
        if slave_status is None:
            return {'success': False, 'errno': -1, 'errmsg': 'The server is not configured as a slave'}

        if thread is None:
            if slave_status['slave_sql_running'] != 'No' and slave_status['slave_io_running'] != 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'Both IO and SQL threads are already running or trying to connect'}
            slave_thread = ''
        elif thread.lower() == 'sql':
            if slave_status['slave_sql_running'] != 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'SQL thread is already running'}
            slave_thread = 'SQL_THREAD'
        elif thread.lower() == 'io':
            if slave_status['slave_io_running'] != 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'IO thread is already running or trying to connect'}
            slave_thread = 'IO_THREAD'
        else:
            return {'success': False,  'errno': -1, 'errmsg': 'Slave Thread can only be SQL, IO or None (default)'}

        query = 'START SLAVE {}'.format(slave_thread).rstrip()
        result = self.connection.execute(query, timeout=self.timeout)

        if not result['success']:
            return result

        slave_status = self.slave_status()
        if not slave_status['success']:
            return slave_status
        timeout_start = time.time()
        while (slave_thread == '' and (slave_status['slave_io_running'] != 'Yes' or slave_status['slave_sql_running'] != 'Yes')) or \
              (slave_thread == 'SQL_THREAD' and slave_status['slave_sql_running'] != 'Yes') or \
              (slave_thread == 'IO_THREAD' and slave_status['slave_io_running'] != 'Yes'):
            time.sleep(0.1)
            if time.time() > (timeout_start + self.timeout):
                break
            slave_status = self.slave_status()

        if slave_thread == '' and (slave_status['slave_io_running'] != 'Yes' or slave_status['slave_sql_running'] != 'Yes'):
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_io_error'] + slave_status['last_sql_error']}
        elif slave_thread == 'SQL_THREAD' and slave_status['slave_sql_running'] != 'Yes':
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_sql_error']}
        elif slave_thread == 'IO_THREAD' and slave_status['slave_io_running'] != 'Yes':
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_io_error']}
        else:
            return slave_status

    def stop_slave(self, thread=None):
        """
        Starts the replication thread given (allowed values are 'sql', 'io', or None- the default, which will try
        to stop both threads). If all the given threads are already stopped, it returns with an error.
        For convenience, if the action is succesful, it will return a copy of the replication status.
        """
        slave_status = self.slave_status()
        if thread is None:
            if slave_status['slave_sql_running'] == 'No' and slave_status['slave_io_running'] == 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'Both IO and SQL threads are already stopped'}
            slave_thread = ''
        elif thread.lower() == 'sql':
            if slave_status['slave_sql_running'] == 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'SQL thread is already stopped'}
            slave_thread = 'SQL_THREAD'
        elif thread.lower() == 'io':
            if slave_status['slave_io_running'] == 'No':
                return {'success': False, 'errno': -1, 'errmsg': 'IO thread is already stopped'}
            slave_thread = 'IO_THREAD'
        else:
            return {'success': False, 'errno': -1, 'errmsg': 'Slave Thread can only be SQL, IO or None (default)'}

        query = 'STOP SLAVE {}'.format(slave_thread).rstrip()
        result = self.connection.execute(query, timeout=self.timeout)

        if not result['success']:
            return result

        slave_status = self.slave_status()
        if not slave_status['success']:
            return slave_status
        timeout_start = time.time()
        while (slave_thread == '' and (slave_status['slave_io_running'] != 'No' or slave_status['slave_sql_running'] != 'No')) or \
              (slave_thread == 'SQL_THREAD' and slave_status['slave_sql_running'] != 'No') or \
              (slave_thread == 'IO_THREAD' and slave_status['slave_io_running'] != 'No'):
            time.sleep(0.1)
            if time.time() > (timeout_start + self.timeout):
                break
            slave_status = self.slave_status()

        if slave_thread == '' and (slave_status['slave_io_running'] != 'No' or slave_status['slave_sql_running'] != 'No'):
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_io_error'] + slave_status['last_sql_error']}
        elif slave_thread == 'SQL_THREAD' and slave_status['slave_sql_running'] != 'No':
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_sql_error']}
        elif slave_thread == 'IO_THREAD' and slave_status['slave_io_running'] != 'No':
            return {'success': False, 'errno': -1, 'errmsg': slave_status['last_io_error']}
        else:
            return slave_status

    def master(self):
        """
        Returns a WMFMariaDB object of the current master of the replicating instance or None if
        the current instance does not have a working replica.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            return None
        else:
            return WMFMariaDB.WMFMariaDB(host=slave_status['master_host'], port=slave_status['master_port'])

    def slaves(self):
        """
        Returns a list of WMFMariaDB objects of all currently connected replicas of the instances, as detected by
        show slave hosts. It requires --report-host correctly configured on the replicas.
        If report-host is not configured, it fails back to assume its server_id is the same as its ipv4 (which has
        the limitation that there cannot be more than 1 server from the same ip).
        This means it will only detect replicas which io_thread is running (connected to the master).
        If no replica is detected, it returns the empty array.
        TODO: SHOW SLAVE HOSTS is a limited way to discover replicas; however, processlist doesn't show the source
        port.
        """
        slaves = []
        result = self.connection.execute('SHOW SLAVE HOSTS')
        if not result['success'] or result['numrows'] == 0 or result['fields'][1] != 'Host' or result['fields'][2] != 'Port' or result['fields'][0] != 'Server_id':
            return slaves
        for row in result['rows']:
            host = row[1]
            if host is None or host == '':
                # --report-host is not set, use server_id as the ipv4, and perform an inverse dns resolution
                server_id = int(row[0])
                ip = str(ipaddress.IPv4Address(server_id))
                host = socket.gethostbyaddr(ip)[0]
            port = int(row[2])
            if port not in (0, 3306):
                host += ':' + str(port)
            connection = WMFMariaDB.WMFMariaDB(host)
            if WMFReplication(connection).is_direct_replica_of(self.connection):
                slaves.append(connection)
        return slaves

    def caught_up_to_master(self, master=None):
        """
        Checks if replication has caught up to the direct master (normally after the master
        stopped replication or is set as read_only). The optional parameters allows to provide
        a master host explicitly, mainly if we are already connected to it so we can reuse the
        connection or to force certain connection options, but if none is given, we will
        auto-discover it.
        Returns true if the slave caught up, and false if there was any error or there is lag
        between the master and the slave. Note that if writes are ongoing on the master, this
        will be meaningless, as the slave will lag and catch up continuously- use
        lag() or heatbeat_status() to check ongoing lag.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            return False
        if master is None:
            # Autodiscover master
            master = self.master()
        master_replication = WMFReplication(master)
        master_status = master_replication.master_status()
        if master_status is None or not master_status['success']:
            return False
        return slave_status['relay_master_log_file'] == master_status['file'] and slave_status['exec_master_log_pos'] == master_status['position']

    def restore_replication_status(self, new_master_replication, slave_status, start_if_stopped):
        """
        Returns replication to the original state on both the current host and the master,
        based either on the forced start parameter (start_if_stopped), or on the original
        state (slave_status).
        Not intended to be used directly, but only when calling move()
        """
        if start_if_stopped:
            self.start_slave()
            new_master_replication.start_slave()
        elif slave_status['slave_sql_running'] == 'Yes' and slave_status['slave_io_running'] == 'Yes':
            self.start_slave()
        elif slave_status['slave_sql_running'] == 'Yes':
            self.start_slave(thread='sql')
        elif slave_status['slave_io_running'] == 'Yes':
            self.start_slave(thread='io')

        # TODO: What about GTID mode restoration?
        return self.slave_status()

    def move(self, new_master, start_if_stopped=False):
        """
        Highly WIP, only the most trivial scenarios implemented.
        Switches the current instance from the current master to another, trying to
        auto-discover the right coordinates (even in cases where topology is not
        trivial) and without stopping or modifying the given master instance in any
        way. The original master can have replication stopped or ongoing, but it must
        be already setup. If the change cannot be done or it is tried and it fails,
        it will return to the replication state before the change.
        new_master is a WMFMariaDB object (an open connection to the instance that
        will be the new master.
        """
        slave_status = self.slave_status()
        # is the replication on the slave already setup?
        if slave_status is None or not slave_status['success']:
            return {'success': False, 'errno': -1, 'errmsg': 'The host is not configured as a replica'}
        # is the host already replicating from the new master ?
        if new_master.host == slave_status['master_host'] and new_master.port == int(slave_status['master_port']):
            return {'success': False, 'errno': -1, 'errmsg': 'The host is already configured as a master of {}'.format(':'.join((new_master.host, str(new_master.port))))}
        # is the instance to be changed and the new master the same instance (are we trying to connect it to itself)?
        if new_master.host == self.connection.host and new_master.port == self.connection.port:
            return {'success': False, 'errno': -1, 'errmsg': 'The host is trying to connect to itself'}

        # TODO: Does the new master have working replication credentials?

        new_master_replication = WMFReplication(new_master)
        new_master_master_status = new_master_replication.master_status()
        new_master_slave_status = new_master_replication.slave_status()

        # Are both hosts replicating from the same master and stopped on the same coordinate? If yes, then just move them directly
        current_master = self.master()
        current_master_new_master = new_master_replication.master()
        if current_master.is_same_instance_as(current_master_new_master) and \
           slave_status['slave_sql_running'] == 'No' and \
           new_master_slave_status['slave_sql_running'] == 'No' and \
           self.caught_up_to_master(current_master) and \
           new_master_replication.caught_up_to_master(current_master):
            self.stop_slave()
            self.reset_slave()
            result = self.setup(master_host=new_master.host, master_port=new_master.port, master_log_file=new_master_master_status['file'], master_log_pos=new_master_master_status['position'])
            if result['success']:
                return self.restore_replication_status(new_master_replication, slave_status, start_if_stopped)
            else:
                return result

        # Is the host we are replicating from replicating directly from the new master, stopped, and the replica caught up?
        # Also move it directly
        current_master_replication = WMFReplication(current_master)
        current_master_slave_status = current_master_replication.slave_status()
        # current_master_master_status = current_master_replication.master_status()

        if current_master_slave_status is not None and \
           current_master_replication.master().is_same_instance_as(new_master) and \
           current_master_slave_status['slave_sql_running'] == 'No' and \
           self.caught_up_to_master(current_master):
            self.stop_slave()
            self.reset_slave()
            result = self.setup(master_host=new_master.host, master_port=new_master.port, master_log_file=current_master_slave_status['relay_master_log_file'], master_log_pos=current_master_slave_status['exec_master_log_pos'])
            if result['success']:
                return self.restore_replication_status(new_master_replication, slave_status, start_if_stopped)
            else:
                return result

        # Are both hosts replicating and without lag? Then stop the replica and move it
        if slave_status['slave_sql_running'] == 'Yes' and \
           slave_status['slave_io_running'] == 'Yes' and \
           slave_status['seconds_behind_master'] < self.timeout and \
           (new_master_slave_status is None or
           (new_master_slave_status['slave_sql_running'] == 'Yes' and
               new_master_slave_status['slave_io_running'] == 'Yes' and
               new_master_slave_status['seconds_behind_master'] < self.timeout)):
            # TODO: Probably, in this case, lag check should be done using heartbeat
            # TODO: Flush logs before stop for easier find?
            result = self.stop_slave()
            if not result['success']:
                return {'success': False, 'errno': -1, 'errmsg': 'The instance could not stop its replication, impossible to perform the topology change'}
            master_log_file_after_stop = result['relay_master_log_file']
            master_log_pos_after_stop = result['exec_master_log_pos']
            result = self.master_status()
            slave_log_file_after_stop = result['file']
            slave_log_post_after_stop = result['position']
            print('Stopped at master position: {}, slave position {}'.format(master_log_file_after_stop + ':' + str(master_log_pos_after_stop),
                                                                             slave_log_file_after_stop + ':' + str(slave_log_post_after_stop)))
            slave_binlogs = self.connection.execute('SHOW BINARY LOGS')
            last_gtid_executed = None
            for current_binlog_index in range(slave_binlogs['numrows'] - 1, -1, -1):
                binlog_file = slave_binlogs['rows'][current_binlog_index][0]
                events = self.connection.execute('SHOW BINLOG EVENTS IN \'{}\''.format(binlog_file))

                for event in events['rows']:
                    if event[1] >= slave_log_post_after_stop:
                        break  # we have reached the end of the events replicated
                    if event[2] == 'Gtid' and event[5].startswith('BEGIN GTID'):
                        last_gtid_executed = event[5]
                if last_gtid_executed is not None:
                    break
            if last_gtid_executed is None:
                self.restore_replication_status(new_master_replication, slave_status, start_if_stopped)
                return {'success': False, 'errno': -1, 'errmsg': 'Failed to find a usable GTID on the current instance'}
            print('Last GTID executed: {}'.format(last_gtid_executed))

            master_binlogs = new_master.execute('SHOW BINARY LOGS')
            found_master_binlog_file = None
            found_master_binlog_pos = None
            for current_binlog_index in range(master_binlogs['numrows'] - 1, -1, -1):
                binlog_file = master_binlogs['rows'][current_binlog_index][0]
                events = new_master.execute('SHOW BINLOG EVENTS IN \'{}\''.format(binlog_file))

                for event in events['rows']:
                    if event[2] == 'Gtid' and event[5] == last_gtid_executed:
                        found_master_binlog_file = event[0]
                        found_master_binlog_pos = event[1]
                        print('Position found on the master at: {}:{}'.format(found_master_binlog_file, str(found_master_binlog_pos)))
                        break
                if found_master_binlog_file is not None:
                    break
            if found_master_binlog_file is None:
                self.restore_replication_status(new_master_replication, slave_status, start_if_stopped)
                return {'success': False, 'errno': -1, 'errmsg': 'Failed to find a usable GTID on the master'}

            self.reset_slave()
            result = self.setup(master_host=new_master.host, master_port=new_master.port, master_log_file=found_master_binlog_file, master_log_pos=found_master_binlog_pos)
            if result['success']:
                return self.restore_replication_status(new_master_replication, slave_status, start_if_stopped)
            else:
                return result

        return {'success': False, 'errno': -1, 'errmsg': 'The topology change cannot be done at the moment- check its relationship, replication status or replication lag'}

    def debug(self):
        """
        Prints in a human readable format the replication status of the host.
        """
        # master_status = self.master_status()  # print binlog coordinates too?
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            print('{}> Not configured as a slave'.format(self.connection.name()))
        else:
            print('{}> master: {}, io: {}, sql: {}, pos: {}'.format(self.connection.name(),
                                                                    slave_status['master_host'] +
                                                                    ':' +
                                                                    str(slave_status['master_port']),
                                                                    slave_status['slave_io_running'],
                                                                    slave_status['slave_sql_running'],
                                                                    slave_status['relay_master_log_file'] +
                                                                    '/' +
                                                                    str(slave_status['exec_master_log_pos'])))

    def gtid_mode(self):
        """
        Returns the current gtid mode of the replication. Returns None on error.
        """
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            return None
        else:
            return slave_status['using_gtid']

    def set_gtid_mode(self, mode):
        """
        Changes the gtid mode of a replica to the one given (no, slave_pos or current_pos). It
        returns true if it is succesful, false otherwise.
        """
        if mode.lower() not in ('no', 'slave_pos', 'current_pos'):
            print('Incorrect mode')
            return False
        slave_status = self.slave_status()
        if slave_status is None or not slave_status['success']:
            print('Server is not a slave or other error happened on checking the current status')
            return False
        if slave_status['using_gtid'].lower() == mode:
            print('The server is already on "{}" mode'.format(mode))
            return True
        if slave_status['slave_io_running'] == 'Yes' or slave_status['slave_sql_running'] == 'Yes':
            stop_slave = self.stop_slave()
            if not stop_slave['success']:
                print('Could not stop slave: {}'.format(stop_slave['errmsg']))
                return False
        change_master = self.connection.execute('CHANGE MASTER TO MASTER_USE_GTID = {}'.format(mode))

        if not change_master['success']:
            print('Could not change gtid mode: {}'.format(change_master['errmsg']))
        if slave_status['slave_io_running'] == 'Yes' and slave_status['slave_sql_running'] == 'Yes':
            start_slave = self.start_slave()
        elif slave_status['slave_io_running'] == 'Yes':
            start_slave = self.start_slave(thread='io')
        elif slave_status['slave_sql_running'] == 'Yes':
            start_slave = self.start_slave(thread='sql')
        else:
            start_slave = dict()
            start_slave['success'] = True

        if not start_slave['success']:
            print('Could not restart slave after change master: {}'.format(start_slave['errmsg']))
        return change_master['success']

    def heartbeat_status(self):
        """
        Returns the status of the replication, according to heartbeat (without running show slave
        status).
        Useful when a blocking query like show slave status wants to be avoided (e.g. it has to be
        fast and/or run many times) or a better lag detection is needed.
        """
        pass
