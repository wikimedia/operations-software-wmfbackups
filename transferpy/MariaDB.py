#!/usr/bin/python3


class MariaDB(object):
    """Class for Transferer MariaDB related command execution"""
    def __init__(self, remote_execution):
        """
        Initialize the instance variables.

        :param remote_execution: remote execution helper
        """
        self.remote_executor = remote_execution

    def run_command(self, host, command):
        """
        Executes command on the target host.

        :param host: command execution target host
        :param command: command to be executed
        :return: execution result (returncode, stdout, stderr)
        """
        return self.remote_executor.run(host, command)

    def start_replication(self, host, socket):
        """
        Starts slave on the instance of the given host and socket.

        :param host: MariaDB slave host
        :param socket: MariaDB slave socket
        :return: remote run exit code, successful(0)
        """
        command = ['/usr/local/bin/mysql', '--socket', socket,
                   '--connect-timeout=10',
                   '--execute="START SLAVE"']
        result = self.run_command(host, command)
        return result.returncode

    def stop_replication(self, host, socket):
        """
        Stops slave on the instance of the given host and socket.

        :param host: MariaDB slave host
        :param socket: MariaDB slave socket
        :return: remote run exit code, successful(0)
        """
        command = ['/usr/local/bin/mysql', '--socket', socket,
                   '--connect-timeout=10',
                   '--execute="STOP SLAVE"']
        result = self.run_command(host, command)
        return result.returncode
