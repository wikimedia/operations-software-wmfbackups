#!/usr/bin/python3


class MariaDB(object):
    def __init__(self, remote_execution):
        self.remote_executor = remote_execution

    def run_command(self, host, command):
        return self.remote_executor.run(host, command)

    def start_replication(self, host, socket):
        """
        Start slave on instance of the given host and socket
        """
        command = ['/usr/local/bin/mysql', '--socket', socket,
                   '--connect-timeout=10',
                   '--execute="START SLAVE"']
        result = self.run_command(host, command)
        return result.returncode

    def stop_replication(self, host, socket):
        """
        Stop slave on instance of the given host and socket
        """
        command = ['/usr/local/bin/mysql', '--socket', socket,
                   '--connect-timeout=10',
                   '--execute="STOP SLAVE"']
        result = self.run_command(host, command)
        return result.returncode
