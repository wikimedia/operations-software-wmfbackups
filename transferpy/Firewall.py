#!/usr/bin/python3


class Firewall(object):
    """Class for Transferer firewall related command execution"""
    def __init__(self, target_host, remote_execution):
        """
        Initialize the instance variables.

        :param target_host: host address for port open/close
        :param remote_execution: remote execution helper
        """
        self.target_host = target_host
        self.remote_executor = remote_execution

    def run_command(self, command):
        """
        Executes command on the target host.

        :param command: command to run
        :return: execution result (returncode, stdout, stderr)
        """
        return self.remote_executor.run(self.target_host, command)

    def open(self, source_host, target_port):
        """
        Opens target port on iptables of target host.

        :param source_host: sender host
        :param target_port: port to be opened
        :return: raises exception if not successful
        """
        command = ['/sbin/iptables', '-A', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(command)
        if result.returncode != 0:
            raise Exception('iptables execution failed')

    def close(self, source_host, target_port):
        """
        Closes target port on iptables of target host.

        :param source_host: sender host
        :param target_port: port to be closed
        :return: remote run exit code, successful(0)
        """
        command = ['/sbin/iptables', '-D', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(command)
        return result.returncode

    def __del__(self):
        """Destructor"""
        pass
