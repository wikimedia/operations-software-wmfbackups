#!/usr/bin/python3


class Firewall(object):
    def __init__(self, target_host, remote_execution):
        self.target_host = target_host
        self.remote_executor = remote_execution

    def run_command(self, command):
        return self.remote_executor.run(self.target_host, command)

    def open(self, source_host, target_port):
        command = ['/sbin/iptables', '-A', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(command)
        if result.returncode != 0:
            raise Exception('iptables execution failed')

    def close(self, source_host, target_port):
        command = ['/sbin/iptables', '-D', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(command)
        return result.returncode

    def __del__(self):
        pass
