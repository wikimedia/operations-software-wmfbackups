#!/usr/bin/python3


class Firewall(object):
    def __init__(self, remote_execution):
        self.remote_executor = remote_execution

    def run_command(self, host, command):
        return self.remote_executor.run(host, command)

    def open(self, source_host, target_host, target_port):
        command = ['/sbin/iptables', '-A', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(target_host, command)
        if result.returncode != 0:
            raise Exception('iptables execution failed')

    def close(self, source_host, target_host, target_port):
        command = ['/sbin/iptables', '-D', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(target_host, command)
        return result.returncode
