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
        self.search_start_port = 4400
        self.search_end_port = 4500

    @property
    def find_available_port_command(self):
        """
        Property: command to find to available port.

        :return: command to find available port
        """
        # TODO: Make this command in terms of search_start_port and search_end_port
        command = ["netstat -altn | awk '{print $4}' | awk -F: '{print $NF}' | grep ^44[0-9][0-9]$ || echo 0"]
        return command

    def find_pid(self, target_port):
        """
        Finds pid of the process based on the port it is using.

        :param target_port: the port using by process
        """
        command = "fuser {}/tcp".format(target_port)
        result = self.run_command(command)
        if result.returncode != 0:
            raise Exception('failed to find PID based on the port {} on {}'
                            .format(target_port, self.target_host))
        else:
            try:
                pid = int(result.stdout.split(':')[1].strip())
            except Exception as e:
                raise Exception('failed to find PID based on the port {} on {}, {}'
                                .format(target_port, self.target_host, str(e)))
        return pid

    def kill_process(self, target_port):
        """
        Kill the process based on the port it is using.

        :param target_port: the port using by process
        :return: raises exception if not successful
        """
        command = "fuser -k {}/tcp || echo 0".format(target_port)
        result = self.run_command(command)
        if result.returncode != 0:
            raise Exception('failed to kill process based on the port {} on {}'
                            .format(target_port, self.target_host))

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
        # If target port is 0, find a free port automatically
        if target_port == 0:
            target_port = self.find_available_port()

        command = ['/sbin/iptables', '-A', 'INPUT', '-p', 'tcp', '-s',
                   '{}'.format(source_host),
                   '--dport', '{}'.format(target_port),
                   '-j', 'ACCEPT']
        result = self.run_command(command)
        if result.returncode != 0:
            raise Exception('iptables execution failed')
        return target_port

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

    def find_available_port(self):
        """
        Checks port availability from a given range of ports on
        the target host and select one among them.

        :return: available port if successful, else raises ValueError
        """
        result = self.run_command(self.find_available_port_command)
        num_of_searches = self.search_end_port - self.search_start_port
        if result.returncode != 0 or len(result.stdout.split('\n')) == num_of_searches:
            raise ValueError('failed to find an available port on {}'.format(self.target_host))

        try:
            used_ports = [int(i) for i in result.stdout.split('\n')]
        except Exception as e:
            raise ValueError("ERROR: Returned non integer value for used ports "
                             "on {}\n{}".format(self.target_host, str(e)))

        for p in range(self.search_start_port, self.search_end_port):
            if p not in used_ports:
                port = p
                break
        return port

    def __del__(self):
        """Destructor"""
        pass
