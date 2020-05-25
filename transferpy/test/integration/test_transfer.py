"""Integration tests for Transferer class."""
import unittest
import time

from transferpy.Transferer import Transferer
from transferpy.Firewall import Firewall


class TestTransferer(unittest.TestCase):
    """Test cases for Transferer."""
    HOST_NAME = "localhost"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _use_ports(self, host, ports):
        """
        nc listen to the given ports on the give host.

        :param host: nc target host
        :param ports: nc target ports
        :return: list of references to nc listen jobs
        """
        jobs = []
        for port in ports:
            self.options['port'] = port
            listen_command = ['/bin/bash', '-c', r'"{}"'.format(self.transferer.netcat_listen_command)]
            jobs.append(self.transferer.remote_executor.start_job(host, listen_command))
        time.sleep(3)
        return jobs

    def _kill_use_ports(self, host, jobs, ports):
        """
        Kill the jobs on the given host.

        :param host: host on which the jobs to be killed
        :param jobs: list of jobs to be killed
        :param ports: list of ports used
        """
        for job in jobs:
            self.transferer.remote_executor.kill_job(host, job)
        for p in ports:
            self.transferer.firewall_handler.kill_process(p)

    def setUp(self):
        """Setup the tests."""
        self.options = {'verbose': False}
        self.host = self.HOST_NAME
        self.transferer = Transferer(self.host, 'path',
                                     [self.host], ['path'],
                                     self.options)

    def test_find_available_port(self):
        """Test find available port."""
        use_ports = [4400, 4401]

        jobs = self._use_ports(self.host, use_ports)
        self.options['port'] = 0
        self.transferer.firewall_handler = Firewall(self.host, self.transferer.remote_executor)
        self.options['port'] = self.transferer.firewall_handler.open(self.host, self.options['port'])

        # Close ports
        self._kill_use_ports(self.host, jobs, use_ports)
        if self.transferer.firewall_handler.close(self.host, self.options['port']) != 0:
            print('WARNING: Firewall\'s temporary rule could not be deleted')

        # In the test running machine expect no other
        # process uses port between 4400 and 4499.
        # Since 4400 and 4401 are used by this test,
        # port opened for transfer should be 4402.
        self.assertEqual(self.options['port'], 4402)
