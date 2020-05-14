"""Tests for CuminExecution class."""
import unittest
from unittest.mock import patch, MagicMock

from transferpy.RemoteExecution.CuminExecution import CuminExecution


class TestCuminExecution(unittest.TestCase):
    """Test cases for CuminExecution."""

    def setUp(self):
        self.executor = CuminExecution()

    @patch('transferpy.RemoteExecution.CuminExecution.cumin.Config')
    def test_config(self, config_mock):
        config_mock.return_value = MagicMock()

        conf1 = self.executor.config
        conf2 = self.executor.config

        self.assertEqual(config_mock.return_value, conf1)
        self.assertEqual(config_mock.return_value, conf2)
        self.assertEqual(1, config_mock.call_count)

    def test_format_command_str(self):
        orig_cmd = "some command"
        formatted_command = self.executor.format_command(orig_cmd)

        self.assertEqual(orig_cmd, formatted_command)

    def test_format_command_list(self):
        orig_cmd = ["some", "command"]
        formatted_command = self.executor.format_command(orig_cmd)

        self.assertEqual(' '.join(orig_cmd), formatted_command)

    @patch('transferpy.RemoteExecution.CuminExecution.cumin.Config',
           return_value={'transport': 'clustershell', 'default_backend': 'knownhosts'})
    def test_run_invalid_host(self, config_mock):
        host = 'wrong_host.eqiad.wmnet'
        command_return = self.executor.run(host, 'some command')

        self.assertEqual(command_return.returncode, 1)
        self.assertEqual(command_return.stdout, None)
        self.assertEqual(command_return.stderr, 'host is wrong or does not match rules')
