"""Tests for CuminExecution class."""
import unittest
from unittest.mock import patch, MagicMock

from wmfmariadbpy.CuminExecution import CuminExecution


class TestCuminExecution(unittest.TestCase):
    """Test cases for CuminExecution."""

    def setUp(self):
        self.executor = CuminExecution()

    @patch('wmfmariadbpy.CuminExecution.cumin.Config')
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
