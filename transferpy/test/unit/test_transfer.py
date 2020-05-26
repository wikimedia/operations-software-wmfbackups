"""Tests for transfer.py class."""
import sys
import unittest
from unittest.mock import patch, MagicMock

from transferpy.transfer import option_parse
from transferpy.Transferer import Transferer

from transferpy.test.utils import hide_stderr


class TestTransferer(unittest.TestCase):

    @patch('transferpy.Transferer.RemoteExecution')
    def setUp(self, executor_mock):
        self.executor = MagicMock()
        executor_mock.return_value = self.executor

        self.options = {'verbose': False}

        self.transferer = Transferer('source', 'path',
                                     ['target'], ['path'],
                                     self.options)

    def test_run_command(self):
        self.transferer.run_command('host', 'command')

        self.executor.run.assert_called_with('host', 'command')

    def test_is_dir(self):
        path = 'path'
        self.transferer.is_dir('host', path)

        args = self.executor.run.call_args[0]
        self.assertIn(r'"[ -d "{}" ]"'.format(path), args[1])

    def test_file_exists(self):
        file = 'path'
        self.transferer.file_exists('host', file)

        args = self.executor.run.call_args[0]
        self.assertIn(r'"[ -a "{}" ]"'.format(file), args[1])

    def test_calculate_checksum_for_dir(self):
        self.transferer.source_is_dir = True
        self.executor.run.return_value = MagicMock()
        self.executor.run.return_value.returncode = 0

        self.transferer.calculate_checksum('host', 'path')

        args = self.executor.run.call_args[0]
        self.assertIn('find', args[1][-1])
        self.assertIn('md5sum', args[1][-1])

    def test_calculate_checksum_for_file(self):
        self.transferer.source_is_dir = False
        self.executor.run.return_value = MagicMock()
        self.executor.run.return_value.returncode = 0

        self.transferer.calculate_checksum('host', 'path')

        args = self.executor.run.call_args[0]
        self.assertNotIn('find', args[1][-1])
        self.assertIn('md5sum', args[1][-1])

    def test_has_available_disk_space(self):
        self.executor.run.return_value = MagicMock()
        self.executor.run.return_value.returncode = 0

        size = 100
        self.executor.run.return_value.stdout = str(size + 1)

        result = self.transferer.has_available_disk_space('host', 'path', size)

        self.assertTrue(result)

    def test_disk_usage(self):
        self.executor.run.return_value = MagicMock()
        self.executor.run.return_value.returncode = 0
        size = 1024
        self.executor.run.return_value.stdout = "{} path".format(size)

        result = self.transferer.disk_usage('host', 'path')

        self.assertEqual(size, result)

    def test_compress_command_compressing(self):
        self.options['compress'] = True

        command = self.transferer.compress_command
        self.assertIn('pigz -c', command)

    def test_compress_command_not_compressing(self):
        self.options['compress'] = False

        self.transferer.source_is_dir = True
        command = self.transferer.compress_command
        self.assertEqual('', command)

        self.transferer.source_is_dir = False
        command = self.transferer.compress_command
        self.assertIn('cat', command)

    def test_decompress_command_compressing(self):
        self.options['compress'] = True

        command = self.transferer.decompress_command
        self.assertIn('pigz -c -d', command)

    def test_decompress_command_not_compressing(self):
        self.options['compress'] = False

        command = self.transferer.decompress_command
        self.assertEqual('', command)

    def test_encrypt_command_encrypting(self):
        self.options['encrypt'] = True

        command = self.transferer.encrypt_command
        self.assertIn('openssl enc', command)

    def test_encrypt_command_not_encrypting(self):
        self.options['encrypt'] = False

        command = self.transferer.encrypt_command
        self.assertEqual('', command)

    def test_decrypt_command_encrypting(self):
        self.options['encrypt'] = True

        command = self.transferer.decrypt_command
        self.assertIn('openssl enc -d', command)

    def test_decrypt_command_not_encrypting(self):
        self.options['encrypt'] = False

        command = self.transferer.decrypt_command
        self.assertEqual('', command)

    def test_run_sanity_checks_failing(self):
        """Test case for Transferer.run function which simulates sanity check failure."""
        with patch.object(Transferer, 'sanity_checks') as mocked_sanity_check:
            mocked_sanity_check.side_effect = ValueError('Test sanity_checks')
            command = self.transferer.run()
            self.assertTrue(type(command) == list)

    def test_run_stoping_slave(self):
        """Test case for Transferer.run function which provides stop_slave option"""
        with patch.object(Transferer, 'sanity_checks') as mocked_sanity_check,\
                patch('transferpy.Transferer.MariaDB.stop_replication') as mocked_stop_replication:
            self.options['stop_slave'] = True
            #  Return value should be anything other than 0 for the if block to execute
            mocked_stop_replication.return_value = 1
            mocked_sanity_check.called_once()
            command = self.transferer.run()
            self.assertTrue(type(command) == list)

    def test_run_successfully(self):
        """Test case for Transferer.run function starting transfer successfully"""
        with patch.object(Transferer, 'sanity_checks') as mocked_sanity_check,\
                patch('transferpy.Transferer.Firewall.open') as mocked_open_firewall,\
                patch.object(Transferer, 'copy_to') as mocked_copy_to,\
                patch('transferpy.Transferer.Firewall.close') as mocked_close_firewall,\
                patch.object(Transferer, 'after_transfer_checks') as mocked_after_transfer_checks,\
                patch('transferpy.Transferer.MariaDB.start_replication') as mocked_start_replication:
            self.options['port'] = 4444
            mocked_copy_to.return_value = 0
            mocked_close_firewall.return_value = 0
            mocked_after_transfer_checks.return_value = 0
            mocked_start_replication.return_value = 0
            mocked_sanity_check.called_once()
            mocked_open_firewall.called_once()
            command = self.transferer.run()
            self.assertTrue(type(command) == list)

    def test_run_start_slave(self):
        """Test case for Transferer.run function for when it runs the
           start_slave function with the stop_slave option
        """
        with patch('transferpy.Transferer.MariaDB.stop_replication') as mocked_stop_replication,\
                patch.object(Transferer, 'sanity_checks') as mocked_sanity_check,\
                patch('transferpy.Transferer.Firewall.open') as mocked_open_firewall,\
                patch.object(Transferer, 'copy_to') as mocked_copy_to,\
                patch('transferpy.Transferer.Firewall.close') as mocked_close_firewall,\
                patch.object(Transferer, 'after_transfer_checks') as mocked_after_transfer_checks,\
                patch('transferpy.Transferer.MariaDB.start_replication') as mocked_start_replication:
            self.options['port'] = 4444
            self.options['stop_slave'] = True
            # We need to skip the first if statement
            # which checks the stop slave option
            mocked_stop_replication.return_value = 0
            mocked_copy_to.return_value = 0
            mocked_close_firewall.return_value = 0
            mocked_after_transfer_checks.return_value = 0
            # Return value should be anything other than 0
            # for this if block to execute
            mocked_start_replication.return_value = 1
            mocked_sanity_check.called_once()
            mocked_open_firewall.called_once()
            command = self.transferer.run()
            self.assertTrue(type(command) == list)


class TestArgumentParsing(unittest.TestCase):
    """Test cases for the command line arguments parsing."""

    def option_parse(self, args):
        """Call parse_args patching the arguments."""
        with patch.object(sys, 'argv', args):
            return option_parse()

    def check_bad_args(self, args, expected_error=SystemExit):
        """Check arg parsing fails for the given args."""
        with self.assertRaises(expected_error) as exc:
            with hide_stderr():
                self.option_parse(args)

        if expected_error == SystemExit:
            self.assertEquals(exc.exception.code, 2)

    def test_missing_required_args(self):
        """Test errors with missing required args."""
        missing_required_args_list = [
            ['transfer'],
            ['transfer', 'src:path'],
            ['transfer', 'trg?:path'],
        ]
        for test_args in missing_required_args_list:
            self.check_bad_args(test_args)

    def test_bad_source(self):
        """Test errors with the source."""
        test_args = ['transfer', 'source', 'target:path']
        self.check_bad_args(test_args)

    def test_bad_target(self):
        """Test errors with the target."""
        test_args = ['transfer', 'source:path', 'target']
        self.check_bad_args(test_args)

    def test_just_source_and_targets(self):
        """Test call with just source and targets."""
        src = 'source'
        src_path = 'source_path'
        trg1 = 'target1'
        trg1_path = 'dst_path1'
        trg2 = 'target2'
        trg2_path = 'dst_path2'
        test_args = ['transfer',
                     '{}:{}'.format(src, src_path),
                     '{}:{}'.format(trg1, trg1_path),
                     '{}:{}'.format(trg2, trg2_path)]
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(test_args)

        self.assertEqual(src, source_host)
        self.assertEqual(src_path, source_path)
        self.assertEqual([trg1, trg2], target_hosts)
        self.assertEqual([trg1_path, trg2_path], target_paths)
        self.assertEqual(other_options['port'], 0)
        self.assertTrue(other_options['compress'])
        self.assertTrue(other_options['encrypt'])

    def test_port(self):
        """Test port param."""
        port = 12345
        test_args = ['transfer', 'source:path', 'target:path', '--port', str(port)]
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(test_args)
        self.assertEqual(other_options['port'], port)
        self.assertTrue(other_options['compress'])
        self.assertTrue(other_options['encrypt'])

    def test_compress(self):
        """Test compress params."""
        base_args = ['transfer', 'source:path', 'target:path']

        compress_test_args = base_args + ['--compress']
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(compress_test_args)
        self.assertTrue(other_options['compress'])
        self.assertTrue(other_options['encrypt'])

        no_compress_test_args = base_args + ['--no-compress']
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(no_compress_test_args)
        self.assertFalse(other_options['compress'])
        self.assertTrue(other_options['encrypt'])

    def test_encrypt(self):
        """Test encrypt params."""
        base_args = ['transfer', 'source:path', 'target:path']

        encrypt_test_args = base_args + ['--encrypt']
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(encrypt_test_args)
        self.assertTrue(other_options['compress'])
        self.assertTrue(other_options['encrypt'])

        no_encrypt_test_args = base_args + ['--no-encrypt']
        (source_host, source_path, target_hosts, target_paths, other_options)\
            = self.option_parse(no_encrypt_test_args)
        self.assertTrue(other_options['compress'])
        self.assertFalse(other_options['encrypt'])
