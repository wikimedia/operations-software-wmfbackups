"""
Testing of the MariaBackup class
"""

import unittest
from unittest.mock import patch, mock_open

from wmfbackups.MariaBackup import MariaBackup, XtrabackupError
from wmfbackups.WMFBackup import WMFBackup


class TestMariaBackup(unittest.TestCase):
    """test module implementing the execution of xtrabackup --backup & xtrabackup --prepare"""

    def setUp(self):
        """Set up the tests."""
        self.backup = WMFBackup('test', {'type': 'null'})
        self.backup.dir_name = 'test'
        self.maria_backup = MariaBackup({'type': 'null'}, self.backup)

    def test_uniformize_vendor_string(self):
        mb = self.maria_backup
        self.assertEqual(mb.uniformize_vendor_string(None), 'MySQL')
        self.assertEqual(mb.uniformize_vendor_string(''), 'MySQL')
        self.assertEqual(mb.uniformize_vendor_string('log'), 'MySQL')
        self.assertEqual(mb.uniformize_vendor_string('debug'), 'MySQL')
        self.assertIsNone(mb.uniformize_vendor_string('MySQL'))
        self.assertIsNone(mb.uniformize_vendor_string('Second Edition™'))
        self.assertEqual(mb.uniformize_vendor_string('MariaDB'), 'MariaDB')
        self.assertEqual(mb.uniformize_vendor_string('MariaDB-log'), 'MariaDB')
        self.assertEqual(mb.uniformize_vendor_string('MariaDB-volans-edition'), 'MariaDB')
        self.assertEqual(mb.uniformize_vendor_string('16'), 'Percona Server')
        self.assertEqual(mb.uniformize_vendor_string('16.1'), 'Percona Server')
        self.assertEqual(mb.uniformize_vendor_string('16-57'), 'Percona Server')
        self.assertEqual(mb.uniformize_vendor_string('16-57-log'), 'Percona Server')

    @patch('subprocess.Popen')
    def test__get_xtrabackup_version(self, mock):
        """Test getting xtrabackup version"""
        mb = self.maria_backup

        # basic run
        process_mock = mock.Mock()
        attrs = {
            'communicate.return_value': (
                b'',
                b'xtrabackup based on MariaDB server 10.4.22-MariaDB Linux (x86_64)'
            ),
            'returncode': 0
        }
        process_mock.configure_mock(**attrs)
        mock.return_value = process_mock
        self.assertEqual(mb._get_xtrabackup_version(), {'major': '10.4',
                                                        'minor': 22,
                                                        'vendor': 'MariaDB'})

        # basic run
        process_mock = mock.Mock()
        attrs = {
            'communicate.return_value': (
                b'',
                b'Xtrabackup 2.3 based on MySQL 5.7.10'
            ),
            'returncode': 0
        }
        process_mock.configure_mock(**attrs)
        mock.return_value = process_mock
        self.assertEqual(mb._get_xtrabackup_version(), {'major': '5.7',
                                                        'minor': 10,
                                                        'vendor': 'MySQL'})

        # failed execution
        process_mock = mock.Mock()
        attrs = {
            'returncode': 127
        }
        process_mock.configure_mock(**attrs)
        mock.return_value = process_mock
        self.assertRaises(XtrabackupError, mb._get_xtrabackup_version)

        # fail to get version from error output
        process_mock = mock.Mock()
        attrs = {
            'communicate.return_value': (
                b'',
                b'Xtrabackup based on an unknown MariaDB version (10.4 family, not yet released)'
            ),
            'returncode': 0
        }
        process_mock.configure_mock(**attrs)
        mock.return_value = process_mock
        self.assertRaises(XtrabackupError, mb._get_xtrabackup_version)

    def test__get_backup_source_server_version(self):
        """Test retrieving the server version from the xtrabackup_info file"""
        mb = self.maria_backup

        # mariadb basic run
        sample_xtrabackup_info = """
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 10.4.22-MariaDB
            start_time = 2022-02-15 13:08:12
            end_time = 2018-02-15 15:18:58
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertEqual(mb._get_backup_source_server_version(''),
                             {'major': '10.4',
                              'minor': 22,
                              'vendor': 'MariaDB'})
        # mariadb -log
        sample_xtrabackup_info = """
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 10.4.22-MariaDB-log
            start_time = 2022-02-15 13:08:12
            end_time = 2018-02-15 15:18:58
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertEqual(mb._get_backup_source_server_version(''),
                             {'major': '10.4',
                              'minor': 22,
                              'vendor': 'MariaDB'})
        # Percona server run
        sample_xtrabackup_info = """
            uuid = a6336c86-1b33-11e8-8de6-080027f3a5d8
            tool_name = xtrabackup
            tool_command = –backup –target-dir=/Backups/base –datadir=/var/lib/mysql –user=root –password=… –galera-info
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 5.7.19-17-57-log
            start_time = 2022-02-26 15:28:22
            end_time = 2022-02-26 15:28:48
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertEqual(mb._get_backup_source_server_version(''),
                             {'major': '5.7',
                              'minor': 19,
                              'vendor': 'Percona Server'})
        # MySQL
        sample_xtrabackup_info = """
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 8.0.29-log
            start_time = 2022-02-15 13:08:12
            end_time = 2018-02-15 15:18:58
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertEqual(mb._get_backup_source_server_version(''),
                             {'major': '8.0',
                              'minor': 29,
                              'vendor': 'MySQL'})
        # not found
        sample_xtrabackup_info = 'start_time = 2033-01-01 00:00:00'
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertRaises(XtrabackupError,
                              mb._get_backup_source_server_version,
                              '')

    def test_get_backup_command(self):
        """test returning the backup command based on the options"""
        mb = self.maria_backup
        mock = mock_open(read_data='')
        with patch('builtins.open', mock):  # skip reading the port list section
            self.assertEqual(mb.get_backup_cmd('test_dir'),
                             ['xtrabackup', '--backup',
                              '--target-dir', 'test_dir/test',
                              '--datadir', '/srv/sqldata',
                              '--socket', '/run/mysqld/mysqld.sock'])

    def test_errors_on_metadata(self):
        """test check of errors on the xtrabackup_info file"""
        mb = self.maria_backup

        # correct run
        sample_xtrabackup_info = """
            uuid = a6336c86-1b33-11e8-8de6-080027f3a5d8
            tool_name = xtrabackup
            tool_command = –backup –target-dir=/Backups/base –datadir=/var/lib/mysql –user=root –password=… –galera-info
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 5.7.19-17-57-log
            start_time = 2022-02-26 15:28:22
            end_time = 2022-02-26 15:28:48
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertFalse(mb.errors_on_metadata(''))

        # missing end_time
        sample_xtrabackup_info = """
            uuid = a6336c86-1b33-11e8-8de6-080027f3a5d8
            tool_name = xtrabackup
            tool_command = –backup –target-dir=/Backups/base –datadir=/var/lib/mysql –user=root –password=… –galera-info
            tool_version = 2.4.9
            ibbackup_version = 2.4.9
            server_version = 5.7.19-17-57-log
            start_time = 2022-02-26 15:28:22
        """
        mock = mock_open(read_data=sample_xtrabackup_info)
        with patch('builtins.open', mock):
            self.assertTrue(mb.errors_on_metadata(''))

        # simulate open error
        mock = mock_open(read_data="")
        with patch('builtins.open', mock) as mocked_open:
            mocked_open.side_effect = OSError
            self.assertTrue(mb.errors_on_metadata(''))

        # simulate read error
        mock = mock_open(read_data="")
        with patch('builtins.open', mock) as mocked_open:
            mocked_file = mocked_open.return_value
            mocked_file.read.side_effect = IOError
            self.assertTrue(mb.errors_on_metadata(''))

    def test__get_xtraback_prepare_cmd(self):
        """test building the prepare command"""
        mb = self.maria_backup
        self.assertEqual(mb._get_xtraback_prepare_cmd('/a/dir'), ['xtrabackup', '--prepare',
                                                                  '--target-dir', '/a/dir/test',
                                                                  '--use-memory', '40G',
                                                                  '--open-files-limit', '200000'])

    def test_errors_on_output(self):
        """Check errors on standard output & standard error after backup"""
        mb = self.maria_backup

        # normal run
        errors = b"""
        220309 11:19:09 Finished backing up non-InnoDB tables and files
        220309 11:19:09 Executing FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS...
        xtrabackup: The latest check point (for incremental): '92134324'
        xtrabackup: Stopping log copying thread..
        220309 11:19:10 >> log scanned up to (900123121)
        220309 11:19:10 Executing UNLOCK TABLES
        220309 11:19:10 All tables unlocked
        220309 11:19:10 Backup created in directory '/a/dir'
        220309 11:19:10 [00] Writing backup-my.cnf
        220309 11:19:10 [00]        ...done
        220309 11:19:10 [00] Writing xtrabackup_info
        220309 11:19:10 [00]        ...done
        xtrabackup: Transaction log of lsn (89423125) to (900123121) was copied.
        220309 11:19:10 completed OK!
        """
        self.assertFalse(mb.errors_on_output(b'', errors))

        # failed run
        errors = b"""
        xtrabackup: error: log block numbers mismatch:
        xtrabackup: error: expected log block no. 293842034, but got no. 13324598 from the log file.
        xtrabackup: error: it looks like InnoDB log has wrapped around before xtrabackup
        could process all records due to either log copying being too slow, or log files being too small.
        xtrabackup: Error: xtrabackup_copy_logfile() failed
        """
        self.assertTrue(mb.errors_on_output(b'', errors))

    def test_errors_on_log(self):
        """Test errors on disk-based logs"""
        mb = self.maria_backup
        self.assertFalse(mb.errors_on_log())  # at the moment, xtrabackup execution does not generate disk logs

    def test_get_prepare_cmd(self):
        """Test version check and return of the prepare command"""
        mb = self.maria_backup
        prepare_cmd = ['prepare']

        # same exact version
        xtrabackup_version = {'major': '10.1', 'minor': 16, 'vendor': 'MariaDB'}
        backup_version = {'major': '10.1', 'minor': 16, 'vendor': 'MariaDB'}
        with patch.object(MariaBackup, '_get_xtrabackup_version', return_value=xtrabackup_version), \
                patch.object(MariaBackup, '_get_backup_source_server_version', return_value=backup_version), \
                patch.object(MariaBackup, '_get_xtraback_prepare_cmd', return_value=prepare_cmd):
            self.assertEqual(mb.get_prepare_cmd('/a/dir'), prepare_cmd)

        # different vendor (not allowed)
        xtrabackup_version = {'major': '10.1', 'minor': 16, 'vendor': 'MariaDB'}
        backup_version = {'major': '10.1', 'minor': 16, 'vendor': None}
        with patch.object(MariaBackup, '_get_xtrabackup_version', return_value=xtrabackup_version), \
                patch.object(MariaBackup, '_get_backup_source_server_version', return_value=backup_version), \
                patch.object(MariaBackup, '_get_xtraback_prepare_cmd', return_value=prepare_cmd):
            self.assertRaises(XtrabackupError, mb.get_prepare_cmd, '/a/dir')

        # xtrabackup has a higher minor version (allowed)
        xtrabackup_version = {'major': '10.1', 'minor': 17, 'vendor': 'MariaDB'}
        backup_version = {'major': '10.1', 'minor': 16, 'vendor': 'MariaDB'}
        with patch.object(MariaBackup, '_get_xtrabackup_version', return_value=xtrabackup_version), \
                patch.object(MariaBackup, '_get_backup_source_server_version', return_value=backup_version), \
                patch.object(MariaBackup, '_get_xtraback_prepare_cmd', return_value=prepare_cmd):
            self.assertEqual(mb.get_prepare_cmd('/a/dir'), prepare_cmd)

        # backup has a higher minor version (not allowed)
        xtrabackup_version = {'major': '10.1', 'minor': 3, 'vendor': 'MariaDB'}
        backup_version = {'major': '10.1', 'minor': 22, 'vendor': 'MariaDB'}
        with patch.object(MariaBackup, '_get_xtrabackup_version', return_value=xtrabackup_version), \
                patch.object(MariaBackup, '_get_backup_source_server_version', return_value=backup_version), \
                patch.object(MariaBackup, '_get_xtraback_prepare_cmd', return_value=prepare_cmd):
            self.assertRaises(XtrabackupError, mb.get_prepare_cmd, '/a/dir')

        # different major versions (not allowed)
        xtrabackup_version = {'major': '10.1', 'minor': 22, 'vendor': 'MariaDB'}
        backup_version = {'major': '10.4', 'minor': 22, 'vendor': 'MariaDB'}
        with patch.object(MariaBackup, '_get_xtrabackup_version', return_value=xtrabackup_version), \
                patch.object(MariaBackup, '_get_backup_source_server_version', return_value=backup_version), \
                patch.object(MariaBackup, '_get_xtraback_prepare_cmd', return_value=prepare_cmd):
            self.assertRaises(XtrabackupError, mb.get_prepare_cmd, '/a/dir')


if __name__ == "__main__":
    unittest.main()
