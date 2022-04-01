import wmfbackups.check.check_mariadb_backups as check

from freezegun import freeze_time
from freezegun.api import FakeDatetime

import datetime
import unittest
from unittest.mock import MagicMock, mock_open, patch


class MockOptions:
    section = 'g1'
    valid_sections = ['g1', 'g4', 'rex5']
    datacenter = 'eqiad'
    type = 'snapshot'
    freshness = 100000
    crit_size_percentage = 30
    warn_size_percentage = 25
    min_size = 10000


class TestCheckMariaDBBackups(unittest.TestCase):

    test_data = [{'id': 2, 'name': 'snapshot.t1.2022-01-01--00-00-01', 'status': 'finished',
                  'source': 'db1001.eqiad.wmnet:3301', 'host': 'dbprov1001.eqiad.wmnet',
                  'type': 'snapshot', 'section': 't1',
                  'start_date': datetime.datetime(2022, 1, 2, 0, 0, 1),
                  'end_date': datetime.datetime(2022, 1, 1, 2, 16, 00), 'total_size': 12000},
                 {'id': 1, 'name': 'snapshot.t1.2022-01-01--00-00-01', 'status': 'finished',
                  'source': 'db1001.eqiad.wmnet:3301', 'host': 'dbprov1001.eqiad.wmnet',
                  'type': 'snapshot', 'section': 't1',
                  'start_date': datetime.datetime(2022, 1, 1, 0, 0, 1),
                  'end_date': datetime.datetime(2022, 1, 1, 2, 15, 00), 'total_size': 10000}]

    def setUp(self):
        """Set up the tests."""
        pass

    def test_format_size(self):
        """Test instancing"""
        self.assertEqual(check.format_size(0), '0 B')
        self.assertEqual(check.format_size(1), '1 B')
        self.assertEqual(check.format_size(900), '0.9 KiB')
        self.assertEqual(check.format_size(1024), '1.0 KiB')
        self.assertEqual(check.format_size(1935), '1.9 KiB')
        self.assertEqual(check.format_size(10 * 1024), '10 KiB')
        self.assertEqual(check.format_size(1024 * 1024 * 0.9), '0.9 MiB')
        self.assertEqual(check.format_size(1024 * 1024 * 128), '128 MiB')
        self.assertEqual(check.format_size(1024 * 1024 * 1024 * 0.9), '0.9 GiB')
        self.assertEqual(check.format_size(1024 * 1024 * 1024 * 128), '128 GiB')
        self.assertEqual(check.format_size(1024 * 1024 * 1024 * 1024 * 7), '7168 GiB')

    def test_get_valid_sections(self):
        mock = mock_open(read_data="a1\nb2\nc3")
        with patch('builtins.open', mock):
            self.assertEqual(check.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="a1\n\n\nb2\nc3\n")
        with patch('builtins.open', mock):
            self.assertEqual(check.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="   \na1 \n  b2\n c3  \n \n")
        with patch('builtins.open', mock):
            self.assertEqual(check.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="")
        self.assertRaises(check.BadConfigException, check.get_valid_sections)
        mock = mock_open(read_data="\n \n")
        self.assertRaises(check.BadConfigException, check.get_valid_sections)
        mock = mock_open(read_data="a1\nb2\nc3")
        mock_open.side_effect = IOError()
        self.assertRaises(check.BadConfigException, check.get_valid_sections)

    @freeze_time('2022-01-03')
    def test_validate_input(self):
        options = MockOptions()
        options.section = 't1'
        self.assertRaises(check.BadSectionException, check.validate_input, options)
        options.section = 'g1'
        options.datacenter = 'drmrs'
        self.assertRaises(check.BadDatacenterException, check.validate_input, options)
        options.datacenter = 'eqiad'
        options.type = 'warm-backup'
        self.assertRaises(check.BadTypeException, check.validate_input, options)
        options.type = 'snapshot'
        self.assertEqual(check.validate_input(options),
                         ('g1', 'eqiad', 'snapshot', FakeDatetime(2022, 1, 1, 20, 13, 20),
                          'a day ago', 30, 25, 10000, '10 KiB'))

    def test_format_percentage(self):
        self.assertEqual(check.format_percentage(1), '+1.0 %')
        self.assertEqual(check.format_percentage(-0.11), '-0.1 %')
        self.assertEqual(check.format_percentage(45), '+45.0 %')
        self.assertEqual(check.format_percentage(0), '+0.0 %')

    def test_process_previous_backup_data(self):
        data = self.test_data
        self.assertEqual(check.process_previous_backup_data(12000, data),
                         (10000, '10 KiB', 20, '+20.0 %'))
        self.assertEqual(check.process_previous_backup_data(10000, [data[1], data[0]]),
                         (12000, '12 KiB', -16.666666666666664, '-16.7 %'))

    @freeze_time('2022-01-03')
    def test_check_backup_database(self):
        mock = MagicMock(return_value=self.test_data)
        with patch('wmfbackups.check.check_mariadb_backups.query_metadata_database', mock):
            options = MockOptions()
            # valid backup
            self.assertEqual(check.check_backup_database(options),
                             (0, 'Last snapshot for g1 at eqiad (db1001) taken on 2022-01-02 00:00:01 (12 KiB, +20.0 %)'))
            # warning because size change
            options.warn_size_percentage = 15
            self.assertEqual(check.check_backup_database(options),
                             (1, 'Last snapshot for g1 at eqiad (db1001) taken on '
                                 '2022-01-02 00:00:01 is 12 KiB, but the previous one was 10 KiB, '
                                 'a change of +20.0 %'))
            # critical because size change
            options.crit_size_percentage = 10
            self.assertEqual(check.check_backup_database(options),
                             (2, 'Last snapshot for g1 at eqiad (db1001) taken on '
                                 '2022-01-02 00:00:01 is 12 KiB, but the previous one was 10 KiB, '
                                 'a change of +20.0 %'))
            options = MockOptions()
            # old last backup
            options.freshness = 3600
            self.assertEqual(check.check_backup_database(options),
                             (2, 'snapshot for g1 at eqiad (db1001) taken more than an hour ago: '
                                 'Most recent backup 2022-01-02 00:00:01'))
            # backup too small
            options = MockOptions()
            options.min_size = 30000
            self.assertEqual(check.check_backup_database(options),
                             (2, 'snapshot for g1 at eqiad (db1001, 2022-01-02 00:00:01): '
                                 '12 KiB is less than 29 KiB'))
        # No backups found
        mock = MagicMock(return_value=[])
        with patch('wmfbackups.check.check_mariadb_backups.query_metadata_database', mock):
            options = MockOptions()
            self.assertEqual(check.check_backup_database(options),
                             (2, 'We could not find any completed snapshot for g1 at eqiad'))
        # Only 1 backup found
        mock = MagicMock(return_value=[self.test_data[0]])
        with patch('wmfbackups.check.check_mariadb_backups.query_metadata_database', mock):
            options = MockOptions()
            self.assertEqual(check.check_backup_database(options),
                             (1, 'There is only 1 snapshot for g1 at eqiad (db1001) '
                                 'taken on 2022-01-02 00:00:01 (12 KiB)'))


if __name__ == "__main__":
    unittest.main()
