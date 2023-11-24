import datetime

import pymysql
from unittest import mock, TestCase
from unittest.mock import mock_open, patch

import wmfbackups.WMFMetrics as WMFMetrics


class MockOptions:
    user = 'root'
    password = 'abc'
    host = 'localhost'
    database = 'my_database'
    type = 'my_backup_type'
    section = 'my_section'
    datacenter = 'my_dc'


class TestWMFMetrics(TestCase):

    def setUp(self):
        """Set up the tests."""
        pass

    def test_get_valid_sections(self):
        options = MockOptions()
        metrics = WMFMetrics.WMFMetrics(options)
        mock = mock_open(read_data="a1\nb2\nc3")
        with patch('builtins.open', mock):
            self.assertEqual(metrics.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="a1\n\n\nb2\nc3\n")
        with patch('builtins.open', mock):
            self.assertEqual(metrics.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="   \na1 \n  b2\n c3  \n \n")
        with patch('builtins.open', mock):
            self.assertEqual(metrics.get_valid_sections(), ['a1', 'b2', 'c3'])
        mock = mock_open(read_data="")
        self.assertRaises(WMFMetrics.BadConfigException, metrics.get_valid_sections)
        mock = mock_open(read_data="\n \n")
        self.assertRaises(WMFMetrics.BadConfigException, metrics.get_valid_sections)
        mock = mock_open(read_data="a1\nb2\nc3")
        mock_open.side_effect = IOError()
        self.assertRaises(WMFMetrics.BadConfigException, metrics.get_valid_sections)

    @patch('wmfbackups.WMFMetrics.pymysql')
    def test_query_metadata_database(self, mock_pymysql):
        """Test querying the database"""
        # expected normal behaviour
        options = MockOptions()
        mock_cursor = mock.MagicMock()
        test_data = [
            {'id': 1, 'name': 'dump.s2.2023-11-07--11-14-53', 'status': 'finished', 'source': 'db1001.eqiad.wmnet',
             'host': 'dbprov3003.eqiad.wmnet', 'type': 'dump', 'section': 's2', 'start_date': datetime.datetime(2023, 11, 7, 11, 14, 54, 453223),
             'end_date': datetime.datetime(2023, 11, 7, 11, 16, 32, 453223), 'total_size': 123452353324},
            {'id': 1, 'name': 'dump.s2.2023-10-30--11-14-53', 'status': 'finished', 'source': 'db1001.eqiad.wmnet',
             'host': 'dbprov3003.eqiad.wmnet', 'type': 'dump', 'section': 's2', 'start_date': datetime.datetime(2023, 10, 30, 11, 14, 54, 453223),
             'end_date': datetime.datetime(2023, 10, 30, 11, 16, 32, 453223), 'total_size': 113452353324}
        ]
        mock_cursor.fetchall.return_value = test_data
        mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        metrics = WMFMetrics.WMFMetrics(options)
        self.assertEqual(test_data, metrics.query_metadata_database(options))

        # on connection error
        mock_pymysql.connect.side_effect = pymysql.err.OperationalError()
        mock_pymysql.err = type('PymysqlError', (Exception,),
                                {'OperationalError': Exception, 'InternalError': Exception, 'ProgrammingError': Exception})
        with self.assertRaises(WMFMetrics.DatabaseConnectionException):
            metrics.query_metadata_database(options)

        mock_pymysql.connect.side_effect = pymysql.err.InternalError()
        with self.assertRaises(WMFMetrics.DatabaseConnectionException):
            metrics.query_metadata_database(options)

        # on query error
        mock_pymysql.connect.side_effect = None
        mock_cursor.execute.side_effect = pymysql.err.ProgrammingError()
        mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        with self.assertRaises(WMFMetrics.DatabaseQueryException):
            metrics.query_metadata_database(options)

        mock_cursor.execute.side_effect = pymysql.err.InternalError()
        mock_pymysql.connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        with self.assertRaises(WMFMetrics.DatabaseQueryException):
            metrics.query_metadata_database(options)
        mock_cursor.execute.side_effect = None
