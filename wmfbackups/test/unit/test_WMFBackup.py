import unittest
from unittest.mock import patch

from freezegun import freeze_time

from wmfbackups.WMFBackup import WMFBackup


class TestWMFBackup(unittest.TestCase):

    def setUp(self):
        """Set up the tests."""
        self.backup = WMFBackup('test', {'type': 'null'})

    def test_properties(self):
        """Test basic properties"""
        b = self.backup
        self.assertEqual(b.default_ongoing_backup_dir, '/srv/backups/nulls/ongoing')
        self.assertEqual(b.default_final_backup_dir, '/srv/backups/nulls/latest')
        self.assertEqual(b.default_archive_backup_dir, '/srv/backups/nulls/archive')
        self.assertEqual(b.name_regex, r'null\.([a-z0-9\-_]+)\.(20\d\d-[01]\d-[0123]\d\--\d\d-\d\d-\d\d)(\.[a-z0-9\.]+)?')

    @freeze_time('2022-01-03')
    def test_generate_file_name(self):
        """test backup name generation"""
        # uncompressed
        b = self.backup
        b.generate_file_name('/srv/backups/nulls/ongoing')
        self.assertEqual(b.dir_name, 'null.test.2022-01-03--00-00-00')
        self.assertEqual(b.file_name, 'null.test.2022-01-03--00-00-00')
        self.assertEqual(b.log_file, '/srv/backups/nulls/ongoing/null_log.test')

        # compressed
        b2 = WMFBackup('test', {'type': 'null', 'compress': True})
        b2.generate_file_name('/srv/backups/nulls/ongoing')
        self.assertEqual(b2.file_name, 'null.test.2022-01-03--00-00-00.tar.gz')

    @patch('os.path.isdir')
    def test_parse_backup_file(self, mock):
        """Test the parser for absolute paths """
        # not a dir
        b = WMFBackup('/a/dir/null.test.2022-01-02--00-00-00', {'type': 'null'})
        mock.return_value = False
        self.assertEqual(b.parse_backup_file(), None)

        # basic run
        mock.return_value = True
        self.assertEqual(b.parse_backup_file(), 0)
        self.assertEqual(b.name, 'test')
        self.assertEqual(b.dir_name, 'null.test.2022-01-02--00-00-00')
        self.assertEqual(b.file_name, 'null.test.2022-01-02--00-00-00')
        self.assertEqual(b.log_file, '/a/dir/null_log.test')

        # trailing /
        b = WMFBackup('/another/dir/null.test.2022-01-01--11-11-11/', {'type': 'null'})
        mock.return_value = True
        self.assertEqual(b.parse_backup_file(), 0)
        self.assertEqual(b.dir_name, 'null.test.2022-01-01--11-11-11')
        self.assertEqual(b.log_file, '/another/dir/null_log.test')

        # unmatching type & dir
        b = WMFBackup('/another/dir/null.test.2022-01-01--11-11-11/', {'type': 'dump'})
        mock.return_value = True
        self.assertEqual(b.parse_backup_file(), None)

        # malformed final dir
        b = WMFBackup('/another/dir/null.test.2022-01-01 11:11:11', {'type': 'null'})
        mock.return_value = True
        self.assertEqual(b.parse_backup_file(), None)

    @patch('os.listdir')
    def test_find_backup_file(self, mock):
        """test with default paths"""
        b = self.backup

        # basic run
        mock.return_value = [
            'dump.test.2022-01-01--00-00-00',
            'null.another_section.2022-01-02--00-00-00',
            'null.test.2022-01-02--00-00-00',
            'garbage',
            'null_log.test',
            '.dotfile'
            ]
        self.assertEqual(b.find_backup_file('/a/dir'), 0)
        self.assertEqual(b.dir_name, 'null.test.2022-01-02--00-00-00')
        self.assertEqual(b.file_name, 'null.test.2022-01-02--00-00-00')
        self.assertEqual(b.log_file, '/a/dir/null_log.test')

        # no good backup candidates
        mock.return_value = [
            'dump.test.2022-01-01--00-00-00',
            'null.another_section.2022-01-02--00-00-00',
            'garbage',
            'null_log.test',
            '.dotfile'
        ]
        self.assertEqual(b.find_backup_file('/a/dir'), None)

        # too many backup candidates
        mock.return_value = [
            'null.test.2022-01-01--00-00-00',
            'null.test.2022-01-02--00-00-00',
            'garbage'
        ]
        self.assertEqual(b.find_backup_file('/a/dir'), None)

    def test_run(self):
        """Test run"""
        b = self.backup
        self.assertEqual(b.run(), 0)


if __name__ == "__main__":
    unittest.main()
