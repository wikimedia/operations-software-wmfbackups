from wmfbackups.WMFBackup import WMFBackup

import unittest


class TestWMFBackup(unittest.TestCase):

    def setUp(self):
        """Set up the tests."""
        pass

    def test_WMFBackup(self):
        """Test instancing"""
        backup = WMFBackup('test', {'type': 'null'})
        self.assertEqual(backup.config['type'], 'null')
        self.assertEqual(backup.run(), 0)


if __name__ == "__main__":
    unittest.main()
