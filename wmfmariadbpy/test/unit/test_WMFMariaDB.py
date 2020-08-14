from wmfmariadbpy.WMFMariaDB import WMFMariaDB

import unittest
import unittest.mock as um


def mock_open(csvdata, *args, **kargs):
    m_o = um.mock_open(*args, **kargs)
    m_o.return_value.__iter__ = lambda self: iter(csvdata)
    return m_o


class TestWMFMariaDB(unittest.TestCase):
    def setUp(self):
        """Set up the tests."""
        self.test_csv_data = ("s1, 1", "s2, 2", "m3, 100")

    def test_read_section_ports_list(self):
        with um.patch("builtins.open", mock_open(self.test_csv_data)):
            sections, ports = WMFMariaDB.read_section_ports_list()
            self.assertEqual(sections[2], "s2")
            self.assertEqual(ports["m3"], 100)

    def test_get_port_from_section(self):
        test_cases = [("m3", 100), ("nonexistent", 3306), (None, 3306)]
        with um.patch("builtins.open", mock_open(self.test_csv_data)):
            for case in test_cases:
                self.assertEqual(WMFMariaDB.get_port_from_section(case[0]), case[1])

    def test_get_section_from_port(self):
        test_cases = [(2, "s2"), (100, "m3")]
        null_cases = [655359, -1, 0, None]
        with um.patch("builtins.open", mock_open(self.test_csv_data)):
            for case in test_cases:
                self.assertEqual(WMFMariaDB.get_section_from_port(case[0]), case[1])
            for case in null_cases:
                self.assertIsNone(WMFMariaDB.get_section_from_port(case))

    def test_get_datadir_from_port(self):
        test_cases = [
            (1, "/srv/sqldata.s1"),
            (100, "/srv/sqldata.m3"),
            (101, "/srv/sqldata"),
            (None, "/srv/sqldata"),
        ]
        with um.patch("builtins.open", mock_open(self.test_csv_data)):
            for case in test_cases:
                self.assertEqual(WMFMariaDB.get_datadir_from_port(case[0]), case[1])

    def test_get_socket_from_port(self):
        test_cases = [
            (2, "/run/mysqld/mysqld.s2.sock"),
            (101, "/run/mysqld/mysqld.sock"),
            (None, "/run/mysqld/mysqld.sock"),
        ]
        with um.patch("builtins.open", mock_open(self.test_csv_data)):
            for case in test_cases:
                self.assertEqual(WMFMariaDB.get_socket_from_port(case[0]), case[1])

    def test_resolve(self):
        test_cases = [
            ("localhost", ("localhost", 3306)),
            ("localhost:3311", ("localhost", 3311)),
            ("db1001", ("db1001.eqiad.wmnet", 3306)),
            ("db5999:3321", ("db5999.eqsin.wmnet", 3321)),
            ("db2001.codfw.wmnet", ("db2001.codfw.wmnet", 3306)),
            ("dbmonitor1001.wikimedia.org", ("dbmonitor1001.wikimedia.org", 3306)),
        ]
        for test in test_cases:
            self.assertEqual(WMFMariaDB.resolve(test[0]), test[1])


if __name__ == "__main__":
    unittest.main()
