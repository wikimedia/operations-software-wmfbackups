"""Integration tests for OnlineSchemaChanger class."""
import unittest
from unittest.mock import MagicMock

from wmfmariadbpy.cli_admin.osc_host import OnlineSchemaChanger
from wmfmariadbpy.WMFMariaDB import WMFMariaDB


class TestOnlineSchemaChanger(unittest.TestCase):
    """Test cases for OnlineSchemaChanger."""
    DB_NAME = "osc_test_db"
    TABLE_NAME = "osc_test_table"
    COL_NAME = "osc_test_column"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conf = MagicMock()
        self.conf.host = 'localhost'
        self.conf.port = 3306
        self.conf.user = 'root'
        self.conf.debug = True
        self.conf.dblist = [self.DB_NAME]
        self.conf.table = self.TABLE_NAME
        self.conf.gtid_domain_id = None

    def _create_test_db_and_table(self):
        res = self._conn.execute("create database {}".format(self.DB_NAME))
        self.assertTrue(res.get('success'))

        self.addCleanup(self._drop_test_db)

        self._conn.change_database(self.DB_NAME)
        create_sql = "create table {} (test_col int primary key)".format(self.TABLE_NAME)
        res = self._conn.execute(create_sql)
        self.assertTrue(res.get('success'))

    def _drop_test_db(self):
        res = self._conn.execute("drop database {}".format(self.DB_NAME))
        self.assertTrue(res.get('success'))

    def setUp(self):
        self._conn = WMFMariaDB(host=self.conf.host,
                                port=self.conf.port,
                                debug=self.conf.debug)
        self.addCleanup(self._conn.disconnect)
        self._create_test_db_and_table()

        self.osc = OnlineSchemaChanger(self.conf)
        self.osc.confirm = MagicMock()

    def test_ddl(self):
        for method in ['ddl', 'ddlonline', 'percona']:
            self.conf.method = method

            self.conf.altersql = "add column {} int".format(self.COL_NAME)

            self.osc.run()

            add_sql = "alter table {} add column {} int".format(self.TABLE_NAME, self.COL_NAME)
            res = self._conn.execute(add_sql)
            self.assertFalse(res.get('success'))
            self.assertEqual(1060, res.get('errno'))

            drop_sql = "alter table {} drop column {}".format(self.TABLE_NAME, self.COL_NAME)
            res = self._conn.execute(drop_sql)
            self.assertTrue(res.get('success'))
