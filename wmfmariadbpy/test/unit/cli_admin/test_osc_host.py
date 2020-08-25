"""Tests for OnlineSchemaChanger class."""
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, PropertyMock, call, patch

from wmfmariadbpy.cli_admin.osc_host import OnlineSchemaChanger, parse_args
from wmfmariadbpy.test.utils import hide_stderr


class TestOnlineSchemaChanger(unittest.TestCase):
    """Test cases for OnlineSchemaChanger."""

    def setUp(self):
        """Set up the tests."""
        self.conf = MagicMock()
        self.conf.host = "host"
        self.conf.port = 1234
        self.conf.debug = False
        self.osc = OnlineSchemaChanger(self.conf)

    def test_destructor(self):
        """Test destructor."""
        connection = MagicMock()
        self.osc._conn = connection

        self.osc.__del__()

        self.assertEqual(None, self.osc._conn)

        self.osc.__del__()

        connection.disconnect.assert_called_once_with()

    @patch("wmfmariadbpy.cli_admin.osc_host.WMFMariaDB")
    def test_connection(self, mock):
        """Test connection creation."""
        new_connection = MagicMock()
        mock.return_value = new_connection

        self.assertEqual(None, self.osc._conn)
        self.assertEqual(new_connection, self.osc.connection)
        self.assertEqual(new_connection, self.osc._conn)
        self.assertEqual(new_connection, self.osc.connection)

        mock.assert_called_once_with(
            host=self.conf.host, port=self.conf.port, debug=self.conf.debug
        )

    @patch("wmfmariadbpy.cli_admin.osc_host.WMFMariaDB")
    @patch("builtins.print")
    @patch("sys.exit")
    def test_failed_connection(self, exit_mock, print_mock, db_mock):
        """Test failed connection creation."""
        new_connection = MagicMock()
        new_connection.connection = None
        db_mock.return_value = new_connection

        self.osc.connection

        expected_error = "Connect failed: {}@{}:{}".format(
            self.conf.user, self.conf.host, self.conf.port
        )
        print_mock.assert_called_once_with(expected_error)
        exit_mock.assert_called_once_with(1)

    def test_ddl_rep(self):
        """Test default ddl replication args."""
        self.conf.replicate = False
        self.conf.no_replicate = False
        self.conf.gtid_domain_id = None

        self.assertEqual([], self.osc.ddl_rep)
        self.assertEqual([], self.osc._ddlrep)

    def test_ddl_rep_replicate(self):
        """Test ddl replication args with replicate option."""
        self.conf.replicate = True
        self.conf.no_replicate = False
        gtid = 0
        self.conf.gtid_domain_id = gtid

        ddlrp = self.osc.ddl_rep
        self.assertEqual(ddlrp, self.osc._ddlrep)
        self.assertIn("set session sql_log_bin=1;", ddlrp)
        self.assertIn("set session gtid_domain_id = {};".format(gtid), ddlrp)

    def test_ddl_rep_no_replicate(self):
        """Test ddl replication args with no-replicate option."""
        self.conf.replicate = False
        self.conf.no_replicate = True
        gtid = 1234
        self.conf.gtid_domain_id = gtid

        ddlrp = self.osc.ddl_rep
        self.assertEqual(ddlrp, self.osc._ddlrep)
        self.assertIn("set session sql_log_bin=0;", ddlrp)
        self.assertIn("set session gtid_domain_id = {};".format(gtid), ddlrp)

    def test_ddl_args(self):
        """Test ddl argument construction."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.ddl_rep",
            new_callable=PropertyMock,
        ) as mock_ddlrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ddlrep.return_value = rep_args
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ddlargs)
            ddlargs = mocked_osc.ddl_args
            self.assertEqual(2 + len(rep_args), len(ddlargs))
            for arg in rep_args:
                self.assertIn(arg, ddlargs)
            self.assertListEqual(ddlargs, mocked_osc._ddlargs)
            mock_ddlrep.assert_called_once_with()

    def test_pt_ost_rep(self):
        """Test default percona replication args."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        existing_connection.execute.return_value = {}

        self.conf.replicate = False
        self.conf.no_replicate = False

        expected_ptrep = ["--recurse=0"]
        self.assertEqual([], self.osc._ptrep)
        self.assertListEqual(expected_ptrep, self.osc.pt_osc_rep)
        self.assertEqual(expected_ptrep, self.osc._ptrep)
        self.assertListEqual(expected_ptrep, self.osc.pt_osc_rep)

        existing_connection.execute.assert_called_once_with("show slave status")

    def test_pt_ost_rep_replicate(self):
        """Test percona replication args with replicate option."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        existing_connection.execute.return_value = {}

        self.conf.replicate = True
        self.conf.no_replicate = False

        ptrep = self.osc.pt_osc_rep
        self.assertIn("--recurse=1", ptrep)
        self.assertNotIn("--recurse=0", ptrep)
        self.assertIn("--chunk-size-limit=10", ptrep)

    def test_pt_ost_rep_no_replicate(self):
        """Test percona replication args with no-replicate option."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        existing_connection.execute.return_value = {}

        self.conf.replicate = False
        self.conf.no_replicate = True

        ptrep = self.osc.pt_osc_rep
        self.assertIn("--recurse=0", ptrep)
        self.assertNotIn("--recurse=1", ptrep)
        self.assertIn("--set-vars=sql_log_bin=off", ptrep)

    def test_pt_ost_rep_slave(self):
        """Test default percona replication args for a slave."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        existing_connection.execute.return_value = {"numrows": 1}

        self.conf.replicate = False
        self.conf.no_replicate = False

        expected_ptrep = ["--recurse=0", "--check-slave-lag={}".format(self.conf.host)]
        self.assertEqual([], self.osc._ptrep)
        self.assertListEqual(expected_ptrep, self.osc.pt_osc_rep)
        self.assertEqual(expected_ptrep, self.osc._ptrep)
        self.assertListEqual(expected_ptrep, self.osc.pt_osc_rep)

        existing_connection.execute.assert_called_once_with("show slave status")

    def test_pt_osc_dry_run_args(self):
        """Test default Percona Toolkit OSC args for the dry run."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_rep",
            new_callable=PropertyMock,
        ) as mock_ptrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ptrep.return_value = rep_args
            self.conf.primary_key = False
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ptdrargs)
            ptdrargs = mocked_osc.pt_osc_dry_run_args
            self.assertEqual(len(rep_args), len(ptdrargs))
            for arg in rep_args:
                self.assertIn(arg, ptdrargs)
            self.assertListEqual(ptdrargs, mocked_osc._ptdrargs)
            mock_ptrep.assert_called_once_with()

    def test_pt_osc_dry_run_args_primery_key(self):
        """Test Percona Toolkit OSC args for the dry run with primary-key option."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_rep",
            new_callable=PropertyMock,
        ) as mock_ptrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ptrep.return_value = rep_args
            self.conf.primary_key = True
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ptdrargs)
            ptdrargs = mocked_osc.pt_osc_dry_run_args
            self.assertEqual(1 + len(rep_args), len(ptdrargs))
            self.assertIn("--no-check-alter", ptdrargs)
            for arg in rep_args:
                self.assertIn(arg, ptdrargs)
            self.assertListEqual(ptdrargs, mocked_osc._ptdrargs)
            mock_ptrep.assert_called_once_with()

    def test_pt_osc_args(self):
        """Test default Percona Toolkit OSC args."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_rep",
            new_callable=PropertyMock,
        ) as mock_ptrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ptrep.return_value = rep_args
            self.conf.primary_key = False
            self.conf.no_cleanup = False
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ptargs)
            ptargs = mocked_osc.pt_osc_args
            self.assertEqual(len(rep_args), len(ptargs))
            for arg in rep_args:
                self.assertIn(arg, ptargs)
            self.assertListEqual(ptargs, mocked_osc._ptargs)
            mock_ptrep.assert_called_once_with()

    def test_pt_osc_args_primery_key(self):
        """Test Percona Toolkit OSC args with primary-key option."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_rep",
            new_callable=PropertyMock,
        ) as mock_ptrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ptrep.return_value = rep_args
            self.conf.primary_key = True
            self.conf.no_cleanup = False
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ptargs)
            ptargs = mocked_osc.pt_osc_args
            self.assertEqual(1 + len(rep_args), len(ptargs))
            self.assertIn("--no-check-alter", ptargs)
            for arg in rep_args:
                self.assertIn(arg, ptargs)
            self.assertListEqual(ptargs, mocked_osc._ptargs)
            mock_ptrep.assert_called_once_with()

    def test_pt_osc_args_no_cleanup(self):
        """Test Percona Toolkit OSC args with no-cleanup option."""
        with patch(
            "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_rep",
            new_callable=PropertyMock,
        ) as mock_ptrep:
            rep_args = ["rep_arg1", "rep_arg2"]
            mock_ptrep.return_value = rep_args
            self.conf.primary_key = False
            self.conf.no_cleanup = True
            mocked_osc = OnlineSchemaChanger(self.conf)

            self.assertEqual([], mocked_osc._ptargs)
            ptargs = mocked_osc.pt_osc_args

            no_cleanup_args = [
                "--no-swap-tables",
                "--no-drop-new-table",
                "--no-drop-old-table",
                "--no-drop-triggers",
            ]
            self.assertEqual(len(no_cleanup_args) + len(rep_args), len(ptargs))
            for arg in no_cleanup_args:
                self.assertIn(arg, ptargs)
            for arg in rep_args:
                self.assertIn(arg, ptargs)
            self.assertListEqual(ptargs, mocked_osc._ptargs)
            mock_ptrep.assert_called_once_with()

    @patch("builtins.input", lambda x: "y")
    @patch("builtins.print")
    @patch("sys.exit")
    def test_confirm_yes(self, exit_mock, print_mock):
        """Afirmative confirmation."""
        self.osc.confirm()

        exit_mock.assert_not_called()
        print_mock.assert_not_called()

    @patch("builtins.input", lambda x: "n")
    @patch("builtins.print")
    @patch("sys.exit")
    def test_confirm_no(self, exit_mock, print_mock):
        """Negative confirmation."""
        self.osc.confirm()

        exit_mock.assert_called_once_with(0)
        print_mock.assert_called_once_with("abort")

    @patch("builtins.print")
    def test_execute(self, print_mock):
        """Test query execution."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        test_sql = "test sql"

        self.assertTrue(self.osc.execute(test_sql))

        print_mock.assert_called_once_with(test_sql)
        existing_connection.execute.assert_called_once_with(test_sql)

        args = ["args1", "args2"]
        self.assertTrue(self.osc.execute(test_sql, args))

        call_list = [call(a) for a in args]
        call_list.append(call(test_sql))
        print_mock.assert_has_calls(call_list)
        existing_connection.execute.assert_has_calls(call_list, True)

    @patch("builtins.print")
    def test_failed_execute(self, print_mock):
        """Test failed query execution."""
        existing_connection = MagicMock()
        self.osc._conn = existing_connection
        test_sql = "test sql"
        args = ["args1", "args2"]
        existing_connection.execute.return_value = {"success": False}

        self.assertFalse(self.osc.execute(test_sql, args))

        print_mock.assert_called_once_with(args[0])
        existing_connection.execute.assert_called_once_with(args[0])

    @patch("shutil.which")
    def test_osctool(self, which_mock):
        """Test osctool calls."""
        path = "osctool/path"
        which_mock.return_value = path

        self.assertEqual("", self.osc._osctool)
        self.assertEqual(path, self.osc.osctool)
        self.assertEqual(path, self.osc._osctool)
        self.assertEqual(path, self.osc.osctool)

        which_mock.assert_called_once_with("pt-online-schema-change")

    @patch("shutil.which")
    @patch("builtins.print")
    @patch("sys.exit")
    def test_no_osctool(self, exit_mock, print_mock, which_mock):
        """Test osctool calls with the script not availabe."""
        which_mock.return_value = None

        self.osc.osctool

        print_mock.assert_called_once_with(
            "Error: Could not find 'pt-online-schema-change'."
        )
        exit_mock.assert_called_once_with(1)

    @patch("subprocess.Popen")
    def test_run_command(self, popen_mock):
        """Test run_command method."""
        cmd = ["test", "--some-arg"]

        self.conf.debug = False
        popen_mock.return_value.wait.return_value = 0
        self.assertTrue(self.osc.run_command(cmd))

        self.conf.debug = True
        popen_mock.return_value.wait.return_value = 1
        self.assertFalse(self.osc.run_command(cmd))

        self.conf.debug = False
        popen_mock.return_value.wait.return_value = -1
        self.assertFalse(self.osc.run_command(cmd))

        call_list = [
            call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE),
            call(cmd, stdout=None, stderr=None),
            call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE),
        ]
        popen_mock.assert_has_calls(call_list, True)

    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_dry_run_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.osctool",
        new_callable=PropertyMock,
    )
    def test_run_pt_ost_alter(
        self, osctool_mock, pt_osc_args_mock, pt_osc_dr_args_mock
    ):
        """Test percona osc run."""
        self.osc.run_command = MagicMock(return_value=True)

        self.assertTrue(self.osc.run_pt_ost_alter("test_db"))
        actual_cmd = self.osc.run_command.call_args[0][0]
        self.assertIn("--execute", actual_cmd)
        self.assertNotIn("--dry-run", actual_cmd)
        pt_osc_args_mock.assert_called_once_with()
        pt_osc_dr_args_mock.assert_not_called()

    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_dry_run_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.osctool",
        new_callable=PropertyMock,
    )
    def test_run_pt_ost_alter_dry_run(
        self, osctool_mock, pt_osc_args_mock, pt_osc_dr_args_mock
    ):
        """Test percona osc dry run."""
        self.osc.run_command = MagicMock(return_value=True)

        self.assertTrue(self.osc.run_pt_ost_alter("test_db", dry_run=True))
        actual_cmd = self.osc.run_command.call_args[0][0]
        self.assertNotIn("--execute", actual_cmd)
        self.assertIn("--dry-run", actual_cmd)
        pt_osc_args_mock.assert_not_called()
        pt_osc_dr_args_mock.assert_called_once_with()

    def test_run_pt_cleanup(self):
        """Test db cleanup after a percona run."""
        self.osc.confirm = MagicMock()
        self.osc.execute = MagicMock()

        self.osc.run_pt_cleanup("test_db")

        self.osc.confirm.assert_called_once_with()
        self.assertEqual(4, self.osc.execute.call_count)

    @patch("builtins.print")
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_dry_run_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.pt_osc_args",
        new_callable=PropertyMock,
    )
    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.ddl_args",
        new_callable=PropertyMock,
    )
    def test_show_conf(
        self, ddl_args_mock, pt_osc_args_mock, pt_osc_dry_run_args_mock, print_mock
    ):
        """Test the methos used to show the conf."""
        self.osc.show_conf()

        self.assertEqual(10, print_mock.call_count)

    @patch("sys.exit")
    def test_change_database(self, exit_mock):
        """Test successful change of db."""
        connection = MagicMock()
        self.osc._conn = connection

        new_db = "test_db"
        connection.database = new_db

        self.osc.change_database(new_db)

        connection.change_database.assert_called_once_with(new_db)
        exit_mock.assert_not_called()

    @patch("sys.exit")
    def test_change_database_fail(self, exit_mock):
        """Test failed change of db."""
        connection = MagicMock()
        self.osc._conn = connection

        new_db = "test_db"
        self.osc.change_database(new_db)
        connection.change_database.assert_called_once_with(new_db)
        exit_mock.assert_called_once_with(1)

    def test_check_collision_with_collision(self):
        """Test check_collision with an existing collision."""
        self.osc.confirm = MagicMock()
        connection = MagicMock()
        self.osc._conn = connection

        connection.execute.return_value = {"numrows": 1}

        self.osc.check_collision()

        self.osc.confirm.assert_called_once_with()

    def test_check_collision_without_collision(self):
        """Test check_collision without any collision."""
        self.osc.confirm = MagicMock()
        connection = MagicMock()
        self.osc._conn = connection

        connection.execute.return_value = {"numrows": 0}

        self.osc.check_collision()

        self.osc.confirm.assert_not_called()

    def test_run_percona(self):
        """Test run_percona method."""
        self.osc.run_pt_ost_alter = MagicMock(return_value=True)
        self.osc.run_pt_cleanup = MagicMock()
        self.osc.execute = MagicMock()
        self.conf.no_cleanup = True
        self.conf.analyze = True
        self.osc._ddlrep = ["somehting"]

        db = "test_db"
        self.assertTrue(self.osc.run_percona(db))

        alter_call_list = (call(db, dry_run=True), call(db))
        self.osc.run_pt_ost_alter.assert_has_calls(alter_call_list)
        self.osc.run_pt_cleanup.assert_called_once_with(db)
        self.assertEqual(1, self.osc.execute.call_count)

    def test_run_percon_failed_dry_run(self):
        """Test run_percona method with failed dry-run."""
        self.osc.run_pt_ost_alter = MagicMock(return_value=False)

        self.assertFalse(self.osc.run_percona("test_db"))

        self.osc.run_pt_ost_alter.assert_called_once_with("test_db", dry_run=True)

    def test_run_percon_failed_run(self):
        """Test run_percona method with failed run."""
        self.osc.run_pt_ost_alter = MagicMock(side_effect=[True, False])

        db = "test_db"
        self.assertFalse(self.osc.run_percona(db))

        alter_call_list = (call(db, dry_run=True), call(db))
        self.osc.run_pt_ost_alter.assert_has_calls(alter_call_list)

    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.ddl_args",
        new_callable=PropertyMock,
    )
    def test_run_ddl_with_ddl_method(self, ddl_args_mock):
        """Test run_ddl with the ddl method."""
        self.osc.execute = MagicMock(return_value=True)
        table = "table"
        alter = "alter"
        self.conf.table = table
        self.conf.altersql = alter
        self.conf.method = "ddl"
        ddlargs = ["arg"]
        ddl_args_mock.return_value = ddlargs

        self.assertTrue(self.osc.run_ddl("test_db"))

        expected_query = "alter table `{}` {}".format(table, alter)
        self.osc.execute.assert_called_once_with(expected_query, ["arg"])

    @patch(
        "wmfmariadbpy.cli_admin.osc_host.OnlineSchemaChanger.ddl_args",
        new_callable=PropertyMock,
    )
    def test_run_ddl_with_ddlonline_method(self, ddl_args_mock):
        """Test run_ddl with the ddlonline method."""
        self.osc.execute = MagicMock(return_value=False)
        table = "table"
        alter = "alter"
        self.conf.table = table
        self.conf.altersql = alter
        self.conf.method = "ddlonline"
        ddlargs = ["arg"]
        ddl_args_mock.return_value = ddlargs

        self.assertFalse(self.osc.run_ddl("test_db"))

        expected_query = "alter online table `{}` {}".format(table, alter)
        self.osc.execute.assert_called_once_with(expected_query, ["arg"])

    def test_run_method_with_percona(self):
        """Test main run with the percona method."""
        self.osc.show_conf = MagicMock()
        self.osc.confirm = MagicMock()
        self.osc.change_database = MagicMock()
        self.osc.check_collision = MagicMock()
        self.osc.run_percona = MagicMock()
        self.osc.run_ddl = MagicMock()
        self.conf.dblist = ["test_db1", "test_db2"]

        self.conf.method = "percona"

        self.osc.run()

        self.osc.show_conf.assert_called_once_with()

        db_calls = [call(db) for db in self.conf.dblist]
        self.osc.change_database.assert_has_calls(db_calls)

        self.assertEqual(len(self.conf.dblist), self.osc.check_collision.call_count)

        self.osc.run_percona.assert_has_calls(db_calls, True)
        self.osc.run_ddl.assert_not_called()

    def test_run_method_with_ddl(self):
        """Test main run with the ddl method."""
        self.osc.show_conf = MagicMock()
        self.osc.confirm = MagicMock()
        self.osc.change_database = MagicMock()
        self.osc.check_collision = MagicMock()
        self.osc.run_percona = MagicMock()
        self.osc.run_ddl = MagicMock()
        self.conf.dblist = ["test_db1", "test_db2"]

        self.conf.method = "ddl"

        self.osc.run()

        self.osc.show_conf.assert_called_once_with()

        db_calls = [call(db) for db in self.conf.dblist]
        self.osc.change_database.assert_has_calls(db_calls)

        self.osc.check_collision.assert_not_called()

        self.osc.run_percona.assert_not_called()
        self.osc.run_ddl.assert_has_calls(db_calls, True)


class TestArgumentParsing(unittest.TestCase):
    """Test cases for the command line arguments parsing."""

    def parse_args(self, args):
        """Call parse_args patching the arguments."""
        with patch.object(sys, "argv", args):
            return parse_args()

    def check_bad_args(self, args):
        """Check arg parsing fails for the given args."""
        with self.assertRaises(SystemExit) as exc:
            with hide_stderr():
                self.parse_args(args)
        self.assertEquals(exc.exception.code, 2)

    def test_missing_required_args(self):
        """Test errors with missing required args."""
        missing_required_args_list = [
            ["osc_host"],
            ["osc_host", "--host", "localhost"],
            ["osc_host", "--db", "test"],
            ["osc_host", "--table", "test"],
            ["osc_host", "--host", "localhost", "--db", "test", "--table", "test"],
            ["osc_host", "--db", "test", "--table", "test", "sql"],
            ["osc_host", "--host", "localhost", "--table", "test", "sql"],
            ["osc_host", "--host", "localhost", "--db", "test", "sql"],
        ]
        for test_args in missing_required_args_list:
            self.check_bad_args(test_args)

    def test_invalid_gtid(self):
        """Test error with an invalid gtid."""
        base_args = [
            "osc_host",
            "--host",
            "localhost",
            "--db",
            "test",
            "--table",
            "test",
            "sql",
        ]
        invalid_gtid_list = ["invalid", "-1"]
        for gtid in invalid_gtid_list:
            test_args = base_args + ["--gtid_domain_id", gtid]
            self.check_bad_args(test_args)

    def test_valid_gtid(self):
        """Test valid gtid are properly parsed."""
        base_args = [
            "osc_host",
            "--host",
            "localhost",
            "--db",
            "test",
            "--table",
            "test",
            "sql",
        ]
        gtid_list = ["1", "999"]
        for gtid in gtid_list:
            test_args = base_args + ["--gtid_domain_id", gtid]
            conf = self.parse_args(test_args)
            self.assertEqual(gtid, conf.gtid_domain_id)

    def test_sql_as_a_string(self):
        """Test the alter query is properly returned as a string."""
        base_args = [
            "osc_host",
            "--host",
            "localhost",
            "--db",
            "test",
            "--table",
            "test",
        ]
        sql_args = ["add", "column", "test", "int"]
        parsed = self.parse_args(base_args + sql_args)

        expected_sql = " ".join(sql_args)
        self.assertEqual(expected_sql, parsed.altersql)

    @patch("builtins.open")
    def test_dblist(self, open_mock):
        """Test dblist file is properly read."""
        open_mock.return_value = MagicMock()
        open_mock.return_value.__enter__.return_value = ["db1  \n", "  db2\n", ""]
        expected_dbs = ["db1", "db2"]
        dblist_file = ".dblist"

        args = [
            "osc_host",
            "--host",
            "localhost",
            "--dblist",
            dblist_file,
            "--table",
            "test",
            "sql",
        ]

        conf = self.parse_args(args)
        self.assertListEqual(expected_dbs, conf.dblist)
        open_mock.assert_called_once_with(dblist_file)

    @patch("builtins.open")
    @patch("builtins.print")
    def test_dblist_open_fail(self, print_mock, open_mock):
        """Test dblist file open fail."""
        open_mock.side_effect = IOError()
        dblist_file = ".dblist"

        args = [
            "osc_host",
            "--host",
            "localhost",
            "--dblist",
            dblist_file,
            "--table",
            "test",
            "sql",
        ]

        with self.assertRaises(SystemExit) as exc:
            self.parse_args(args)
        self.assertEquals(exc.exception.code, 1)

        open_mock.assert_called_once_with(dblist_file)
        self.assertEqual(1, print_mock.call_count)
