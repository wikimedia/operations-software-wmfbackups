#!/usr/bin/env python3
"""
Online (mostly) Schema Changes.

Rewritten in Python from the shell script

!!! NOTE !!! If using this on a master consider using --no-replicate.
If you cannot do so, then the schema change probably deserves
a master rotation instead :-)

!!! NOTE !!! Go read about table metadata locking during DDL in MariaDB 5.5.
Be afraid. Be very afraid...
"""
import argparse
import re
from shutil import which
import subprocess

from WMFMariaDB import WMFMariaDB


class OnlineSchemaChanger(object):
    """Class to perform the online schema changes."""

    def __init__(self, conf):
        """Get the configuration."""
        self.conf = conf
        self._conn = None
        self._osctool = ""
        self._ptrep = []
        self._ptargs = []
        self._ptdrargs = []
        self._ddlrep = []
        self._ddlargs = []

    def __del__(self):
        """Tidy up before finishing."""
        if self._conn:
            self._conn.disconnect()

    @property
    def connection(self):
        """Return the connection to the database (and create if needed)."""
        if not self._conn:
            self._conn = WMFMariaDB(host=self.conf.host,
                                    port=self.conf.port,
                                    debug=self.conf.debug)
            if not self._conn or not self._conn.connection:
                print("Connect failed: {}@{}:{}".format(self.conf.user,
                                                        self.conf.host,
                                                        self.conf.port))
                exit(1)

        return self._conn

    @property
    def ddl_rep(self):
        """Get the data definition language replication args."""
        if not self._ddlrep:
            self._ddlrep = []

            if self.conf.replicate:
                self._ddlrep.append("set session sql_log_bin=1;")

            if self.conf.no_replicate:
                self._ddlrep.append("set session sql_log_bin=0;")

            if self.conf.gtid_domain_id:
                gtid = self.conf.gtid_domain_id
                self._ddlrep.append("set session gtid_domain_id = {};".format(gtid))

        return self._ddlrep

    @property
    def ddl_args(self):
        """Get the data definition language args."""
        if not self._ddlargs:
            self._ddlargs = ["SET SESSION innodb_lock_wait_timeout=1;",
                             "SET SESSION lock_wait_timeout=60;"]

            self._ddlargs.extend(self.ddl_rep)

        return self._ddlargs

    @property
    def pt_osc_rep(self):
        """Get the Percona Toolkit OSC replication args."""
        if not self._ptrep:
            self._ptrep = ["--recurse=0"]

            if self.conf.replicate:
                self._ptrep = ["--recurse=1", "--chunk-size-limit=10"]

            if self.conf.no_replicate:
                self._ptrep = ["--recurse=0", "--set-vars=sql_log_bin=off"]

            res = self.connection.execute("show slave status")
            slave = res.get('numrows', 0) > 0
            if slave:
                self._ptrep.append("--check-slave-lag={}".format(self.conf.host))

        return self._ptrep

    @property
    def pt_osc_dry_run_args(self):
        """Get the Percona Toolkit OSC args for the dry run."""
        if not self._ptdrargs:
            self._ptdrargs = []

            if self.conf.primary_key:
                self._ptdrargs.append("--no-check-alter")

            self._ptdrargs.extend(self.pt_osc_rep)

        return self._ptdrargs

    @property
    def pt_osc_args(self):
        """Get the Percona Toolkit OSC args."""
        if not self._ptargs:
            self._ptargs = []

            if self.conf.primary_key:
                self._ptargs.append("--no-check-alter")

            if self.conf.no_cleanup:
                self._ptargs.extend(["--no-swap-tables",
                                     "--no-drop-new-table",
                                     "--no-drop-old-table",
                                     "--no-drop-triggers"])

            self._ptargs.extend(self.pt_osc_rep)

        return self._ptargs

    def confirm(self):
        """Ask for confirmation if we want to be bugged."""
        if not self.conf.warn:
            return
        answer = input("continue? yes/no ")
        if not answer.startswith('y'):
            print('abort')
            exit(0)

    def _execute(self, sql):
        """Run a query and return wether it was successfull or not."""
        print(sql)
        res = self.connection.execute(sql)
        return res.get('success')

    def execute(self, sql, args=[]):
        """Run a query and return wether it was successfull or not.

        An optional list of args can be provided and will be run before.
        """
        for arg in args:
            if not self._execute(arg):
                return False

        return self._execute(sql)

    @property
    def osctool(self):
        """Get the pt osc tool."""
        if not self._osctool:
            self._osctool = which('pt-online-schema-change')
            if not self._osctool:
                print("Error: Could not find 'pt-online-schema-change'.")
                exit(1)

        return self._osctool

    def run_command(self, cmd):
        """Run a command with Popen and return True if successful."""
        stdout = None if self.conf.debug else subprocess.PIPE
        stderr = None if self.conf.debug else subprocess.PIPE
        process = subprocess.Popen(cmd,
                                   stdout=stdout,
                                   stderr=stderr)
        ret_code = process.wait()
        return ret_code == 0

    def run_pt_ost_alter(self, db, dry_run=False):
        """Run the percona ost alter on the given db."""
        cmd = [self.osctool,
               "--critical-load", "Threads_running=400",
               "--max-load", "Threads_running=300",
               "--alter-foreign-keys-method=none", "--force",
               "--nocheck-replication-filters",
               "--no-version-check"]

        if dry_run:
            cmd.append("--dry-run")
            cmd.extend(self.pt_osc_dry_run_args)
        else:
            cmd.append("--execute")
            cmd.extend(self.pt_osc_args)

        cmd.extend(["--alter", '{}'.format(self.conf.altersql),
                    "D={},t={},h={},P={},u={}".format(db,
                                                      self.conf.table,
                                                      self.conf.host,
                                                      self.conf.port,
                                                      self.conf.user)])

        return self.run_command(cmd)

    def run_pt_cleanup(self, db):
        """Run a cleanup on a given db after the percona osc has been run."""
        print("Ready for cleanup. Will do this:")
        sql = "rename table {0} to _{0}_done, _{0}_new to {0}".format(self.conf.table)
        print(sql)
        self.confirm()
        self.execute(sql, self.ddl_rep)

        sql = "drop trigger if exists pt_osc_{}_{}_upd".format(db, self.conf.table)
        self.execute(sql, self.ddl_rep)

        sql = "drop trigger if exists pt_osc_{}_{}_del".format(db, self.conf.table)
        self.execute(sql, self.ddl_rep)

        sql = "drop table if exists _{}_done".format(self.conf.table)
        self.execute(sql, self.ddl_rep)

    def run(self):
        """Perform the online schema change operation."""
        print("Host        : {}".format(self.conf.host))
        print("Port        : {}".format(self.conf.port))
        print("Databases   : {}".format(self.conf.dblist))
        print("Table       : {}".format(self.conf.table))
        print("Alter SQL   : {}".format(self.conf.altersql))
        print("method      : {}".format(self.conf.method))
        print("pt dry args : {}".format(self.pt_osc_dry_run_args))
        print("pt args     : {}".format(self.pt_osc_args))
        print("ddl args    : {}".format(self.ddl_args))
        print("analyze     : {}".format(self.conf.analyze))

        self.confirm()

        for db in self.conf.dblist:
            self.connection.change_database(db)
            if self.connection.database != db:
                print("Error: Could not change to '{}'.".format(db))
                exit(1)
            print("host: {}, database: {}".format(self.conf.host, db))

            if self.conf.method == 'percona':
                new_table = '_{}_new'.format(self.conf.table)
                res = self.connection.execute("show tables like '{}'".format(new_table))
                if res.get('numrows', 0) > 0:
                    print("{} already exists!".format(new_table))
                    self.confirm()

                dry_run_ret = self.run_pt_ost_alter(db, dry_run=True)
                if dry_run_ret:
                    actual_run_ret = self.run_pt_ost_alter(db)
                    if actual_run_ret:
                        if self.conf.no_cleanup:
                            self.run_pt_cleanup(db)

                        if self.conf.analyze:
                            sql = "analyze table {};".format(self.conf.table)
                            self.execute(sql, self.ddl_rep)
                    else:
                        print("WARNING {} : {} encountered problems".format(db, self.conf.table))
                        self.confirm()
                else:
                    print("SKIPPING {} : {} dry-run encountered problems".format(db, self.conf.table))
                    self.confirm()

            elif self.conf.method in ("ddl", "ddlonline"):
                table = self.conf.table
                alter = self.conf.altersql
                if self.conf.method == "ddl":
                    sql = "alter table `{}` {}".format(table, alter)
                else:
                    sql = "alter online table `{}` {}".format(table, alter)

                if not self.execute(sql, self.ddl_args):
                    print("WARNING {} encountered problems while being executed at {}.{}"
                          .format(alter, db, table))
                    self.confirm()


def parse_args():
        """Parse the execution parameters and return an object with them."""
        parser = argparse.ArgumentParser()

        parser.add_argument('--host',
                            help="the host to connect to",
                            required=True)
        parser.add_argument('--port', type=int,
                            help="the port to connect to",
                            default=3306)

        parser.add_argument('--user', help="username to use",
                            default='root')

        dblist_group = parser.add_mutually_exclusive_group(required=True)
        dblist_group.add_argument('--db', nargs='+',
                                  help="Database(s) to be altered")
        dblist_group.add_argument('--dblist',
                                  help="File with the list of databases")

        parser.add_argument('--table',
                            help="Table to alter",
                            required=True)

        methods = ['percona', 'ddl', 'ddlonline']
        parser.add_argument('--method',
                            help="Method to use ({})".format(', '.join(methods)),
                            choices=methods,
                            default='percona')

        warn_group = parser.add_mutually_exclusive_group()
        warn_group.add_argument('--warn', action='store_true', dest='warn',
                                help="Ask for confirmation after a problem")
        warn_group.add_argument('--no-warn', action='store_false', dest='warn',
                                help="Don't ask for confirmation after a problem")

        analyze_group = parser.add_mutually_exclusive_group()
        analyze_group.add_argument('--analyze',
                                   action='store_true', dest='analyze',
                                   help="Analyze after the alter")
        analyze_group.add_argument('--no-analyze',
                                   action='store_false', dest='analyze',
                                   help="Don't analyze after the alter")

        parser.add_argument('altersql', nargs="+",
                            help="Modification to be applied")

        parser.add_argument('--debug',
                            action='store_true',
                            help="Show debug info")

        parser.set_defaults(warn=True)
        parser.set_defaults(analyze=False)

        replicate_group = parser.add_mutually_exclusive_group()
        replicate_group.add_argument('--replicate', action='store_true',
                                     help="Replicate the changes")
        replicate_group.add_argument('--no-replicate', action='store_true',
                                     help="Don't replicate the changes")

        def valid_gtid(value):
            pat = re.compile(r"[0-9]+")
            if not pat.match(value):
                error_msg = "'{}' is an invalid gtid".format(value)
                raise argparse.ArgumentTypeError(error_msg)
            return value

        parser.add_argument('--gtid_domain_id', type=valid_gtid,
                            help="gtid domain id")

        parser.add_argument('--primary-key', action='store_true',
                            help="Don't panic when altering a primary key")
        parser.add_argument('--no-cleanup', action='store_true',
                            help="Don't actually switch the new and old tables on completion")

        args = parser.parse_args()

        args.altersql = ' '.join(args.altersql)

        # Check list of databases is correct
        if args.dblist:
            if not args.dblist.endswith('.dblist'):
                print("'{}' doesn't have the 'dblist' extension".format(args.dblist))
                exit(1)

            try:
                with open(args.dblist) as f:
                    args.dblist = [l.strip() for l in f if l.strip()]
            except IOError:
                print("Can't read '{}'".format(args.dblist))
                exit(1)
        else:
            args.dblist = args.db

        return args


def main():
    """Parse de arguments and create the object to perform the operation."""
    options = parse_args()
    osc = OnlineSchemaChanger(options)
    osc.run()


if __name__ == "__main__":
    main()
