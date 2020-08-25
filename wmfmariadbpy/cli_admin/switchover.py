#!/usr/bin/python3

import argparse
import re
import sys
import time

from wmfmariadbpy.RemoteExecution.CuminExecution import (
    CuminExecution as RemoteExecution,
)
from wmfmariadbpy.WMFMariaDB import WMFMariaDB
from wmfmariadbpy.WMFReplication import WMFReplication

# Heartbeat execution line, expected to be found on all masters, and to be run after a master switch
# Update if operations/puppet:modules/mariadb/manifests/heartbeat.pp changes
HEARTBEAT_EXEC = "/usr/bin/perl /usr/local/bin/pt-heartbeat-wikimedia --defaults-file=/dev/null \
--user=root --host=localhost -D heartbeat --shard={0} --datacenter={1} --update --replace \
--interval={2} --set-vars=binlog_format=STATEMENT -S {3} --daemonize \
--pid /var/run/pt-heartbeat.pid"

TENDRIL_INSTANCE = "db1115"  # instance_name:port format
ZARCILLO_INSTANCE = "db1115"  # instance_name:port format


def handle_parameters():
    parser = argparse.ArgumentParser(
        description=(
            "Performs a master to direct replica switchover "
            "in the WMF environment, automating the most "
            "error-prone steps. Example usage: "
            "switchover.py db1052 db1067"
        )
    )
    parser.add_argument(
        "master",
        help=("Original master host, in hostname:port format, " "to be switched from"),
    )
    parser.add_argument(
        "slave",
        help=(
            "Direct replica host, in hostname:port format, to be "
            "switched to, and will become the new master"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help=(
            "Timeout in seconds, to wait for several operations before returning "
            "an error (e.g. for START SLAVE). It will also mark the maximum "
            "amount of lag we can tolerate."
        ),
    )
    parser.add_argument(
        "--skip-slave-move",
        action="store_true",
        help="When set, it does not migrate current master replicas to the new host",
    )
    parser.add_argument(
        "--only-slave-move",
        action="store_true",
        help=(
            "When set, it only migrates current master replicas to the new hosts"
            ", but does not perform the rest of the operations (read only, "
            "replication inversion, etc.)"
        ),
    )
    parser.add_argument(
        "--skip-heartbeat",
        action="store_true",
        help=(
            "When set, it does not try to stop heartbeat at the original "
            "master, nor start it on the new one."
        ),
    )
    parser.add_argument(
        "--replicating-master",
        action="store_true",
        help=(
            "Normally, if the current master is replicating, the switchover is "
            "aborted. By setting this option, the current master is considered "
            "an intermediate master and its replication setup is moved to the "
            "new master"
        ),
    )
    parser.add_argument(
        "--read-only-master",
        action="store_true",
        help=(
            "The master is expected to be in read-write mode. However, in some cases, "
            "the master may be in a problematic state, or maybe it is a passive "
            "or intermediate master that is expected to be all the time in read only. "
            "When this option is set, read-only will be expected on both master and "
            "replica, and will not be enabled on switch."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="When set, do not ask for confirmation before applying the changes.",
    )
    options = parser.parse_args()
    return options


def do_preflight_checks(
    master_replication, slave_replication, timeout, replicating_master, read_only_master
):
    master = master_replication.connection
    slave = slave_replication.connection
    print("Starting preflight checks...")

    # Read only values are expected 0/1 for a normal switch, 1/1 for a read only switch
    master_result = master.execute("SELECT @@GLOBAL.read_only")
    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if not master_result["success"] or not slave_result["success"]:
        print("[ERROR]: Read only status could be not read from one or more servers")
        sys.exit(-1)
    elif not read_only_master and (
        not master_result["rows"][0][0] == 0 or not slave_result["rows"][0][0] == 1
    ):
        print(
            (
                "[ERROR]: Initial read_only status "
                "check failed: original master read_only: {} / original slave read_only: {}"
            ).format(master_result["rows"][0][0], slave_result["rows"][0][0])
        )
        sys.exit(-1)
    elif read_only_master and (
        not master_result["rows"][0][0] == 1 or not slave_result["rows"][0][0] == 1
    ):
        print(
            (
                "[ERROR]: Initial read_only status "
                "check failed: original master read_only: {} / original slave read_only: {}"
            ).format(master_result["rows"][0][0], slave_result["rows"][0][0])
        )
        sys.exit(-1)
    print(
        (
            "* Original read only values are as expected "
            "(master: read_only={}, slave: read_only=True)"
        ).format((str(read_only_master)),)
    )

    # Check current replica is a direct slave of the current master
    if not slave_replication.is_direct_replica_of(master):
        print(
            "[ERROR]: {} is not a direct replica of {}".format(
                slave.name(), master.name()
            )
        )
        sys.exit(-1)
    print("* The host to fail over is a direct replica of the master")

    # Check replication is running between hosts
    slave_status = slave_replication.slave_status()
    if (
        slave_status["slave_sql_running"] != "Yes"
        or slave_status["slave_io_running"] != "Yes"
    ):
        print("[ERROR]: The replica is not currently running")
        sys.exit(-1)
    print("* Replication is up and running between the 2 hosts")

    # Check binlog_format is the same
    master_result = master.execute("SELECT @@GLOBAL.binlog_format")
    slave_result = slave.execute("SELECT @@GLOBAL.binlog_format")
    if not master_result["success"] or not slave_result["success"]:
        print("[ERROR]: Binary log format could be not read from one or more servers")
        sys.exit(-1)
    if master_result["rows"][0][0] != slave_result["rows"][0][0]:
        print(
            "[ERROR]: The binary log format of the master is {} and the slave one is {}.".format(
                master_result["rows"][0][0], slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)
    print("* Binary log format is the same: {}".format(master_result["rows"][0][0]))

    # Check lag is not excessive
    lag = slave_replication.lag()
    if lag is None:
        print(
            "[ERROR]: It was impossible to measure the lag between the master and the slave"
        )
        sys.exit(-1)
    elif lag > timeout:
        print(
            "[ERROR]: The replica is too lagged: {} seconds, please allow it to catch up first".format(
                str(lag)
            )
        )
        sys.exit(-1)
    print(
        "* The replication lag is acceptable: {} (lower than the configured or default timeout)".format(
            str(lag)
        )
    )

    # Check for additional topology issues (replicating master or circular replication)
    master_slave_status = master_replication.slave_status()
    if replicating_master and master_slave_status is None:
        print(
            "[ERROR]: --replicating-master was set, but replication is not enabled on the master"
        )
        sys.exit(-1)
    elif not replicating_master and master_slave_status is not None:
        print(
            "[ERROR]: The master is replicating from somewhere and --replicating-master was not set, aborting"
        )
        sys.exit(-1)
    if replicating_master and master_replication.is_direct_replica_of(slave):
        print(
            "[ERROR]: Master and replica are setup in a circular replication, aborting switchover"
        )
        sys.exit(-1)


def set_master_in_read_only(master_replication):
    print("Setting up original master as read-only")
    result = master_replication.connection.execute("SET GLOBAL read_only = 1")
    if not result["success"]:
        print("[ERROR]: Could not set the master as read only")
        sys.exit(-1)


def wait_for_slave_to_catch_up(master_replication, slave_replication, timeout):
    timeout_start = time.time()
    while not slave_replication.caught_up_to_master(master_replication.connection):
        time.sleep(0.1)
        if time.time() > (timeout_start + timeout):
            break
    if not slave_replication.caught_up_to_master(master_replication.connection):
        print(
            "[ERROR]: We could not wait to catch up replication, trying now to "
            "revert read only on the master back to read-write"
        )
        result = master_replication.connection.execute("SET GLOBAL read_only = 0")
        if not result["success"]:
            print(
                "[ERROR]: We could not revert the master back to read_only, "
                "server may be down or other issues"
            )
        else:
            print("Switchover failed, but we put back the master in read/write again")
        print("Try increasing the timeout parameter, or debuging the current status")
        sys.exit(-1)

    print(
        "Slave caught up to the master after waiting {} seconds".format(
            str(time.time() - timeout_start)
        )
    )


def stop_slave(slave_replication):
    print("Stopping original master->slave replication")
    result = slave_replication.stop_slave()
    if not result["success"]:
        print("Could not stop slave: {}".format(result["errmsg"]))
        sys.exit(-1)


def set_replica_in_read_write(master_replication, slave_replication):
    slave = slave_replication.connection
    master = master_replication.connection
    print("Setting up replica as read-write")
    result = slave.execute("SET GLOBAL read_only = 0")
    if not result["success"]:
        print(
            "[ERROR]: Could not set the slave as read write, "
            "trying to revert read only on the master back to read-write"
        )
        result = master.execute("SET GLOBAL read_only = 0")
        if not result["success"]:
            print(
                "We could not revert the master back to read_only, server may be down or other issues"
            )
        else:
            print("Switchover failed, but we put back the master in read/write again")
        sys.exit(-1)

    master_result = master.execute("SELECT @@GLOBAL.read_only")
    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if (
        not master_result["success"]
        or not slave_result["success"]
        or not master_result["numrows"] == 1
        or not master_result["rows"][0][0] == 1
        or not slave_result["numrows"] == 1
        or not slave_result["rows"][0][0] == 0
    ):
        print(
            "[ERROR]: Post check failed, current status: "
            "original master read_only: {} / original slave read_only: {}".format(
                master_result["rows"][0][0], slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)
    print(
        "All commands where successful, current status: "
        "original master read_only: {} / original slave read_only: {}".format(
            master_result["rows"][0][0], slave_result["rows"][0][0]
        )
    )


def invert_replication_direction(
    master_replication, slave_replication, master_status_on_switch
):
    slave = slave_replication.connection
    print("Trying to invert replication direction")
    result = master_replication.setup(
        master_host=slave.host,
        master_port=slave.port,
        master_log_file=master_status_on_switch["file"],
        master_log_pos=master_status_on_switch["position"],
    )
    if not result["success"]:
        print("[ERROR]: We could not repoint the original master to the new one")
        sys.exit(-1)
    result = master_replication.start_slave()
    if not result["success"]:
        print("[ERROR]: We could not start replicating towards the original master")
        sys.exit(-1)
    result = slave_replication.reset_slave()
    if not result["success"]:
        print("[ERROR]: We could not reset replication on the new master")
        sys.exit(-1)


def stop_master_replication(master_replication):
    """
    Stops replication towards the master, and reset it, then return the status after the stop,
    while keeping the running status before the stop
    """
    # Save the original replication running status
    master_slave_status = master_replication.slave_status()
    original_io_status = master_slave_status["slave_io_running"]
    original_sql_status = master_slave_status["slave_sql_running"]
    print("Stopping replication to master")
    result = master_replication.stop_slave()
    if not result["success"]:
        print(
            "[ERROR]: Could not stop replication to master: {}".format(result["errmsg"])
        )
        sys.exit(-1)
    master_slave_status = master_replication.slave_status()
    master_slave_status["slave_io_running"] = original_io_status
    master_slave_status["slave_sql_running"] = original_sql_status
    if not master_slave_status["success"]:
        print(
            (
                "[ERROR]: Could not get the slave status"
                " of the master after stopping it: {}"
            ).format(result["errmsg"])
        )
        sys.exit(-1)
    result = master_replication.reset_slave()
    if not result["success"]:
        print(
            (
                "[ERROR]: Could not reset replication "
                "of master after stopping it: {}"
            ).format(result["errmsg"])
        )
        sys.exit(-1)
    print(
        ("Original master replication " "was stopped and reset at {}:{}").format(
            master_slave_status["relay_master_log_file"],
            master_slave_status["exec_master_log_pos"],
        )
    )
    return master_slave_status


def setup_new_master_replication(slave_replication, old_master_slave_status):
    """
    Restore old replication setup from the old master into the new master
    """
    # change master
    result = slave_replication.setup(
        master_host=old_master_slave_status["master_host"],
        master_port=old_master_slave_status["master_port"],
        master_log_file=old_master_slave_status["relay_master_log_file"],
        master_log_pos=old_master_slave_status["exec_master_log_pos"],
    )
    if not result["success"]:
        print(
            (
                "[ERROR]: Old replication setup was not able to be recovered, "
                "new master will not be configured as a slave"
            )
        )
        return -1
    # start slave
    if (
        old_master_slave_status["slave_io_running"] != "No"
        and old_master_slave_status["slave_sql_running"] != "No"
    ):
        print("Restarting new master replication (both threads)")
        result = slave_replication.start_slave()
    elif (
        old_master_slave_status["slave_io_running"] != "No"
        and old_master_slave_status["slave_sql_running"] == "No"
    ):
        print("Restarting new master replication io thread")
        result = slave_replication.start_slave(thread="io")
    elif (
        old_master_slave_status["slave_io_running"] == "No"
        and not old_master_slave_status["slave_sql_running"] != "No"
    ):
        print("Restarting new master replication sql thread")
    else:
        result = dict()
        result["success"] = True
    if not result["success"]:
        print(
            (
                "[ERROR]: Old replication setup was not able to be recovered, "
                "new master will not be configured as a slave"
            )
        )
        return -1
    # set gtid
    if old_master_slave_status["using_gtid"].lower() in ["slave_pos", "current_pos"]:
        changed = slave_replication.set_gtid_mode(old_master_slave_status["using_gtid"])
        if not changed:
            print("[ERROR]: Original GTID mode was not recovered on the new master")
    return 0


def verify_status_after_switch(
    master_replication, slave_replication, timeout, replicating_master, read_only_master
):
    master = master_replication.connection
    slave = slave_replication.connection
    print("Verifying everything went as expected...")
    master_result = master.execute("SELECT @@GLOBAL.read_only")
    slave_result = slave.execute("SELECT @@GLOBAL.read_only")
    if not master_result["success"] or not slave_result["success"]:
        print("[ERROR] read_only status of one or more servers could not be checked")
        sys.exit(-1)
    elif not read_only_master and (
        not master_result["rows"][0][0] == 1 or not slave_result["rows"][0][0] == 0
    ):
        print(
            "[ERROR]: Read_only status verification failed: "
            "original master read_only: {} / original slave read_only: {}".format(
                master_result["rows"][0][0], slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)
    elif read_only_master and (
        not master_result["rows"][0][0] == 1 or not slave_result["rows"][0][0] == 1
    ):
        print(
            "[ERROR]: Read_only status verification failed: "
            "original master read_only: {} / original slave read_only: {}".format(
                master_result["rows"][0][0], slave_result["rows"][0][0]
            )
        )
        sys.exit(-1)

    if not master_replication.is_direct_replica_of(slave):
        print(
            "[ERROR]: {} is not a direct replica of {}".format(
                master.name(), slave.name()
            )
        )
        sys.exit(-1)

    master_status = master_replication.slave_status()
    if (
        master_status is None
        or not master_status["success"]
        or master_status["slave_sql_running"] != "Yes"
        or master_status["slave_io_running"] != "Yes"
    ):
        print(
            "[ERROR]: The original master is not replicating correctly from the switched instance"
        )
        sys.exit(-1)

    slave_status = slave_replication.slave_status()
    if replicating_master and slave_status is None:
        print(
            "[ERROR]: --replicating-master was set, but the new master is not replicating from anywhere"
        )
        sys.exit(-1)


def move_replicas_to_new_master(master_replication, slave_replication, timeout):
    """
    Migrates all old master direct slaves to the new master, maintaining the consistency.
    """
    print("Disabling GTID on new master...")
    slave_replication.set_gtid_mode("no")
    clients = 0
    for replica in master_replication.slaves():
        print(
            "Checking if {} needs to be moved under the new master...".format(
                replica.name()
            )
        )
        if replica.is_same_instance_as(slave_replication.connection):
            print("Nope")
            continue  # do not move the target replica to itself
        replication = WMFReplication(replica, timeout)
        print("Disabling GTID on {}...".format(replica.name()))
        replication.set_gtid_mode("no")
        print("Waiting some seconds for db to catch up...")
        time.sleep(timeout)
        result = replication.move(
            new_master=slave_replication.connection, start_if_stopped=True
        )
        if result is None or not result["success"]:
            print(
                "[ERROR]: {} failed to be moved under the new master".format(
                    replica.name()
                )
            )
            sys.exit(-1)
        print("Reenabling GTID on {}...".format(replica.name()))
        replication.set_gtid_mode("slave_pos")
        print("{} was moved successfully under the new master".format(replica.name()))
        clients += 1

    query = "SHOW GLOBAL STATUS like 'Rpl_semi_sync_master_clients'"
    result = slave_replication.connection.execute(query)
    if (
        not result["success"]
        or result["numrows"] != 1
        or int(result["rows"][0][1]) < clients
    ):
        print("[WARNING]: Semisync was not enabled on all hosts")
        return -1
    return 0


def stop_heartbeat(master):
    """
    Tries to kill heartbeat at the given master host so it stops writing to it before setting it in read only.
    It gathers the options it was using from the process. If heartbeat wasn't running, it takes the information
    from the last heartbeat row available on the database and shows a warning.
    If any error happens, where the kill is not successful, it exits. Otherwise is retuns the section, datacenter,
    interval and socket used by this master
    """
    runner = RemoteExecution()
    result = runner.run(master.host, "/bin/ps --no-headers -o pid,args -C perl")
    process_id = None
    regex = "([0-9]+) " + HEARTBEAT_EXEC.format(
        "(.+)", "(.+)", "(.+)", "(.+)"
    )  # section, datacenter, interval, socket
    if result.stdout is not None:
        for line in result.stdout.splitlines():
            match = re.search(regex, line)
            if match is not None:
                process_id = int(match.group(1))
                section = match.group(2)
                datacenter = match.group(3)
                interval = match.group(4)
                socket = match.group(5)
                break
    if process_id is None:
        result = master.execute(
            "SELECT * FROM heartbeat.heartbeat ORDER BY ts DESC LIMIT 1"
        )
        if result["success"] and result["numrows"] == 1:
            print(
                "[WARNING]: Could not find a pt-heartbeat process to kill, "
                "using heartbeat table to determine the section"
            )
            section = str(result["rows"][0][6].decode("ascii"))
            datacenter = str(result["rows"][0][7].decode("ascii"))
            interval = 1
            socket = "/var/run/mysqld.sock"
            return section, datacenter, interval, socket
        else:
            print(
                "[ERROR]: Could not find pt-heartbeat process, nor read the heartbeat.heartbeat table"
            )
            sys.exit(-1)

    print("Stopping heartbeat pid {} at {}".format(str(process_id), master.name()))
    result = runner.run(master.host, "/bin/kill {}".format(str(process_id)))
    if result.returncode != 0:
        print("[ERROR]: Could not stop the heartbeat process correctly")
        sys.exit(-1)
    return section, datacenter, interval, socket


def start_heartbeat(master, section, datacenter, interval, socket):
    """
    Starts heartbeat on the given master, with the given section name (e.g. 's1', 'pc3', ...) and interval.
    Datacenter, and socket given are only used if we cannot determine the ones of the current host automatically.
    """
    if master.host.endswith("eqiad.wmnet"):
        datacenter = "eqiad"
    elif master.host.endswith("codfw.wmnet"):
        datacenter = "codfw"
    else:
        print(
            "[WARNING]: We could not determine the datacenter of {}, "
            "using the same as the original master".format(master.host)
        )
    result = master.execute("SELECT @@GLOBAL.socket")
    if not result["success"] or result["numrows"] != 1:
        print(
            "[WARNING]: We could not determine the socket of {}, "
            "using the same as the original master".format(master.host)
        )
    else:
        socket = result["rows"][0][0]

    print("Starting heartbeat section {} at {}".format(section, master.host))
    command = (
        "/usr/bin/nohup "
        + HEARTBEAT_EXEC.format(section, datacenter, interval, socket)
        + " &> /dev/null &"
    )
    runner = RemoteExecution()
    result = runner.run(master.host, command)
    if result.returncode != 0:
        print(
            "[ERROR]: Could not run pt-heartbeat-wikimedia, got output: {} {}".format(
                runner.stdout, runner.stderr
            )
        )
        sys.exit(-1)

    result = runner.run(master.host, "/bin/ps --no-headers -o pid,args -C perl")
    process_id = None
    regex = "([0-9]+) " + HEARTBEAT_EXEC.format(
        "(.+)", "(.+)", "(.+)", "(.+)"
    )  # section, datacenter, interval, socket
    if result.stdout is not None:
        for line in result.stdout.splitlines():
            match = re.search(regex, line)
            if match is not None:
                process_id = int(match.group(1))
                break
    if process_id is None:
        print(
            "[ERROR]: pt-heartbeat execution was not successful- it could not be detected running"
        )
        sys.exit(-1)
    else:
        print(
            "Detected heartbeat at {} running with PID {}".format(
                master.host, str(process_id)
            )
        )


def update_tendril(master, slave):
    """
    After switching over the master role from the 'master' host to the 'slave' one,
    update tendril so it reflects reality
    """
    print("Updating tendril...")
    # get section of the original master
    tendril = WMFMariaDB(TENDRIL_INSTANCE, database="tendril")
    query = (
        "SELECT name "
        "FROM shards "
        "WHERE master_id = (SELECT id "
        "                   FROM servers "
        "                   WHERE host = '{}' AND port = {})"
    )
    result = tendril.execute(query.format(master.host, master.port))
    if not result["success"] or result["numrows"] != 1:
        print("[WARNING] Old master not found on tendril server list")
        return -1
    section = result["rows"][0][0]
    # update section with new host id
    query = (
        "UPDATE shards "
        "SET master_id = "
        "(SELECT id FROM servers WHERE host = '{}' and port = {}) "
        "WHERE name = '{}' LIMIT 1"
    )
    result = tendril.execute(query.format(slave.host, slave.port, section))
    if not result["success"]:
        print("[WARNING] New master could not be updated on tendril")
        return -1
    print(
        ("Tendril updated successfully: " "{} is the new master of {}").format(
            slave.name(), section
        )
    )
    return 0


def update_zarcillo(master, slave):
    """
    After switching over the master role from the 'master' host to the 'slave' one,
    update zarcillo so it reflects reality
    """
    print("Updating zarcillo...")
    # get section and dc of the original master
    zarcillo = WMFMariaDB(ZARCILLO_INSTANCE, database="zarcillo")
    query = (
        "SELECT section, dc "
        "FROM masters "
        "WHERE instance = (SELECT name "
        "                  FROM instances "
        "                  WHERE server = '{}' AND port = {})"
    )
    result = zarcillo.execute(query.format(master.host, master.port))
    if not result["success"] or result["numrows"] != 1:
        print("[WARNING] Old master not found on zarcillo master list")
        return -1
    section = result["rows"][0][0]
    dc = result["rows"][0][1]
    # update section with section name from the former slave
    query = (
        "UPDATE masters "
        "SET instance = (SELECT name "
        "                FROM instances "
        "                WHERE server = '{}' AND port = {})"
        "WHERE section = '{}' AND dc = '{}' LIMIT 1"
    )
    result = zarcillo.execute(query.format(slave.host, slave.port, section, dc))
    if not result["success"]:
        print("[WARNING] New master could not be updated on zarcillo")
        return -1
    print(
        ("Zarcillo updated successfully: " "{} is the new master of {} at {}").format(
            slave.name(), section, dc
        )
    )
    return 0


def reenable_gtid_on_old_master(master_replication):
    print("Enabling GTID on old master...")
    master_replication.set_gtid_mode("slave_pos")


def handle_new_master_semisync_replication(slave):
    # Disable semi_sync_replica and enable semi_sync_master on the new master
    result = slave.execute("SET GLOBAL rpl_semi_sync_slave_enabled = 0")
    if not result["success"]:
        print("[WARNING] Semisync slave could not be disabled on the new master")
    if slave.get_version() < (10, 3, 0):
        slave.execute("UNINSTALL PLUGIN rpl_semi_sync_slave")
        slave.execute("INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so'")
    result = slave.execute("SET GLOBAL rpl_semi_sync_master_enabled = 1")
    if not result["success"]:
        print("[WARNING] Semisync could not be enabled on the new master")


def handle_old_master_semisync_replication(master):
    # Enable semi_sync_replica and disable semi_sync_master on the old master
    result = master.execute("SET GLOBAL rpl_semi_sync_master_enabled = 0")
    if not result["success"]:
        print("[WARNING] Semisync could not be disabled on the old master")
    if master.get_version() < (10, 3, 0):
        master.execute("UNINSTALL PLUGIN rpl_semi_sync_master")
        master.execute("INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so'")
    result = master.execute("SET GLOBAL rpl_semi_sync_slave_enabled = 1")
    if not result["success"]:
        print("[WARNING] Semisync slave could not be enabled on the old master")


def update_events(master, slave):
    # TODO full automation- requires core db detection
    print(
        (
            "Please remember to run the following commands as root to "
            "update the events if they are Mediawiki databases:"
        )
    )
    print(
        "mysql.py -h {} < /home/jynus/software/dbtools/events_coredb_slave.sql".format(
            master
        )
    )
    print(
        "mysql.py -h {} < /home/jynus/software/dbtools/events_coredb_master.sql".format(
            slave
        )
    )
    return 0


def ask_for_confirmation(master, slave):
    """
    Prompt console for confirmation of action of stopping instances replication
    """
    answer = ""
    while answer not in ["yes", "no"]:
        answer = input(
            "Are you sure you want to switchover current "
            "master {} and promote {} instead [yes/no]? ".format(master, slave)
        ).lower()
        if answer not in ["yes", "no"]:
            print('Please type "yes" or "no"')
    if answer == "no":
        print("Aborting switchover without touching anything!")
        sys.exit(0)


def main():
    # Preparatory steps
    options = handle_parameters()
    master = WMFMariaDB(options.master)
    slave = WMFMariaDB(options.slave)
    timeout = options.timeout
    slave_replication = WMFReplication(slave, timeout)
    master_replication = WMFReplication(master, timeout)
    replicating_master = options.replicating_master
    read_only_master = options.read_only_master

    do_preflight_checks(
        master_replication,
        slave_replication,
        timeout,
        replicating_master,
        read_only_master,
    )

    handle_new_master_semisync_replication(slave)

    if not options.skip_slave_move:
        move_replicas_to_new_master(master_replication, slave_replication, timeout)

    if options.only_slave_move:
        print(
            "SUCCESS: All slaves moved correctly, but not continuing further because --only-slave-move"
        )
        sys.exit(0)

    if not options.force:
        ask_for_confirmation(options.master, options.slave)

    # core steps
    if not options.skip_heartbeat:
        section, datacenter, interval, socket = stop_heartbeat(master)

    if replicating_master:
        old_master_slave_status = stop_master_replication(master_replication)

    if not read_only_master:
        set_master_in_read_only(master_replication)

    wait_for_slave_to_catch_up(master_replication, slave_replication, timeout)

    slave_status_on_switch = slave_replication.slave_status()
    master_status_on_switch = slave_replication.master_status()
    print(
        "Servers sync at master: {} slave: {}".format(
            slave_status_on_switch["relay_master_log_file"]
            + ":"
            + str(slave_status_on_switch["exec_master_log_pos"]),
            master_status_on_switch["file"]
            + ":"
            + str(master_status_on_switch["position"]),
        )
    )
    stop_slave(slave_replication)

    if not read_only_master:
        set_replica_in_read_write(master_replication, slave_replication)

    invert_replication_direction(
        master_replication, slave_replication, master_status_on_switch
    )

    handle_old_master_semisync_replication(master)

    if not options.skip_heartbeat:
        start_heartbeat(slave, section, datacenter, interval, socket)

    if replicating_master:
        setup_new_master_replication(slave_replication, old_master_slave_status)

    verify_status_after_switch(
        master_replication,
        slave_replication,
        timeout,
        replicating_master,
        read_only_master,
    )

    print("SUCCESS: Master switch completed successfully")

    # Additional steps
    reenable_gtid_on_old_master(master_replication)
    update_tendril(master, slave)
    update_zarcillo(master, slave)
    update_events(options.master, options.slave)

    sys.exit(0)


if __name__ == "__main__":
    main()
