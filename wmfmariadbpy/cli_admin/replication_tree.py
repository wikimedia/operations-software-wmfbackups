#!/usr/bin/python3

import argparse
import sys
import time
from operator import attrgetter

from wmfmariadbpy.WMFMariaDB import WMFMariaDB
from wmfmariadbpy.WMFReplication import WMFReplication

COLOR_UNDERLINE = "\033[4m"
COLOR_NORMAL = "\033[0m"
COLOR_RED = "\033[91m"
COLOR_DARK_YELLOW = "\033[33m"
COLOR_GREEN = "\033[92m"
COLOR_BLUE = "\033[34m"
COLOR_MAGENTA = "\033[95m"


class Instance:
    def __init__(
        self,
        name,
        read_only,
        uptime,
        lag=0,
        processes=0,
        connection_latency=0.0,
        query_latency=0.0,
        version=None,
        binlog_format=None,
        replicas=[],
        cross_dc_replication=False,
    ):
        self.name = name if name is not None else "<None>"
        self.uptime = uptime
        self.lag = lag
        if processes is None or processes == 0:
            self.processes = None
        else:
            self.processes = processes
        self.connection_latency = (connection_latency,)
        self.query_latency = query_latency
        self.version = version
        self.binlog_format = binlog_format
        self.read_only = read_only
        self.replicas = replicas
        self.cross_dc_replication = cross_dc_replication

    def print_name(self):
        return COLOR_UNDERLINE + str(self.name) + COLOR_NORMAL

    def print_uptime(self):
        if not isinstance(self.uptime, int):
            time = str(self.uptime)
        elif self.uptime < 60:
            time = COLOR_RED + str(self.uptime) + "s" + COLOR_NORMAL
        elif self.uptime < 3600:
            time = COLOR_RED + str(int(self.uptime / 60)) + "m" + COLOR_NORMAL
        elif self.uptime < (3600 * 24):
            time = COLOR_DARK_YELLOW + str(int(self.uptime / 3600)) + "h" + COLOR_NORMAL
        elif self.uptime < (3600 * 24 * 365):
            time = str(int(self.uptime / 3600 / 24)) + "d"
        else:
            time = (
                COLOR_DARK_YELLOW
                + str(int(self.uptime / 3600 / 24 / 365))
                + "y"
                + COLOR_NORMAL
            )
        return "up: " + time

    def print_binlog_format(self):
        return "binlog: " + str(self.binlog_format)

    def print_read_only(self):
        if self.read_only == 0:
            read_only = "OFF"
            color = COLOR_RED
        else:
            read_only = "ON"
            color = COLOR_GREEN
        return "RO: " + color + str(read_only) + COLOR_NORMAL

    def print_lag(self):
        if self.lag is None or self.lag > 10:
            color = COLOR_RED
        elif self.lag <= 10 and self.lag > 1:
            color = COLOR_DARK_YELLOW
        else:
            color = COLOR_GREEN
        return "lag: " + color + str(self.lag) + COLOR_NORMAL

    def print_processes(self):
        if self.processes is None or self.processes >= 200:
            color = COLOR_RED
        elif self.processes < 200 and self.processes > 15:
            color = COLOR_DARK_YELLOW
        else:
            color = COLOR_GREEN
        return "processes: " + color + str(self.processes) + COLOR_NORMAL

    def print_version(self):
        return "version: " + str(self.version)

    def print_replication(self):
        if self.cross_dc_replication:
            color = COLOR_MAGENTA
        else:
            color = COLOR_BLUE
        return color + "+" + COLOR_NORMAL + " "

    def print_latency(self):
        return "latency: {:.4f}".format(self.query_latency)

    def console(self, level=0):
        if self.version is None:
            fields = [self.print_name(), COLOR_RED + "DOWN" + COLOR_NORMAL]
        else:
            fields = [
                self.print_name(),
                self.print_version(),
                self.print_uptime(),
                self.print_read_only(),
                self.print_binlog_format(),
                self.print_lag(),
                self.print_processes(),
                self.print_latency(),
            ]
        output = ", ".join(fields)
        if level < 10:  # prevent infinite loops
            for replica in sorted(self.replicas, key=attrgetter("name")):
                output += (
                    "\n"
                    + " " * (level * 2)
                    + replica.print_replication()
                    + replica.console(level=level + 1)
                )
        return output

    def __str__(self):
        return self.console()


def handle_parameters():
    parser = argparse.ArgumentParser(
        description=("Shows in console a summary of a replication graph")
    )
    parser.add_argument(
        "instance", help=("Host part of the replica set which information is shown")
    )
    options = parser.parse_args()
    return options


def get_instance_data(instance):
    name = instance.name(show_db=False)
    binlog_format = None
    processes = None
    version = None
    lag = None
    read_only = False
    uptime = None
    replicas = []
    time_before_query = time.time()
    numerical_dc = 0
    query = (
        "SELECT @@GLOBAL.binlog_format, "
        "SUBSTRING_INDEX(@@GLOBAL.version, '-', 1), @@GLOBAL.read_only "
        "UNION ALL SELECT count(*), NULL, NULL "
        "FROM sys.x$processlist WHERE conn_id IS NOT NULL "
        "UNION ALL SELECT VARIABLE_VALUE, NULL, NULL "
        "FROM information_schema.global_status WHERE VARIABLE_NAME = 'UPTIME'"
    )
    result = instance.execute(query)
    time_after_query = time.time()
    query_latency = float(time_after_query - time_before_query)
    if result["success"]:
        binlog_format = result["rows"][0][0]
        version = result["rows"][0][1]
        read_only = result["rows"][0][2]
        processes = int(result["rows"][1][0])
        uptime = int(result["rows"][2][0])
    replication = WMFReplication(instance)
    lag = replication.lag()
    numerical_dc = name.split(":")[0].split(".")[0][-4:-3]
    slaves = replication.slaves()
    for slave in slaves:
        instance = get_instance_data(slave)
        # Mark different dcs from the master
        if instance.name.split(":")[0].split(".")[0][-4:-3] != numerical_dc:
            instance.cross_dc_replication = True
        replicas.append(instance)
    return Instance(
        name=name,
        binlog_format=binlog_format,
        processes=processes,
        version=version,
        lag=lag,
        read_only=read_only,
        query_latency=query_latency,
        replicas=replicas,
        uptime=uptime,
    )


def generate_tree(instance):
    db = WMFMariaDB(instance)
    master = get_instance_data(db)
    return master


def main():
    options = handle_parameters()
    tree = generate_tree(options.instance)
    print(tree)
    sys.exit(0)


if __name__ == "__main__":
    main()
