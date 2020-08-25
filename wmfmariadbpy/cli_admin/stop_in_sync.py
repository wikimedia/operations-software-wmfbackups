#!/usr/bin/python3

import argparse
import sys

from wmfmariadbpy.WMFMariaDB import WMFMariaDB
from wmfmariadbpy.WMFReplication import WMFReplication


def handle_parameters():
    parser = argparse.ArgumentParser(
        description=(
            "Stops replication on the given 2 sibling "
            "database instances (instances replicating "
            "directly from the same master) on the same coordinate"
        )
    )
    parser.add_argument(
        "instance1", help=("Instance #1 to be stopped, in hostname:port format")
    )
    parser.add_argument(
        "instance2", help=("Instance #2 to be stopped, in hostname:port format")
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help=(
            "Timeout in seconds. A lower value will make operations faster, "
            "but it is more likely to fail if there is more lag than that "
            "between instances. Default: 5.0 seconds."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="When set, do not ask for confirmation before applying the changes.",
    )
    options = parser.parse_args()
    return options


def ask_for_confirmation(instance1, instance2):
    """
    Prompt console for confirmation of action of stopping instances replication
    """
    answer = ""
    while answer not in ["yes", "no"]:
        answer = input(
            "Are you sure you want to stop replication "
            "of {} and {} in sync [yes/no]? ".format(instance1, instance2).lower()
        )
        if answer not in ["yes", "no"]:
            print('Please type "yes" or "no"')
    if answer == "no":
        print("Aborting stop without touching anything!")
        sys.exit(0)


def main():
    # Preparatory steps
    options = handle_parameters()
    instance1 = WMFMariaDB(options.instance1)
    instance2 = WMFMariaDB(options.instance2)
    timeout = options.timeout
    instance1_replication = WMFReplication(instance1, timeout)

    if not options.force:
        ask_for_confirmation(options.instance1, options.instance2)

    result = instance1_replication.stop_in_sync_with_sibling(instance2)
    if result is None:
        print(
            "[ERROR]: {} is not a sibling of {}, or they have too much lag".format(
                instance1.name(), instance2.name()
            )
        )
        sys.exit(-1)
    if not result["success"]:
        print("[ERROR]: The stop operation failed: {}".format(result["errmsg"]))
        sys.exit(1)
    print(
        "{} and {} stopped both at {}:{}".format(
            instance1.name(), instance2.name(), result["log_file"], result["log_pos"]
        )
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
