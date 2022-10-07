import sys
import argparse


def parse_arguments(args: list = sys.argv) -> argparse.Namespace:
    """
    Function to Namespace object from a given list of arguments
    (defaults to sys.argv)

    Parameters
    ----------
    args: list
        List of arguments to parse.

    Returns
    -------
    list:
        Namespace object with parsed arguments.
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')

    delete_parser = subparsers.add_parser("delete")

    delete_parser.add_argument("--cfg", default="../cfg/config.cfg")
    delete_parser.add_argument("--dry_run", action="store_true")
    delete_parser.add_argument("--verbose", "-v", action="store_true", default=True)

    return parser.parse_args(args)
