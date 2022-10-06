import sys
import logging
import argparse

from processor import ProcessorFactory

def parse_arguments(args: list = sys.argv) -> argparse.Namespace:
    """
    Function to Namespace object from a given list of arguments (defaults to sys.argv)

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
    delete_parser.add_argument("--verbose", "-v", action="store_true", default=True)
    delete_parser.add_argument("--dry_run", action="store_true")

    return parser.parse_args(args)


def main() -> None:
    """
    Entrypoint of the application. Parses command line arguments, passes them to a processor_factory to get a processor, and runs it.
    """
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s')

    args = parse_arguments()

    processor_factory = ProcessorFactory(args)
    processor = processor_factory.get_processor()

    processor.run()


if __name__ == "__main__":
    main()