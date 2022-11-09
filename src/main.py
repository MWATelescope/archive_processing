import sys
import argparse
import logging

from configparser import ConfigParser

from processor import DeleteProcessor
from repository import DeleteRepository

logger = logging.getLogger()


def parse_arguments(args: list = sys.argv[1:]) -> argparse.Namespace:
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

    delete_parser.add_argument("--ids", type=str, default=None)
    delete_parser.add_argument("--cfg", default="../cfg/config.cfg")
    delete_parser.add_argument("--dry_run", action="store_true")
    delete_parser.add_argument("--verbose", "-v", action="store_true", default=True)

    return parser.parse_args(args)


def read_config(file_name: str) -> ConfigParser:
    """
    Parameters
    ----------
    file_name: str
        Path to a config file.

    Returns
    -------
    ConfigParser
        ConfigParser object with parsed information from file.
    """

    logger.info("Parsing config file.")

    config = ConfigParser()

    try:
        with open(file_name) as f:
            config.read_file(f)

        return config
    except (IOError, FileNotFoundError):
        logger.error("Could not parse config file.")
        sys.exit(1)


def get_dsn(config: ConfigParser) -> str:
    """
    Parameters
    ----------
    config: ConfigParser
        ConfigParser object containing information from the supplied configuration file.

    Returns
    -------
    str
        DSN string which can be used to connect to the database.
    """
    db = {
        'host': config.get("database", "host"),
        'port': config.get("database", "port"),
        'name': config.get("database", "db"),
        'user': config.get("database", "user"),
        'pass': config.get("database", "pass"),
    }

    return f"postgresql://{db['user']}:{db['pass']}@{db['host']}:{db['port']}/{db['name']}"


def main() -> None:
    """
    Entrypoint of the application. Parses command line arguments,
    passes them to a processor_factory to get a processor, and runs it.
    """
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s')

    args = parse_arguments()
    config = read_config(args.cfg)
    dsn = get_dsn(config)

    if args.verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARN)

    match args.subcommand:
        case 'delete':
            repository = DeleteRepository(dsn=dsn, webservices_url=config.get('webservices', 'url'), dry_run=args.dry_run)
            processor = DeleteProcessor(repository=repository, dry_run=args.dry_run, config=config)

            if args.ids is not None:
                processor.run(args.ids)
            else:
                processor.run()

        case _:
            raise ValueError(f"Missing or invalid subcommand {args.subcommand}.")


if __name__ == "__main__":
    main()