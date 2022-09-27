import logging
import argparse

from processor import ProcessorFactory

def main() -> None:
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s')

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')

    delete_parser = subparsers.add_parser("delete")

    delete_parser.add_argument("--cfg", default="../../cfg/config.cfg")
    delete_parser.add_argument("--verbose", "-v", action="store_true")
    delete_parser.add_argument("--dry_run", action="store_true")

    args = parser.parse_args()

    processor_factory = ProcessorFactory(args)
    processor = processor_factory.get_processor()

    processor.run()


if __name__ == "__main__":
    main()