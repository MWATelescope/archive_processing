import logging

from cli import parse_arguments
from processor import ProcessorFactory


def main() -> None:
    """
    Entrypoint of the application. Parses command line arguments,
    passes them to a processor_factory to get a processor, and runs it.
    """
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s')

    args = parse_arguments()

    processor_factory = ProcessorFactory(args)
    processor = processor_factory.get_processor()

    processor.run()


if __name__ == "__main__":
    main()
