import logging
import signal
from abc import ABC, abstractmethod

locations = {2: "acacia", 3: "banksia"}

logger = logging.getLogger("archive_processing")


class Processor(ABC):
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.terminate = False

        if self.dry_run:
            logger.info("Dry run enabled.")

        for sig in [signal.SIGINT]:
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, sig, frame):
        logger.info(f"Interrupted! Received signal {sig}.")
        self.terminate = True

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
