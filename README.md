# archive_processing
Python 3.6+ compatible MWA Archive processing utilities.

This repo will eventually comprise the following utilities all using a common framework and common code:
* offline_compression_processor: Compress and replace older observations.
* flag_processor: RFI detection on new observations.
* delete_processor: Purging of old or poor quality data based on rules.
* voltage_processor: Drives the recombine process to produce useable VCS data.

These utilities run at Pawsey to support MWA Archive Operations.

* test_processor: A test processor to ensure the framework is working correctly.
