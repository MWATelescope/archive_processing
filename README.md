# archive_processing

Utility for processing MWA archive data.

Currently the following are implemented:

* delete_request_processor: Deletes data associated with delete_requests in the MWA database.
* incomplete_processor: Handles checking if incomplete files in Acacia or Banksia can be safely removed but downloading the file and comparing the checksum with our records. If so, the incomplete file can be safely removed.

## Installation

Python 3.10 or greater is required for this software.

We use pyenv to manage virtual environments. Installation of this tools is not covered here.

### Create a new virtual environment

```bash
pyenv virtualenv 3.10 archive_processing
```

### Activate it

```bash
pyenv local archive_processing
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Configuration

A configuration file must be suppled. A Sample configuration file has been included in cfg/test_delete_processor.cfg. This file is used when running the tests in the test_delete_processor.py tests python file.

## Delete Processor: Usage

The entrypoint of this application is src/main.py. Usage is as follows:

```bash
python main.py delete OPTIONS
```

By default (without any options), the processor will delete all files associated with all unactioned & uncancelled delete requests. You can modify this behaviour with the CLI options below:

```bash
--ids=15,16,17 (default None) A comma separated list of delete request IDs to process. All other outstanding delete requests will not be processed.
--cfg=/path/to/config (default ../cfg/config.cfg) path to a configuration file.
--dry_run (Default False) If this option is enabled, no deletes will take place.
--verbose (Default True) Whether to enable verbose logging
```

### Delete Processor: Testing

A CI workflow will run on all pushes and pull requests to master. However these tests can also be run manually.

Note:
This code needs to modify real database rows, and the tests verify that those rows have been modified in the correct way. Therefore, in order for the tests to work, you need to have a local instance of PostgreSQL running. Your local Postgres configuration can be supplied to pytest like so:

```bash
pytest --postgresql-password=postgres --postgresql-host=127.0.0.1
```

### Delete Processor: Todo

* Develop a way to delete single files (VERY niche)
* Develop a way to delete single obs_ids (maybe not a real issue- just create a DR with a single obsid? DONE!)

## Incomplete Processor Usage

The entrypoint of this application is src/main.py. Usage is as follows:

```bash
python main.py incomplete OPTIONS
```

By default (without any options), the processor will delete all files associated with all unactioned & uncancelled delete requests. You can modify this behaviour with the CLI options below:

```bash
--cfg=/path/to/config (default ../cfg/config.cfg) path to a configuration file.
--location {acacia, banksia} which location to check for and remove incomplete files
--dry_run (Default False) If this option is enabled, no removal of incomplete files will take place.
--verbose (Default True) Whether to enable verbose logging
```
