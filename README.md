# archive_processing
Python 3.6+ compatible MWA Archive processing utilities.

This repo will eventually comprise the following utilities all using a common framework and common code:
* offline_compression_processor: Compress and replace older observations.
* flag_processor: RFI detection on new observations.
* delete_processor: Purging of old or poor quality data based on rules.
* voltage_processor: Drives the recombine process to produce useable VCS data.

These utilities run at Pawsey to support MWA Archive Operations.

* test_processor: A test processor to ensure the framework is working correctly.

There are two modes to running a subclass of the GenericObservationProcessor:
* Per observation: This allows the subclass to execute code per observation.
* Per observation and items within an observation: This allows the subclass to execute code at the observation level, and also at the per item level. Items can be anything but for cases will be data files belonging to that observation.

The GenericObservationProcessor executes the following (in pseudocode):

```
get_observation_list()

if implements_per_item_processing:
    for each obs_id:
        process_one_observation(obs_id)
        
        file_list = get_staging_file_list(obs_id)        
        stage_files(obs_id, file_list)

        get_observation_item_list(obs_id)
        
        for each item:
            process_one_observation_item(obs_id, item)
        
        end_of_observation(obs_id)
else:
    for each obs_id:
        file_list = get_staging_file_list(obs_id)
        stage_files(obs_id, file_list)

        process_one_observation(obs_id)
```

The subclass is responsible for overriding these methods:
* get_observation_list()
* process_one_observation(obs_id)
* get_observation_item_list(obs_id) (for per item mode only)
* process_one_observation_item(obs_id, item) (for per item mode only)
* end_of_observation(obs_id) (for per item mode only)