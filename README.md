# archive_processing
Python 3.6+ compatible MWA Archive processing utilities.

This repo will eventually comprise the following utilities all using a common framework and common code:
* offline_compression_processor: Compress and replace older observations.
* flag_processor: RFI detection and flag file generation & archiving on new observations.
* delete_processor: Purging of any observations marked as 'to be deleted'.
* delete_redundant_vcs_processor: Delete redundant raw VCS data after recombine has been verified.
* voltage_processor: Drives the recombine process to produce useable VCS data.

These utilities run at Pawsey to support MWA Archive Operations.

* test_processor: A test processor to ensure the framework is working correctly.

There are two modes to running a subclass of the GenericObservationProcessor:
* Per observation: This allows the subclass to execute code per observation.
* Per observation and items within an observation: This allows the subclass to execute code at the observation level, and also at the per item level. Items can be anything but for cases will be data files belonging to that observation.

The GenericObservationProcessor executes the following (in pseudocode):

```
create_or_clean_working_directory() (if applicable)

get_observation_list()

if implements_per_item_processing:
    for each obs_id:
        create_obs_working_directory() (if applicable)        

        process_one_observation(obs_id)
        
        get_observation_item_list(obs_id)
        
        for each item (in parallel):
            process_one_observation_item(obs_id, item)
        
        end_of_observation(obs_id)

        remove_obs_working_directory() (if applicable)
else:
    # Implements just per observation processing
    for each obs_id (in parallel):
        create_obs_working_directory() (if applicable)
        
        process_one_observation(obs_id)
        
        remove_obs_working_directory() (if applicable)

clean_working_directory() (if applicable)
```

The subclass is responsible for overriding these methods:
* get_observation_list()
* process_one_observation(obs_id)
* get_observation_item_list(obs_id) (for per item mode only)
* process_one_observation_item(obs_id, item) (for per item mode only)
* end_of_observation(obs_id) (for per item mode only)
 