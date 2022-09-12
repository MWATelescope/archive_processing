# archive_processing
Python 3.6+ compatible MWA Archive processing utilities.

Currently the following are implemented:
* delete_request_processor: Deletes data associated with delete_requests in the MWA database.

```
for each unactioned delete_request:
    Get list of observations which have not been deleted
    for each observation:        
        acacia_file_list = Get list of undeleted, remote_archived data files for this obs from database which are in Acacia
            While len(acacia_file_list) > 0:
                Get batch of up to 1000 files
                Start database_transaction:
                    Update data_files setting deleted_timestamp = Now()
                    Call Acacia.Boto3.DeleteObjects(batch)                    
                On Success of boto3 call: 
                    Commit
                    remove batch from acacia_file_list
                On Failure of boto3 call: 
                    Rollback
                    Exit

        banksia_file_list = Get list of undeleted, remote_archived data files for this obs from database which are in Banksia
            While len(banksia_file_list) > 0:
                Get batch of up to 1000 files
                Start database_transaction:
                    Update data_files setting deleted_timestamp = Now()
                    Call Banksia.Boto3.DeleteObjects(batch)
                On Success of boto3 call: 
                    Commit
                    remove batch from banksia_file_list
                On Failure of boto3 call: 
                    Rollback
                    Exit
        
        if all files to be deleted were deleted:
            Update mwa_setting Set deleted_timestamp = Now()
            On Failure of update, Exit
    
    If all observations in delete_request to be deleted were deleted:
        Update delete_request Set actioned_datetime = Now()
        On Failure of update, Exit

```
There are also stand alone utilities allowing for fine grained actions:
* Utils->delete_data_file(filename)
  * This is for when we have need to individual file deletion not associated with a delete request.

```
Lookup data_file record from database
If data_files is remote_archive==True && deleted_timestamp == NULL:
    Start database_transaction:
        Update data_files setting deleted_timestamp = Now()
        Call Banksia.Boto3.DeleteObject(filename)
    On Success of boto3 call: 
        Commit
    On Failure of boto3 call: 
        Rollback
        Exit
```