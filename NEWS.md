## 2019-08-24 v1.6.0-SNAPSHOT
 * Progress mechanism was updated
 * Changed RawRecordsDto schema with extended metadata information
 * Changed relations between UI and Backend statuses for job executions
 * Deleted stub data for job executions
 * Updated Instance schema
 * Removed partial success handling from SRS batch responses
 * Filtered out invalid Instances before saving to inventory
 * Added delete endpoint for job execution and all associated records from SRS
 * Added total records number to logDto
 * Changed logic of adding fields to MARC record resulting in update of leader value
 
 | METHOD |             URL                              | DESCRIPTION                                              |
 |--------|----------------------------------------------|----------------------------------------------------------|
 | DELETE | /change-manager/jobExecutions/{id}/records   | Delete Job Execution and all associated records from SRS |

 
## 2019-06-13 v1.5.0
 * Changed implementation for Job Execution human-readable id using DB sequence
 * JobExecution marked as Error if processing of at least one chunk failed
 * Updated Record-to-Instance mapping rules and Instance schema in accordance with breaking changes in mod-inventory
 * Progress mechanism was updated
 * Changed RawRecordsDto schema with extended metadata information
 * Optimized Record-to-Instance mapping (framed rules into 'entity')

## 2019-06-12 v1.4.1
 * Fixed mapping from Record to Instances
 * Fixed check whether processing is completed for all chunks

## 2019-06-12 v1.4.0
 * Added description for data-import flow
 * Fixed issue with saving ErrorRecords
 * Added batch update of ParsedRecords after assigning Instance id to MARC records
 * Added support for records processing in XML format
 * Applied parallel approach for mapping from records to instances
 * Use batch post to send Instances to the mod-inventory
 * MARC to Instance mapping was updated to the new one version 

## 2019-05-17 v1.3.1
 * Filled complete date and stub HrID for Job Execution
 * Changed implementation for checking statuses for JobExecutionSourceChunk
 * Applied bug fixes to build 999 fields
 
## 2019-05-12 v1.3.0
 * Borrowed mapping of MARC to Instance logic from mod-data-loader. After parsing Records are mapped to Instances and saved in mod-inventory.
 * Added support for records coming for processing in json format.
 * Applied logic for expanding parsed MARC records with additional custom fields (999 field)

## 2019-03-25 v1.2.1
 * Set required recordType field for the Record entity
 * Removed deprecated IMPORT_IN_PROGRESS and IMPORT_FINISHED statuses for JobExecution
 * Set stub data for runBy, progress ans startedDate fields for JobExecution entity

## 2019-03-20 v1.2.0
 * Renamed endpoints
 * Configured log4j2 for logging
 * Raw records MARC parser was added
 * PUT endpoint for update status and jobProfile for single JobExecution was added
 * Created ChunkProcessing Service
 * Added Spring DI support
 * Changed project structure to contain server and client parts. Client builds as a lightweight java library.

   | METHOD |             URL                               | DESCRIPTION                                        |
   |--------|-----------------------------------------------|----------------------------------------------------|
   | GET    | /metadata-provider/jobExecutions              | Get list of the JobExecutions DTO by query         |
   | GET    | /metadata-provider/logs                       | Get list of the Log entities by query              |
   | POST   | /change-manager/jobExecutions                 | Initialize JobExecution entities                   |
   | GET    | /change-manager/jobExecutions/{id}            | Get single JobExecution entity                     |
   | PUT    | /change-manager/jobExecutions/{id}/status     | Update status of JobExecution by id                |
   | PUT    | /change-manager/jobExecutions/{id}/jobProfile | Update jobProfile of single JobExecution entity    |
   | GET    | /change-manager/jobExecutions/{id}/children   | Get children JobExecutions by parent id            |
   | POST   | /change-manager/jobExecutions/{id}/records    | Receive chunk of raw records for JobExecution      |

## 2018-11-30 v0.1.0
 * Created ChangeManager component
 * Added API for managing JobExecution entities

   | METHOD |             URL                   | DESCRIPTION                                        |
   |--------|-----------------------------------|----------------------------------------------------|
   | GET    | /metadata-provider/jobExecutions  | Get list of the JobExecutions DTO by query         |
   | GET    | /metadata-provider/logs           | Get list of the Log entities by query              |
   | POST   | /change-manager/jobExecutions     | Initialize JobExecution entities                   |
   | PUT    | /change-manager/jobExecution/{id} | Update JobExecution entity                         |
