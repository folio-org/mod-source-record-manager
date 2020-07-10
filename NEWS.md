## 2020-07-10 v2.3.1-SNAPSHOT
* [MODDATAIMP-309](https://issues.folio.org/browse/MODDATAIMP-309) Return 204 response when unique constrain is violated upon job progress initialization
* [MODDATAIMP-309](https://issues.folio.org/browse/MODDATAIMP-309) Changed records publishing using blocking coordinator

## 2020-06-25 v2.3.0
* [MODSOURMAN-325](https://issues.folio.org/browse/MODSOURMAN-325) Update SRS client requests for v4

## 2020-06-11 v2.2.0
* [MODSOURMAN-276](https://issues.folio.org/browse/MODSOURMAN-276) Create an endpoint for retrieving MARC record by instance id
* [MODSOURMAN-268](https://issues.folio.org/browse/MODSOURMAN-268) Implement endpoint for updating MARC record by id
* [MODDATAIMP-300](https://issues.folio.org/browse/MODDATAIMP-300) Updated marc4j version to 2.9.1
* [MODDICORE-41](https://issues.folio.org/browse/MODDICORE-41) Update mapping for Preceding/Succeeding Titles
* [MODSOURMAN-310](https://issues.folio.org/browse/MODSOURMAN-310) Added definition that jsonSchemas api doesn't require any permissions
* [MODSOURMAN-311](https://issues.folio.org/browse/MODSOURMAN-311) JobExecution duplicates for Cornell file [BUGFIX]
* [MODDICORE-50](https://issues.folio.org/browse/MODDICORE-50) Fixed placement of newly-created 035 field
* [MODSOURMAN-275](https://issues.folio.org/browse/MODSOURMAN-275) Remove preview area's sample data
* [MODSOURMAN-318](https://issues.folio.org/browse/MODSOURMAN-318) Remove hardcoded diku tenant in db schema.json
* Updated reference to raml-storage
* [MODSOURMAN-321](https://issues.folio.org/browse/MODSOURMAN-321) Change response status to 202 on parsed record update
* [MODSOURMAN-314](https://issues.folio.org/browse/MODSOURMAN-314) Upgrade to RMB 30.0.2

## 2020-04-23 v2.1.3
* [MODSOURMAN-303](https://issues.folio.org/browse/MODSOURMAN-303) Add actual state on creating record
* [MODSOURMAN-298](https://issues.folio.org/browse/MODSOURMAN-298) Added migration script to support RMB version update 
* [MODDICORE-43](https://issues.folio.org/browse/MODDICORE-43) SRS MARC Bib: Fix formatting of 035 field constructed from incoming 001
* [MODSOURMAN-307](https://issues.folio.org/browse/MODSOURMAN-307) Progress for Cornell file duplicates and the job hangs

## 2020-04-07 v2.1.2
* [MODSOURMAN-296](https://issues.folio.org/browse/MODSOURMAN-296) Added filling connection parameters to the data import event payload
* Updated dependency onn data-import-processing-core library

## 2020-03-27 v2.1.1
* Updated mapping for instance type ID and instance format ID
* Fixed duplicate languages in instance mapping
* Updated dependency on data-import-processing-core library

## 2020-03-13 v2.1.0
* Added get journalRecords endpoint
* Incremented RMB version
* MatchedId filled in with the same value as recordId
* Exposed json schemas api
* Added module registration as publisher/subscriber to mod-pubsub
* Added mode of issuance mapping mechanism
* Updated mapping for instance identifier types and unspecified instance type
* Added JobExecutionProgress service
* Added defaultMapping query param to choose between default mapping and application of JobProfiles
* New endpoint for saving results of instance creation to journal was added 
* Implemented endpoint to handle DI_COMPLETED and DI_ERROR events.

 | METHOD |             URL                                     | DESCRIPTION                                         |
 |--------|-----------------------------------------------------|-----------------------------------------------------|
 | GET    | /metadata-provider/journalRecords/{jobExecutionId}  | Get list of the JournalRecords by jobExecution id   |
 | POST   | /change-manager/handlers/created-inventory-instance | Handle event about created inventory instance        |
 | POST   | /change-manager/handlers/processing-result          | Handle DI_COMPLETED and DI_ERROR events              |
 
## 2020-02-10 v2.0.2 
* Added HrId handling on records parsing and after instance creation

## 2020-01-06 v2.0.1
* Fixed MARC-Instance mapping for 260/264 $c
* Using "unspecified" instance type(008) instead of stub value if no 336 field in MARC record

## 2019-12-04 v2.0.0 
* Added table schema for journal service
* Implemented journal service 
* Added get JobExecutionLogDto endpoint
* Applied new JVM features to manage container memory
* Updated instance subject headings to include MARC 655 field
* Updated RuleProcessor documentation

## 2019-11-04 v1.7.0
* Wrote documentation for Marc-to-Instance mapping processor
* Added order of the record in importing file
* Create CLI way for individual tenant to update the default MARC-to-Instance map
* Fixed sorting and filtering of logs
* Deleted jobExecutionDto and logDto entities.
* Response body for endpoint "/metadata-provider/jobExecutions" changed to JobExecutionCollection.
* Deleted endpoint for logs retrieving: "/metadata-provider/logs";
* Updated mapping for "Uniform title" instance alternative title type
* Added loading sample data by "loadSample" tenant parameter.
* Provided cql query support for sorting numeric data
* Broken down source-record-manager interface into smaller ones: source-manager-job-executions, source-manager-records.
* Changed 'hrId' field type to integer for jobExecution

 | METHOD |             URL                               | DESCRIPTION                                              |
 |--------|-----------------------------------------------|----------------------------------------------------------|
 | GET    | /metadata-provider/jobExecutions              | Get list of the JobExecutions by query                   |
 | POST   | /change-manager/jobExecutions                 | Initialize JobExecution entities                         |
 | GET    | /change-manager/jobExecutions/{id}            | Get single JobExecution entity                           |
 | PUT    | /change-manager/jobExecutions/{id}/status     | Update status of JobExecution by id                      |
 | PUT    | /change-manager/jobExecutions/{id}/jobProfile | Update jobProfile of single JobExecution entity          |
 | GET    | /change-manager/jobExecutions/{id}/children   | Get children JobExecutions by parent id                  |
 | POST   | /change-manager/jobExecutions/{id}/records    | Receive chunk of raw records for JobExecution            |
 | DELETE | /change-manager/jobExecutions/{id}/records    | Delete Job Execution and all associated records from SRS |
 | GET    | /mapping-rules                                | Get current mapping rules                                |
 | PUT    | /mapping-rules                                | Update current mapping rules                             |
 | PUT    | /mapping-rules/restore                        | Restore default mapping rules                            |
 

## 2019-09-09 v1.6.1
 * Added instance-type settings loading for mapping
 * Added electronic access relationships loading for mapping
 * Added classification settings loading for mapping
 * Added instance-format settings loading for mapping
 * Added contributor name types setting loading for mapping
 * Added contributor types setting loading for mapping, simple mapping for primary sign and name
 * Added mapping for contributor type free text
 * Added instance-type identifiers settings loading for mapping
 * Applied caching for external mapping parameters
 * Added instance note types settings loading for mapping
 
## 2019-09-09 v1.6.0
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
 * Rule Processor integrated with Settings (mod-inventory-storage)
 * Filled in "fromModuleVersion" value for each "tables" and "scripts" section in schema.json
 
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
