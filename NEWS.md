## XXXX-XX-XX v3.1X.X
* [MODSOURMAN-1304](https://folio-org.atlassian.net/browse/MODSOURMAN-1304) Handle journal records for MARC on Instance errors and COMPLETED INSTANCE/MARC_BIB events
* [MODSOURMAN-1323](https://folio-org.atlassian.net/browse/MODSOURMAN-1323) Ensure holdings validation to check central ECS tenant
* [MODDATAIMP-1214](https://folio-org.atlassian.net/browse/MODDATAIMP-1214) Ensure permission that allows update of shared instance and MARC

## 2025-03-13 v3.10.0
* [MODSOURMAN-1246](https://folio-org.atlassian.net/browse/MODSOURMAN-1246) Added data import completion notifications
* [MODSOURMAN-1266](https://folio-org.atlassian.net/browse/MODSOURMAN-1266) Event DI_JOB_COMPLETED is not being sent upon the completion of the data import process
* [MODDATAIMP-1151](https://folio-org.atlassian.net/browse/MODDATAIMP-1151) Fix import job stuck if used invalid job profile (MODSOURMAN-1301)
* [MODSOURMAN-1276](https://folio-org.atlassian.net/browse/MODSOURMAN-1276) Add MARC fields 147/447/547 to authority mappings
* [MODSOURMAN-1281](https://folio-org.atlassian.net/browse/MODSOURMAN-1281) 500 Error Returned when Posting Records Chunk with Existing ID Value
* [MODINV-1140](https://folio-org.atlassian.net/browse/MODINV-1140) Update MARC-Instance mapping to account for Leader 05 value
* [MODSOURMAN-1282](https://folio-org.atlassian.net/browse/MODSOURMAN-1282) Marc Authority DI completed, but status changed fo failed
* [MODDICORE-440](https://folio-org.atlassian.net/browse/MODDICORE-440) Fix Mode of Issuance mapping if incoming record has no 001 field
* [MODSOURMAN-1288](https://folio-org.atlassian.net/browse/MODSOURMAN-1288) Add MARC fields 180/480/580 to authority mappings
* [MODSOURMAN-1294](https://folio-org.atlassian.net/browse/MODSOURMAN-1294) Fix slow DB query for journal records insert
* [MODSOURMAN-1283](https://folio-org.atlassian.net/browse/MODSOURMAN-1283) Update mod-source-record-manager to Java 21
* [MODSOURMAN-1033](https://folio-org.atlassian.net/browse/MODSOURMAN-1033) Add MARC fields 1XX/4XX/5XX to authority mappings

## 2024-10-29 v3.9.0
* [MODSOURMAN-1232](https://folio-org.atlassian.net/browse/MODSOURMAN-1232) Add the option to exclude job profile names to GET "/metadata-provider/jobExecutions" endpoint
* [MODSOURMAN-1195](https://folio-org.atlassian.net/browse/MODSOURMAN-1195) Save job execution progress in batches
* [MODSOURMAN-1166](https://folio-org.atlassian.net/browse/MODSOURMAN-1166) Sorting by Autority, Order and Error columns is not working on Log details page
* [MODDATAIMP-1029](https://folio-org.atlassian.net/browse/MODDATAIMP-1029) The authority record loaded via data-import using Default - Create SRS MARC Authority job profile is duplicated on the job-summary page
* [MODSOURMAN-1152](https://folio-org.atlassian.net/browse/MODSOURMAN-1152) The error message is not displayed in the di log summary
* [MODSOURMAN-1151](https://folio-org.atlassian.net/browse/MODSOURMAN-1151) Records are not displayed under their original numbers and are constantly changing by places
* [MODSOURMAN-1120](https://folio-org.atlassian.net/browse/MODSOURMAN-1120) Update default mapping to include mapping for Cancelled LCCN
* [MODSOURMAN-1176](https://folio-org.atlassian.net/browse/MODSOURMAN-1176) The 'User' filter is not updated after logs deletion
* [MODSOURMAN-1173](https://issues.folio.org/browse/MODSOURMAN-1173) Handle Situation When Job Profile Has No Child Profiles
* [MODSOURMAN-1127](https://folio-org.atlassian.net/browse/MODSOURMAN-1127) Change mapping for 010 sub-fields to separate LCCN and Cancelled LCCN
* [MODSOURMAN-1145](https://issues.folio.org/browse/MODSOURMAN-1145) Modify action before match breaks non match logs
* [MODSOURMAN-1188](https://issues.folio.org/browse/MODSOURMAN-1188) Change MARC mappings of 010 $z from "Cancelled" LCCN to "Canceled LCCN"
* [MODSOURMAN-1181](https://issues.folio.org/browse/MODSOURMAN-1181) Modify the get_job_log_entries function to increase performance.
* [MODSOURMAN-1185](https://folio-org.atlassian.net/browse/MODSOURMAN-1185) Logs are duplicated on the import logs page for order import
* [MODSOURMAN-1194](https://folio-org.atlassian.net/browse/MODSOURMAN-1194) Include subject metadata subfields in authority name fields
* [MODSOURMAN-1215](https://folio-org.atlassian.net/browse/MODSOURMAN-1215) Upgrade Spring from 5 to 6.1.12
* [MODINV-1069](https://folio-org.atlassian.net/browse/MODINV-1069) Fix DataImportConsumerVerticleTest in mod-inventory and Fix NPE in HoldingsItemMatcher, fix job log entries
* [MODSOURMAN-1212](https://folio-org.atlassian.net/browse/MODSOURMAN-1212) Update MARC bib-instance default mapping for subject source and subject type
* [MODDATAIMP-1085](https://folio-org.atlassian.net/browse/MODDATAIMP-1085) Provide module permissions for subject types and sources
* [MODDICORE-415](https://folio-org.atlassian.net/browse/MODDICORE-415) Adjust mapping of Subject source specified in subfield 2
* [MODSOURMAN-1228](https://folio-org.atlassian.net/browse/MODSOURMAN-1228) Update default mapping for Date type, Date 1, and Date 2 fields
* [MODSOURMAN-1239](https://folio-org.atlassian.net/browse/MODSOURMAN-1239) mod-source-record-manager Ramsons 2024 R2 - RMB v35.3.x update
* [MODSOURMAN-1241](https://folio-org.atlassian.net/browse/MODSOURMAN-1241) Add missing module permissions for PUT /change-manager/parsedRecords/{id}
* [MODSOURMAN-1222](https://folio-org.atlassian.net/browse/MODSOURMAN-1222) Fix inconsistencies in permission namings
* [MODSOURMAN-1244](https://folio-org.atlassian.net/browse/MODSOURMAN-1244) Update MARC bib-instance default mapping to include additional subject types
* [MODSOURMAN-1240](https://folio-org.atlassian.net/browse/MODSOURMAN-1240) The title of record is not displayed on the JSON data after importing file for creating order

## 2023-03-22 v3.8.0
* [MODSOURMAN-1131](https://folio-org.atlassian.net/browse/MODSOURMAN-1131) The import of file for creating orders is completed with errors
* [MODSOURMAN-1139](https://folio-org.atlassian.net/browse/MODSOURMAN-1139) Fix Kafka test failures in ChangeManagerAPITest
* [MODDATAIMP-1003](https://folio-org.atlassian.net/browse/MODDATAIMP-1003) Provide "orders-storage.titles.item.get" module permission
* [MODSOURMAN-1123](https://issues.folio.org/browse/MODSOURMAN-1123) Create Kafka topics instead of relying on auto create in mod-srm
* [MODSOURMAN-1113](https://issues.folio.org/browse/MODSOURMAN-1113) Reduce Memory Allocations Of Strings
* [MODSOURMAN-1085](https://issues.folio.org/browse/MODSOURMAN-1085) MARC record with a 100 tag without a $a is being discarded on import.
* [MODSOURMAN-1020](https://issues.folio.org/browse/MODSOURMAN-1020) Add table to save incoming records for DI logs
* [MODSOURMAN-1021](https://issues.folio.org/browse/MODSOURMAN-1021) Provide endpoint for getting parsed content for DI log
* [MODSOURMAN-1022](https://issues.folio.org/browse/MODSOURMAN-1022) Remove step of initial saving of incoming records to SRS
* [MODSOURMAN-1070](https://issues.folio.org/browse/MODSOURMAN-1070) Fill in Journal Records for created MARC when INSTANCE_CREATED event received
* [MODSOURMAN-1030](https://issues.folio.org/browse/MODSOURMAN-1030) The number of updated records is not correct displayed in the 'SRS Marc' column in the 'Log summary' table
* [MODSOURMAN-976](https://issues.folio.org/browse/MODSOURMAN-976) Incorrect error counts
* [MODSOURMAN-1093](https://issues.folio.org/browse/MODSOURMAN-1093) EventHandlingUtil hangs forever on error
* [MODSOURMAN-1043](https://issues.folio.org/browse/MODSOURMAN-1043) Improper behavior in multiples for holdings when update action on match and create on non-match
* [MODSOURMAN-1091](https://issues.folio.org/browse/MODSOURMAN-1091) The '1' number of Instance is displayed in cell in the row with the 'Updated' row header at the individual import job's log
* [MODSOURMAN-1108](https://issues.folio.org/browse/MODSOURMAN-1108) MARC authority record is not created when use Job profile with match profile and action only on no-match branch (MODSOURMAN-1110)
* [MODSOURMAN-1106](https://issues.folio.org/browse/MODSOURMAN-1106) The status of Instance is '-' in the Import log after uploading file. The numbers of updated SRS and Instance are not displayed in the Summary table. (MODSOURMAN-1114)
* [MODSOURMAN-1063](https://issues.folio.org/browse/MODSOURMAN-1063) Update RecordProcessingLogDto to contain incoming record id
* [MODSOURMAN-1122](https://issues.folio.org/browse/MODSOURMAN-1122) Add additional check for the childSnapshotWrappers
* [MODSOURMAN-1140](https://folio-org.atlassian.net/browse/MODSOURMAN-1140) Invalidate cache before saving new parsed content in cache
* [MODSOURMAN-1133](https://folio-org.atlassian.net/browse/MODSOURMAN-1133) Adjust SQL condition to include DISCARDED holding and items
* [MODDATAIMP-1001](https://folio-org.atlassian.net/browse/MODDATAIMP-1001) Remove 999 validation for instance creation
* [MODSOURMAN-956](https://folio-org.atlassian.net/browse/MODSOURMAN-956) Stop processing the job with incorrect profile
* [MODSOURMAN-1153](https://folio-org.atlassian.net/browse/MODSOURMAN-1153) Add the "acceptInstanceId" flag into the dataImportEventPayload context
* [MODSOURMAN-1150](https://folio-org.atlassian.net/browse/MODSOURMAN-1150) Add incomingRecordId field at record processing log
* [MODSOURMAN-1137](https://folio-org.atlassian.net/browse/MODSOURMAN-1137) Keep order of MARC fields while Creating/Deriving/Editing MARC records
* [MODSOURMAN-1143](https://folio-org.atlassian.net/browse/MODSOURMAN-1143) Upgrade mod-source-record-manager to RMB 35.2.0 and Vertx 4.5.4
* [MODSOURMAN-1141](https://folio-org.atlassian.net/browse/MODSOURMAN-1141) Change get_job_log_entries sql function to retrieve sourceRecordId from existing bib
* [MODSOURMAN-1146](https://folio-org.atlassian.net/browse/MODSOURMAN-1146) Marc authority records with errors are not displayed in the result table in logs
* [MODINV-968](https://folio-org.atlassian.net/browse/MODINV-968) Fix get_job_entries to return UPDATED holdings
* [MODSOURMAN-1084](https://folio-org.atlassian.net/browse/MODSOURMAN-1084) Fill in Journal Records for updated MARC when INSTANCE_UPDATED event received
* [FAT-9178](https://folio-org.atlassian.net/browse/FAT-9178) Return job log entries for purchase order lines
* [MODSOURMAN-1125](https://folio-org.atlassian.net/browse/MODSOURMAN-1125) Fix error during authority creation
* [MODDATAIMP-957](https://folio-org.atlassian.net/browse/MODDATAIMP-957) Adjust order creation flow to remove step of initial saving of incoming records to SRS
* [MODDATAIMP-983](https://folio-org.atlassian.net/browse/MODDATAIMP-983) Add permissions for creating/updating MARC Bib by DI
* [MODSOURMAN-1109](https://folio-org.atlassian.net/browse/MODSOURMAN-1109) Fixed handling of response deserialization error
* [MODSOURMAN-1115](https://folio-org.atlassian.net/browse/MODSOURMAN-1115) Accommodate for authority-source-files api changes
* [MODSOURMAN-1116](https://folio-org.atlassian.net/browse/MODSOURMAN-1116) Accommodate for authority-source-files api optimistic locking changes
* Links in the documentation have been actualized
* [MODSOURMAN-996](https://folio-org.atlassian.net/browse/MODSOURMAN-996) View all logs: Search by fileName is case-sensitive
* [MODSOURMAN-1158](https://folio-org.atlassian.net/browse/MODSOURMAN-1158) Use matchedId as entityId for marcBib records
## 2023-10-13 v3.7.0
* [MODSOURMAN-1045](https://issues.folio.org/browse/MODSOURMAN-1045) Allow create action with non-matches for instance without match profile
* [MODSOURMAN-1003](https://issues.folio.org/browse/MODSOURMAN-1003) Allow create action with non-matches for instance
* [MODSOURMAN-1029](https://issues.folio.org/browse/MODSOURMAN-1029) Introduce Global Backpressure For Kafka Consumption
* [MODSOURMAN-1031](https://issues.folio.org/browse/MODSOURMAN-1031) The status of holdings is not displayed in the Import log after uploading file for creating holdings
* [MODSOURMAN-1011](https://issues.folio.org/browse/MODSOURMAN-1011) Import An Instance With A Known Identifier (new acceptInstanceId parameter)
* [MODSOURMAN-999](https://issues.folio.org/browse/MODSOURMAN-999) Upgrade mod-source-record-manager to Java 17
* [MODSOURMAN-974](https://issues.folio.org/browse/MODSOURMAN-974) MARC bib $9 handling | Remove $9 subfields from linkable fields
* [MODSOURMAN-971](https://issues.folio.org/browse/MODSOURMAN-971) Adjust journal records population to create multiple journal records for each Holdings/Item
* [MODSOURMAN-1014](https://issues.folio.org/browse/MODSOURMAN-1014) Upgrade folio-kafka-wrapper to 3.0.0 version
* [MODDATAIMP-866](https://issues.folio.org/browse/MODDATAIMP-866) Add composite job types to support DI splitting workflow (bump interface `source-manager-job-executions` to version `3.3`)
* [MODSOURMAN-1044](https://issues.folio.org/browse/MODSOURMAN-1044) Adjust logs during marc-to-marc matching on central tenant

## 2023-02-24 v3.6.0
* [MODSOURMAN-873](https://issues.folio.org/browse/MODSOURMAN-873) Add MARC 720 field to default MARC Bib-Instance mapping and adjust relator term mapping
* [MODSOURMAN-837](https://issues.folio.org/browse/MODSOURMAN-837) MARC bib - FOLIO instance mapping | Update default mapping to change how Relator term is populated on instance record
* [MODSOURMAN-892](https://issues.folio.org/browse/MODSOURMAN-892) Logging improvement - Configuration
* [MODDATAIMP-736](https://issues.folio.org/browse/MODDATAIMP-736) Adjust logging configuration in all DI modules to display datetime in a proper format
* [MODSOURMAN-888](https://issues.folio.org/browse/MODSOURMAN-888) Link update: Implement API endpoint to retrieve mapping metadata by record type
* [MODSOURMAN-927](https://issues.folio.org/browse/MODSOURMAN-927) No title displays in import log when SRS MARC and Instance are not updated
* [MODSOURMAN-705](https://issues.folio.org/browse/MODSOURMAN-705) Logging improvement
* [MODSOURMAN-930](https://issues.folio.org/browse/MODSOURMAN-930) Add missed permissions for invoice data import flow
* [MODSOURMAN-928](https://issues.folio.org/browse/MODSOURMAN-928) MARC-to-MARC Holdings update log is unexpected
* [MODSOURMAN-831](https://issues.folio.org/browse/MODSOURMAN-831) Change the SRS MARC column in the summary section for EDIFACT logs
* [MODDATAIMP-758](https://issues.folio.org/browse/MODDATAIMP-758) Improve logging (hide SQL requests)
* [MODSOURMAN-890](https://issues.folio.org/browse/MODSOURMAN-890) The '2' number of Instance is displayed in cell in the row with the 'Updated' row header at the individual import job's log
* [MODSOURMAN-891](https://issues.folio.org/browse/MODSOURMAN-891) SRS MARC Created when No Create Action in Job Profile
* [MODSOURMAN-941](https://issues.folio.org/browse/MODSOURMAN-941) Add query param to allow filtering by fileName
* [MODSOURMAN-936](https://issues.folio.org/browse/MODSOURMAN-936) Add logic in Journal Handler for Post-Processing event
* [MODSOURMAN-948](https://issues.folio.org/browse/MODSOURMAN-948) Improve schema for the 'journal_records'-table to be able to import Orders
* [MODSOURMAN-937](https://issues.folio.org/browse/MODSOURMAN-937) Send DI_MARC_BIB_FOR_ORDER_CREATED event for Importing Orders
* [MODSOURMAN-932](https://issues.folio.org/browse/MODSOURMAN-932) Fill Journal Record info for Orders upon receiving DI_COMPLETED event
* [MODSOURMAN-946](https://issues.folio.org/browse/MODSOURMAN-946) Handle DI_ERROR event for POLines
* [MODSOURMAN-955](https://issues.folio.org/browse/MODSOURMAN-955) Include OrderId to the DTO that is used to display the json for POLine in DI log
* [MODSOURMAN-899](https://issues.folio.org/browse/MODSOURMAN-899) Do not process chunks when the DI is stopped
* [MODSOURMAN-961](https://issues.folio.org/browse/MODSOURMAN-961) Provide actual incoming records total amount on request jobExecution by id
* [MODSOURMAN-924](https://issues.folio.org/browse/MODSOURMAN-924) Mapping bib's $9 into subjects, series, alternativeTitles fields
* [MODSOURMAN-949](https://issues.folio.org/browse/MODSOURMAN-949) Add permissions for links update
* [MODSOURMAN-932](https://issues.folio.org/browse/MODSOURMAN-932) Fill Journal Record info for Orders upon receiving DI_COMPLETED event
* [MODDATAIMP-750](https://issues.folio.org/browse/MODDATAIMP-750) Update util dependencies
* [MODSOURMAN-939](https://issues.folio.org/browse/MODSOURMAN-939) Handle EDIFACT parsing exceptions - complete job with error
* [MODSOURMAN-939](https://issues.folio.org/browse/MODSOURMAN-939) Handle EDIFACT parsing exceptions - complete job with error

## 2022-10-24 v3.5.0
* [MODSOURMAN-858](https://issues.folio.org/browse/MODSOURMAN-858) Mapping bib's $9 into contributors.authorityId field
* [MODSOURMAN-838](https://issues.folio.org/browse/MODSOURMAN-838) Search by LCCN "010 $a" subfield value with "\" at the end don't retrieve results
* [MODSOURMAN-868](https://issues.folio.org/browse/MODSOURMAN-868) Allow sorting of JobExecutions by 'started_date'
* [MODSOURMAN-827](https://issues.folio.org/browse/MODSOURMAN-827) Updated instance mapping rules for 590 field to mark notes as staff only
* [MODSOURMAN-889](https://issues.folio.org/browse/MODSOURMAN-889) folio-di-support 1.6.0 fixing Spring4Shell CVE-2022-22965
* [MODSOURMAN-846](https://issues.folio.org/browse/MODSOURMAN-846) Optimize Data Access Patterns To Update Job Execution Progress
* [MODSOURMAN-828](https://issues.folio.org/browse/MODSOURMAN-828) Cache Metadata Snapshot of Data Import Job
* [MODSOURMAN-883](https://issues.folio.org/browse/MODSOURMAN-883) Upgrade to RMB v35.0.1
* [MODSOURMAN-822](https://issues.folio.org/browse/MODSOURMAN-822) Fix sorting for "Records" column
* [MODSOURMAN-887](https://issues.folio.org/browse/MODSOURMAN-887) Fix sorting for "Status" column
* [MODSOURMAN-885](https://issues.folio.org/browse/MODSOURMAN-885) Installation/migration: column jep1.jobexecutionid does not exist
* [MODSOURMAN-866](https://issues.folio.org/browse/MODSOURMAN-866) Assign each authority record to an Authority Source file list
* [MODSOURMAN-832](https://issues.folio.org/browse/MODSOURMAN-832) Upgrade Users interface to 16.0
* [MODSOURMAN-815](https://issues.folio.org/browse/MODSOURMAN-815) Support MARC-MARC Holdings update action

## 2022-09-20 v3.4.5
* [MODSOURMAN-874](https://issues.folio.org/browse/MODSOURMAN-874) Data import: fails the creation of a Holding through a match on the 999 ff field.

## 2022-09-06 v3.4.4
* [MODSOURMAN-870](https://issues.folio.org/browse/MODSOURMAN-870) Error while updating module after fix for schema differences between MG Bugfest and clean MG deployment

## 2022-09-05 v3.4.3
* [MODSOURMAN-854](https://issues.folio.org/browse/MODSOURMAN-854) Importing MARC Authority and Holdings records with 999 marc fields, with default Create job profiles, causes data problems.
* [MODSOURMAN-833](https://issues.folio.org/browse/MODSOURMAN-833) Schema differences between MG Bugfest and clean MG deployment: mod-source-record-manager
* [MODSOURMAN-859](https://issues.folio.org/browse/MODSOURMAN-859) DI stops processing the following files after using an incorrect type of JobProfile

## 2022-08-25 v3.4.2
* [MODSOURMAN-840](https://issues.folio.org/browse/MODSOURMAN-840) Importing MARC records with 999 ff fields using Create jobs without match profiles causes data problems.
* [MODSOURMAN-852](https://issues.folio.org/browse/MODSOURMAN-852) Fail job for unsupported profile (match MARC BIB to Instance and update MARC BIB)
* [MODSOURCE-521](https://issues.folio.org/browse/MODSOURCE-521) Populate 035 fields for exceptional cases of Update action

## 2022-08-03 v3.4.1
* [MODSOURMAN-818](https://issues.folio.org/browse/MODSOURMAN-818) Improve endpoints to get job executions profiles and users
* [MODSOURMAN-836](https://issues.folio.org/browse/MODSOURMAN-836) Return job users and profiles only for jobs with COMMITTED, ERROR, CANCELED statuses
* [MODSOURMAN-823](https://issues.folio.org/browse/MODSOURMAN-823) View all logs: broken alphabetical sorting via the "Job profile" column
* [MODSOURMAN-840](https://issues.folio.org/browse/MODSOURMAN-840) Importing MARC records with 999 ff fields using Create jobs without match profiles causes data problems.

## 2022-07-05 v3.4.0
* [MODSOURMAN-691](https://issues.folio.org/browse/MODSOURMAN-691) Support Delete MARC Authority Action
* [MODSOURMAN-707](https://issues.folio.org/browse/MODSOURMAN-707) Suppress Delete Authority job logs from Data Import log UI
* [MODSOURMAN-722](https://issues.folio.org/browse/MODSOURMAN-722) Journal does not show error status when importing EDIFACT
* [MODSOURMAN-724](https://issues.folio.org/browse/MODSOURMAN-724) SRM does not process and save error records
* [MODSOURMAN-727](https://issues.folio.org/browse/MODSOURMAN-727) Fix mapping for Authority 010 tag
* [MODSOURMAN-715](https://issues.folio.org/browse/MODSOURMAN-715) marc record type NA causes data-import to not complete
* [MODSOURMAN-732](https://issues.folio.org/browse/MODSOURMAN-732) Upgrade Vertx to 4.2.6
* [MODDATAIMP-472](https://issues.folio.org/browse/MODDATAIMP-472) EDIFACT files with txt file extensions do not import
* [MODSOURMAN-751](https://issues.folio.org/browse/MODSOURMAN-751) Improve sql query used by UI to know is processing completed
* [MODSOURMAN-756](https://issues.folio.org/browse/MODSOURMAN-756) Fix unnecessary No content logs in View All page when import fails
* [MODSOURMAN-763](https://issues.folio.org/browse/MODSOURMAN-763) Weird log display for a job that updates or creates
* [MODSOURMAN-767](https://issues.folio.org/browse/MODSOURMAN-767) Fix state is "In progress" after successful quickMarc update
* [MODSOURMAN-771](https://issues.folio.org/browse/MODSOURMAN-771) Provisioned for marking Job Executions to be deleted
* [MODSOURMAN-778](https://issues.folio.org/browse/MODSOURMAN-778) Add permission for Purchase Order Lines matching
* [MODSOURMAN-784](https://issues.folio.org/browse/MODSOURMAN-784) The status of instance is not updated in the Import log after uploading MARC file for modify
* [MODSOURMAN-785](https://issues.folio.org/browse/MODSOURMAN-785) JobExecutions APIs updated to filter out "Deleted" jobs
* [MODSOURMAN-786](https://issues.folio.org/browse/MODSOURMAN-786) To restrict update of JobExecution that is marked as "Deleted"
* [MODSOURMAN-780](https://issues.folio.org/browse/MODSOURMAN-780) Implement endpoint for adding summary for work accomplished in a job
* [MODSOURMAN-779](https://issues.folio.org/browse/MODSOURMAN-779) Add "CANCELLED" status for Import jobs that are stopped by users.
* [MODSOURMAN-791](https://issues.folio.org/browse/MODSOURMAN-791) Reduce Conversion of Parsed Content Into A MARC4J Record
* [MODSOURMAN-792](https://issues.folio.org/browse/MODSOURMAN-792) Initialize mapping parameters without race conditions
* [MODSOURMAN-790](https://issues.folio.org/browse/MODSOURMAN-790) Implement endpoint to get job executions users.
* [MODSOURMAN-796](https://issues.folio.org/browse/MODSOURMAN-796) Change logic to initialize job execution progress after reading file instead of processing the first chunk
* [MODSOURMAN-798](https://issues.folio.org/browse/MODSOURMAN-798) Change cache invalidation policy for LP data.
* [MODSOURMAN-795](https://issues.folio.org/browse/MODSOURMAN-795) Improve summary endpoint by parameter "errorsOnly".
* [MODSOURMAN-802](https://issues.folio.org/browse/MODSOURMAN-802) Block sending "Cancel" signal to finished task.
* [MODSOURMAN-805](https://issues.folio.org/browse/MODSOURMAN-805) Use exclusiveSubfield for authority rules
* [MODSOURMAN-808](https://issues.folio.org/browse/MODSOURMAN-808) Drop deprecated job_execution(s!) table.
* [MODSOURMAN-710](https://issues.folio.org/browse/MODSOURMAN-710) Improve performance of sql query for retrieving log data for json screen
* [MODSOURMAN-775](https://issues.folio.org/browse/MODSOURMAN-775) Logs show incorrectly formatted request id.
* [MODSOURMAN-810](https://issues.folio.org/browse/MODSOURMAN-810) Improve summary endpoint by parameter "entityType"
* [MODSOURMAN-811](https://issues.folio.org/browse/MODSOURMAN-811) Ensure proper work of flow control in multi-instances and multi-partitions envs
* [MODSOURMAN-813](https://issues.folio.org/browse/MODSOURMAN-813) Remove JobExecutionCache to improve progress bar on distributed envs
* [MODSOURMAN-814](https://issues.folio.org/browse/MODSOURMAN-814) Adjust totalRecords field for filtered jobLogEntries
* [MODSOURMAN-814](https://issues.folio.org/browse/MODSOURMAN-814) Send DI_MARC_FOR_UPDATE_RECEIVED event if job profile contains action for instance update
* [MODSOURMAN-806](https://issues.folio.org/browse/MODSOURMAN-806) Construct JournalRecord for DI_ERRORs even if there is no Record .

## 2022-04-08 v3.3.8
* [MODSOURMAN-768](https://issues.folio.org/browse/MODSOURMAN-768) Fix schema upgrade for SRM

## 2022-04-07 v3.3.7
* [MODSOURMAN-763](https://issues.folio.org/browse/MODSOURMAN-763) Weird log display for a job that updates or creates
* [MODSOURMAN-767](https://issues.folio.org/browse/MODSOURMAN-767) Fix state is "In progress" after successful quickMarc update
* [MODSOURMAN-762](https://issues.folio.org/browse/MODSOURMAN-762) Fix migration issues

## 2022-04-06 v3.3.6
* [MODSOURMAN-751](https://issues.folio.org/browse/MODSOURMAN-751) Improve sql query used by UI to know is processing completed
* [MODSOURMAN-756](https://issues.folio.org/browse/MODSOURMAN-751) Fix unnecessary No content logs in View All page when import fails

## 2022-04-01 v3.3.5
* [MODSOURMAN-752](https://issues.folio.org/browse/MODSOURMAN-752) Fix PgException while restore mapping rules

## 2022-04-01 v3.3.4
* [MODSOURMAN-749](https://issues.folio.org/browse/MODSOURMAN-749) Make it possible to restore default mapping rules for Authority records

## 2022-03-30 v3.3.3
* [MODSOURMAN-746](https://issues.folio.org/browse/MODSOURMAN-746) Avoid creation of trigger for old job progress table which cases an error during jobProgress saving

## 2022-03-29 v3.3.2
* [MODSOURMAN-728](https://issues.folio.org/browse/MODSOURMAN-728) Fix migration issue during upgrade
* [MODSOURMAN-741](https://issues.folio.org/browse/MODSOURMAN-741) Add missed `inventory-storage.holdings-sources.collection.get` permission

## 2022-03-26 v3.3.1
* [MODSOURMAN-722](https://issues.folio.org/browse/MODSOURMAN-722) Journal does not show error status when importing EDIFACT
* [MODSOURMAN-724](https://issues.folio.org/browse/MODSOURMAN-724) SRM does not process and save error records
* [MODSOURMAN-727](https://issues.folio.org/browse/MODSOURMAN-727) Fix mapping for Authority 010 tag
* [MODSOURMAN-715](https://issues.folio.org/browse/MODSOURMAN-715) marc record type NA causes data-import to not complete
* [MODSOURMAN-732](https://issues.folio.org/browse/MODSOURMAN-732) Upgrade Vertx to 4.2.6
* [MODDATAIMP-472](https://issues.folio.org/browse/MODDATAIMP-472) EDIFACT files with txt file extensions do not import

## 2022-03-03 v3.3.0
* [MODSOURMAN-694](https://issues.folio.org/browse/MODSOURMAN-694) Improve sql query for retrieving job execution sourcechunks
* [MODSOURMAN-695](https://issues.folio.org/browse/MODSOURMAN-695) Upgrade RMB and Vertx versions that contain fixes for the connection pool
* [MODINVOICE-356](https://issues.folio.org/browse/MODINVOICE-356) Fix progress bar stuck behaviour after the RecordTooLargeException
* [MODSOURMAN-624](https://issues.folio.org/browse/MODSOURMAN-624) Failed to handle DI_ERROR when 004 is invalid in MARC Holdings
* [MODSOURMAN-614](https://issues.folio.org/browse/MODSOURMAN-614) Authority: Add mapping rule for note types
* [MODSOURMAN-577](https://issues.folio.org/browse/MODSOURMAN-577) Optimistic locking: mod-source-record-manager modifications
* [MODSOURMAN-573](https://issues.folio.org/browse/MODSOURMAN-573) Create mapping rules for AUTHORITY records
* [MODDICORE-184](https://issues.folio.org/browse/MODDICORE-184) Update the MARC-Instance field mapping for InstanceType (336$a and $b)
* [MODSOURMAN-590](https://issues.folio.org/browse/MODSOURMAN-590) Save AUTHORITY rules to database
* [MODSOURMAN-595](https://issues.folio.org/browse/MODSOURMAN-595) "View all" and "Load more" buttons do not load all logs in Data Import
* [MODSOURMAN-570](https://issues.folio.org/browse/MODSOURMAN-570) Edit MARC Authorities via quickMARC | Handle events properly to Authorities
* [MODSOURMAN-594](https://issues.folio.org/browse/MODSOURMAN-594) Cannot build journal record when entity is empty
* [MODSOURMAN-228](https://issues.folio.org/browse/MODSOURMAN-228) Update the MARC-to-Instance mapping documentation
* [MODSOURMAN-605](https://issues.folio.org/browse/MODSOURMAN-605) Authority: update rules for name-title fields/properties
* [MODSOURMAN-619](https://issues.folio.org/browse/MODSOURMAN-619) Add an Authority info for Import Log for record page
* [MODSOURMAN-623](https://issues.folio.org/browse/MODSOURMAN-623) Generate IDs for Inventory authority
* [MODSOURMAN-598](https://issues.folio.org/browse/MODSOURMAN-598) Properly handle DB failures during events processing
* [MODDATAIMP-491](https://issues.folio.org/browse/MODDATAIMP-491) Improve logging to be able to trace the path of each record and file_chunks
* [MODDATAIMP-621](https://issues.folio.org/browse/MODDATAIMP-621) Fix saving of default mapping rules
* [MODDATAIMP-641](https://issues.folio.org/browse/MODDATAIMP-641) Fix NPE exception and fix the main reason of NPE (recordId = null).
* [MODSOURMAN-645](https://issues.folio.org/browse/MODSOURMAN-645) Update permissions related to Authority
* [MODSOURMAN-625](https://issues.folio.org/browse/MODSOURMAN-625) Invoice log detail sort by Record column in Data Import detail log not working properly
* [MODSOURMAN-432](https://issues.folio.org/browse/MODSOURMAN-432) Create job execution requires user with "personal" information
* [MODSOURMAN-656](https://issues.folio.org/browse/MODSOURMAN-656) Support Update MARC Authority Action
* [MODSOURMAN-638](https://issues.folio.org/browse/MODSOURMAN-638) Remove Kafka cache for StoreRecordsChunksKafkaHandler
* [MODSOURMAN-664](https://issues.folio.org/browse/MODSOURMAN-664) Remove Kafka cache for QuickMarcUpdateKafkaHandler
* [MODSOURMAN-639](https://issues.folio.org/browse/MODSOURMAN-639) Improve performance of saving journal records during import
* [MODSOURMAN-640](https://issues.folio.org/browse/MODSOURMAN-640) Remove Kafka cache for DataImportJournalKafkaHandler
* [MODSOURMAN-641](https://issues.folio.org/browse/MODSOURMAN-641) Remove Kafka cache by handling Constraint Violation Exceptions
* [MODDATAIMP-623](https://issues.folio.org/browse/MODDATAIMP-623) Remove Kafka cache initialization and Maven dependency
* [MODSOURMAN-668](https://issues.folio.org/browse/MODSOURMAN-668) Restructure job_execution_progress table for DataImportKafkaHandler
* [MODSOURMAN-675](https://issues.folio.org/browse/MODSOURMAN-675) Data Import handles repeated 020 $a:s in an unexpected manner when creating Instance Identifiers
* [MODSOURMAN-676](https://issues.folio.org/browse/MODSOURMAN-676) Provide Instance UUID for populating Inventory hotlinks for holdings/items
* [MODSOURMAN-682](https://issues.folio.org/browse/MODSOURMAN-682) Consume Authority log event
* [MODSOURMAN-699](https://issues.folio.org/browse/MODSOURMAN-699) Fix Can`t map 'RECORD' or/and 'MARC_BIBLIOGRAPHIC' statements from logs
* [MODSOURMAN-714](https://issues.folio.org/browse/MODSOURMAN-714) Legacy 999 (non-ff) fields cause data import failure
* [MODSOURMAN-719](https://issues.folio.org/browse/MODSOURMAN-719) The 001 is copied to the 035 when the record is updated even though it is unnecessary

## 2022-02-24 v3.2.9
* [MODSOURMAN-706](https://issues.folio.org/browse/MODSOURMAN-706) Error loading MappingParametersSnapshot

## 2022-02-09 v3.2.8
* [MODSOURMAN-688](https://issues.folio.org/browse/MODSOURMAN-688) Update folio-kafka-wrapper to v2.4.3 to allow adding ENV prefix to events_cache Kafka topic [KAFKAWRAP-19](https://issues.folio.org/browse/KAFKAWRAP-20)

## 2021-12-15 v3.2.7
* [MODSOURMAN-647](https://issues.folio.org/browse/MODSOURMAN-647) Failed to handle DI_ERROR when 004 is invalid in MARC Holdings
* [MODSOURMAN-646](https://issues.folio.org/browse/MODSOURMAN-646) Log4j vulnerability correction

## 2021-12-03 v3.2.6
* [MODSOURMAN-621](https://issues.folio.org/browse/MODSOURMAN-621) Fix saving of default mapping rules

## 2021-11-12 v3.2.5
* [MODSOURMAN-594](https://issues.folio.org/browse/MODSOURMAN-594) Cannot build journal record when entity non match

##2021-11-10 v3.2.4
* [MODSOURMAN-595](https://issues.folio.org/browse/MODSOURMAN-595) "View all" and "Load more" buttons do not load all logs in Data Import

## 2021-10-29 v3.2.3
* [MODSOURMAN-522](https://issues.folio.org/browse/MODSOURMAN-522) Fix the effect of DI_ERROR messages when trying to duplicate records on the import job progress bar
* [MODDICORE-184](https://issues.folio.org/browse/MODDICORE-184) Update the MARC-Instance field mapping for InstanceType (336$a and $b)
* Updated data-import-processing-core to v3.2.2

## 2021-10-19 v3.2.2
* [MODSOURMAN-586](https://issues.folio.org/browse/MODSOURMAN-586) Adjust mapping metadata snapshots creation

## 2021-10-13 v3.2.1
* [MODSOURMAN-583](https://issues.folio.org/browse/MODSOURMAN-583) Schema name can't replace in snippet (schema.json)

## 2021-10-06 v3.2.0
* [MODSOURMAN-516](https://issues.folio.org/browse/MODSOURMAN-516) Send QM_COMPLETED event after processing finished
* [MODSOURMAN-517](https://issues.folio.org/browse/MODSOURMAN-517) Change quickMarc producers not to close after message
  sent
* [MODSOURMAN-524](https://issues.folio.org/browse/MODSOURMAN-524) Support MARC Holdings
* [MODSOURMAN-533](https://issues.folio.org/browse/MODSOURMAN-533) Upgrade to RAML Module Builder 33.x
* Improved logging
* Apply new version of clients generated by mod-data-import-converter-storage with updated Raml version
* [KAFKAWRAP-5](https://issues.folio.org/browse/KAFKAWRAP-5) Add mechanism for detection and logging inability to
  create/connect Kafka consumers.
* [MODSOURMAN-550](https://issues.folio.org/browse/MODSOURMAN-550) Reduce BE response payload for DI Landing Page to
  increase performance
* [MODSOURMAN-540](https://issues.folio.org/browse/MODSOURMAN-540) Add default mapping profile for MARC holdings
* [MODSOURMAN-541](https://issues.folio.org/browse/MODSOURMAN-541) Update existing CLI endpoint GET /mapping-rules to
  support MARC Holdings default rules
* [MODSOURMAN-542](https://issues.folio.org/browse/MODSOURMAN-542) Update existing CLI endpoint PUT /mapping-rules to
  support MARC Holdings default rules
* [MODSOURMAN-543](https://issues.folio.org/browse/MODSOURMAN-543) Update existing CLI endpoint PUT
  /mapping-rules/restore to support MARC Holdings default rules
* [MODSOURMAN-547](https://issues.folio.org/browse/MODSOURMAN-547) Update cache for mapping-rules to support MARC Holdings default rules
* [MODSOURMAN-553](https://issues.folio.org/browse/MODSOURMAN-553) Update GET /change-manager/parsedRecords to have externalId param
* [MODSOURMAN-526](https://issues.folio.org/browse/MODSOURMAN-526) Verify persist value in DB during parsing 004 field
* [MODSOURMAN-544](https://issues.folio.org/browse/MODSOURMAN-544) Validate MARC Holdings 004 field from MARC Bib HRID
* [MODSOURMAN-546](https://issues.folio.org/browse/MODSOURMAN-546) Support edit Holdings via quickMarc
* [MODSOURMAN-464](https://issues.folio.org/browse/MODSOURMAN-464) Store snapshots of MappingRules and MappingParams to the database
* [MODSOURMAN-563](https://issues.folio.org/browse/MODSOURMAN-563) Add MARC-Instance field mapping for Cancelled system control number
* [MODSOURMAN-465](https://issues.folio.org/browse/MODSOURMAN-465) Remove MappingRules, MappingParams, and JobProfileSnapshot from the event payload
* [MODSOURMAN-466](https://issues.folio.org/browse/MODSOURMAN-466) Remove zipping mechanism for data import event payloads
* [MODDICORE-172](https://issues.folio.org/browse/MODDICORE-172)  Add MARC-Instance field mapping for New identifier types

## 2021-08-04 v3.1.3
* [MODDICORE-166](https://issues.folio.org/browse/MODDICORE-166)  Near the day boundary data import calculates today incorrectly.
* [MODSOURMAN-535](https://issues.folio.org/browse/MODSOURMAN-535) Data import can't retrieve location with code "olin".
* [MODPUBSUB-187](https://issues.folio.org/browse/MODPUBSUB-187) Add support for max.request.size configuration for Kafka messages
* Update data-import-processing-core dependency to v3.1.4
* Update folio-kafka-wrapper dependency to v3.1.4

## 2021-07-21 v3.1.2
* [MODSOURMAN-513](https://issues.folio.org/browse/MODSOURMAN-513) (Juniper) Data import stopped process before finishing: deadlock for "job_monitoring"
* [MODSOURMAN-508](https://issues.folio.org/browse/MODSOURMAN-508) Log details for Inventory single record imports for Overlays
* [MODSOURMAN-527](https://issues.folio.org/browse/MODSOURMAN-527) Cannot import EDIFACT invoices
* Update data-import-processing-core dependency to v3.1.3

## 2021-06-25 v3.1.1
* [MODSOURMAN-497](https://issues.folio.org/browse/MODSOURMAN-497) Apply MarcRecordAnalyzer to determine MARC related specific type
* [MODSOURMAN-501](https://issues.folio.org/browse/MODSOURMAN-501) Change dataType to have have common type for MARC related subtypes
* Update data-import-processing-core dependency to v3.1.2

## 2021-06-17 v3.1.0
* [MODSOURMAN-411](https://issues.folio.org/browse/MODSOURMAN-411) Dynamically define the payload of DI event depending on MARC record type (Bib, Authority, Holding)
* [MODSOURMAN-448](https://issues.folio.org/browse/MODSOURMAN-448) Update default field mapping for 647 field
* [MODSOURMAN-453](https://issues.folio.org/browse/MODSOURMAN-453) Add index for the "job_execution_source_chunks"
* [MODSOURMAN-461](https://issues.folio.org/browse/MODSOURMAN-461) Data Import fails (no details about cause of failure in ui/log)
* [MODSOURMAN-471](https://issues.folio.org/browse/MODSOURMAN-471) Migrate QM-flow to Kafka
* [MODSOURMAN-480](https://issues.folio.org/browse/MODSOURMAN-471) Create jobs with match profiles that include records with 999 fields cause errors in the srs-instance relationship
* [MODSOURMAN-477](https://issues.folio.org/browse/MODSOURMAN-477) Store MARC Authority record
* [MODSOURMAN-458](https://issues.folio.org/browse/MODSOURMAN-458) Support monitoring table creation and data insertion
* [MODSOURMAN-460](https://issues.folio.org/browse/MODSOURMAN-460) Implement watchdog timer to monitor table
* [MODSOURMAN-485](https://issues.folio.org/browse/MODSOURMAN-485) Update interface version

## 2021-06-18 v3.0.8
* [MODSOURCE-301](https://issues.folio.org/browse/MODSOURCE-301) Cannot import GOBI EDIFACT invoice
* [MODSOURMAN-454](https://issues.folio.org/browse/MODSOURMAN-454) Excessive CPU usage in a system with no user activity
* [MODSOURMAN-456](https://issues.folio.org/browse/MODSOURMAN-456) Ignore event that is not supposed to be saved to data import journal
* [MODSOURMAN-458](https://issues.folio.org/browse/MODSOURMAN-458) Support monitoring table creation and data insertion
* [MODSOURMAN-460](https://issues.folio.org/browse/MODSOURMAN-460) Implement watchdog timer to monitor table

## 2021-05-28 v3.0.7
* [MODSOURMAN-480](https://issues.folio.org/browse/MODSOURMAN-480) Create jobs with match profiles that include records with 999 fields cause errors in the srs-instance relationship

## 2021-05-22 v3.0.6
* [MODSOURMAN-457](https://issues.folio.org/browse/MODSOURMAN-457) Issue with Database migration for Iris release
* [MODSOURCE-278](https://issues.folio.org/browse/MODSOURCE-278) Move logging of the records creation information to the handler receiving saved records
* [MODSOURCE-295](https://issues.folio.org/browse/MODSOURCE-295) Set instanceHrid to externalIdsHolder when 999ff$i is present

## 2021-04-22 v3.0.5
* [MODSOURMAN-420](https://issues.folio.org/browse/MODSOURMAN-420) Expand endpoint for retrieving recordProcessingLogDto to provide data for Invoice JSON screen
* [MODSOURMAN-437](https://issues.folio.org/browse/MODSOURMAN-437) Add logging for event correlationId
* [MODSOURMAN-442](https://issues.folio.org/browse/MODSOURMAN-442) Add indices for the job_executions table in srm
* [MODSOURMAN-441](https://issues.folio.org/browse/MODSOURMAN-441) Update QM-event with user context

##2021-04-14 v3.0.4
* [MODSOURMAN-436](https://issues.folio.org/browse/MODSOURMAN-436) Slow Query Invoked on DI Home Page

## 2021-04-12 v3.0.3
* [MODSOURMAN-419](https://issues.folio.org/browse/MODSOURMAN-419) SQL Exception WRT count function
* [MODSOURMAN-428](https://issues.folio.org/browse/MODSOURMAN-428) Ensure exactly one delivery approach for handler receiving stored records
* [MODSOURMAN-430](https://issues.folio.org/browse/MODSOURMAN-430) Ensure exactly one delivery approach for for data import log handler

## 2021-04-05 v3.0.2
* [MODSOURMAN-429](https://issues.folio.org/browse/MODSOURMAN-429) Add permission to /change-manager/jobExecutions/{id}/jobProfile

## 2021-03-28 v3.0.1
* [MODSOURMAN-421](https://issues.folio.org/browse/MODSOURMAN-421) Syntax problem for 561 field in default mapping rules
* [MODSOURMAN-422](https://issues.folio.org/browse/MODSOURMAN-422) Add record sequence number for records posted direct via API if it is not set
* [MODDATAIMP-388](https://issues.folio.org/browse/MODDATAIMP-388) Import job is not completed on file parsing error

## 2021-03-18 v3.0.0
* [MODSOURMAN-338](https://issues.folio.org/browse/MODSOURMAN-338) Change chunk processing to use kafka
* [MODSOURCE-177](https://issues.folio.org/browse/MODSOURCE-177) Use kafka to save chunks of parsed records in SRS
* [MODSOURMAN-380](https://issues.folio.org/browse/MODSOURMAN-380) Expand journalRecord entity with "title" property
* [MODSOURMAN-385](https://issues.folio.org/browse/MODSOURMAN-385) Enable OCLC update processing
* [MODSOURMAN-381](https://issues.folio.org/browse/MODSOURMAN-381) Add endpoint to retrieve a list of jobLogEntryDto
* [MODSOURMAN-382](https://issues.folio.org/browse/MODSOURMAN-382) Fill title from marc record into journalRecord entity
* [MODSOURMAN-383](https://issues.folio.org/browse/MODSOURMAN-383) Implement endpoint to retrieve a recordProcessingLogDto.
* [MODDICORE-114](https://issues.folio.org/browse/MODDICORE-114) Add MARC-Instance default mappings for 880 fields .
* [MODDSOURMAN-377](https://issues.folio.org/browse/MODSOURMAN-377) Update 5xx Notes mappings to indicate staff only for some notes.
* [MODDSOURMAN-402](https://issues.folio.org/browse/MODSOURMAN-402) Upgrade to RAML Module Builder 32.x.
* [MODDSOURMAN-409](https://issues.folio.org/browse/MODSOURMAN-409) Make tenant API asynchronous.
* [MODDSOURMAN-395](https://issues.folio.org/browse/MODSOURMAN-395) Add personal data disclosure form.
* [MODSOURMAN-384](https://issues.folio.org/browse/MODSOURMAN-384) Implement writing entities processing information to the log
* [MODSOURCE-248](https://issues.folio.org/browse/MODSOURCE-248) Incoming MARC Bib with 003, but no 001 should not create an 035[BUGFIX]
* [MODSOURMAN-410](https://issues.folio.org/browse/MODSOURMAN-410) Expand data import log functionality for EDIFACT records.
* [MODSOURMAN-414](https://issues.folio.org/browse/MODSOURMAN-414) Add record sequence number to the summary log screen for OCLC single record imports
* [MODDATAIMP-370](https://issues.folio.org/browse/MODDATAIMP-370) OCLC single record import: Updates don't work, and the Create action uses the wrong job profile

## 2020-11-20 v2.4.3
* [MODSOURCE-213](https://issues.folio.org/browse/MODSOURCE-213) MARC updates for selected fields is not working
* [MODSOURMAN-374](https://issues.folio.org/browse/MODSOURMAN-374) Fixed permissions for stuck job deletion interface 

## 2020-10-30 v2.4.2
* [MODSOURMAN-362](https://issues.folio.org/browse/MODSOURMAN-362) Mark job status with error when at least one record has not been parsed
* [MODSOURMAN-339](https://issues.folio.org/browse/MODSOURMAN-339) Disable CQL2PgJSON & CQLWrapper extra logging in mod-source-record-manager
* [MODSOURMAN-369](https://issues.folio.org/browse/MODSOURMAN-369) Upgrade to RMB v31.1.5

## 2020-10-26 v2.4.1
* [MODSOURMAN-363](https://issues.folio.org/browse/MODSOURMAN-363) Fix permissions issues
* Update mod-pubsub-client to v1.3.1

## 2020-08-17 v2.4.0
* [MODSOURMAN-344](https://issues.folio.org/browse/MODSOURMAN-344) Fixed default MARC Bib-Instance mapping for 024 and 028 fields
* [MODSOURMAN-340](https://issues.folio.org/browse/MODSOURMAN-340) MARC field sort into numerical order when record is imported
* [MODSOURMAN-345](https://issues.folio.org/browse/MODSOURMAN-345) 003 handling in SRS for MARC Bib records: Create
* [MODSOURMAN-346](https://issues.folio.org/browse/MODSOURMAN-346) Load MARC field protection settings to Mapping params
* [MODDATAIMP-324](https://issues.folio.org/browse/MODDATAIMP-324) Update all Data-Import modules to the new RMB version
* [MODINV-296](https://issues.folio.org/browse/MODINV-296) Added support for journalRecord saving on protected item status update
* [MODSOURMAN-361](https://issues.folio.org/browse/MODSOURMAN-361) Add capability to remove jobs that are stuck

## 2020-08-10 v2.3.2
* [MODSOURMAN-322](https://issues.folio.org/browse/MODSOURMAN-322) Add source-record states storing mechanism for QM edit workflow
* [MODSOURMAN-333](https://issues.folio.org/browse/MODSOURMAN-333) Replace incoming 999 ff fields if file is re-imported

## 2020-07-10 v2.3.1
* [MODSOURMAN-329](https://issues.folio.org/browse/MODSOURMAN-329) Set completed date on error status update for JobExecution

## 2020-06-25 v2.3.0
* [MODSOURMAN-325](https://issues.folio.org/browse/MODSOURMAN-325) Update SRS client requests for v4
* [MODSOURMAN-324](https://issues.folio.org/browse/MODSOURMAN-324) Hardcode JobProfile for importing records using CLI tool.

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
