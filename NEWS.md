## 2018-11-19 v1.0.0
 * Module structure reorganization 
 (now module structure the same as for mod-data-import and mod-source-record-storage)
 * Generic DAO interface
 * Conversion JobExecution entity to DTO
 * Rest tests
 * Rest API
 * CRUD API for rules and password: 

  | METHOD |             URL                   | DESCRIPTION                                        |
  |--------|-----------------------------------|----------------------------------------------------|
  | GET    | /metadata-provider/jobExecutions  | Get list of the JobExecutions DTO by query         |
  | GET    | /metadata-provider/logs           | Get list of the Log entities by query              |

## 2018-10-02 v0.0.1
 * Initial module setup
