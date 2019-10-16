INSERT INTO ${myuniversity}_${mymodule}.JOB_EXECUTIONS (_id, jsonb) values
('e451348f-2273-4b45-b385-0a8b0184b4e1', '{
    "jobProfileInfo": {
      "id": "448ae575-daec-49c1-8041-d64c8ed8e5b1",
      "name": "Authority updates",
      "dataType": "MARC"
    },
    "subordinationType": "PARENT_SINGLE",
    "sourcePath": "import7.marc",
    "fileName": "import7.marc",
    "id": "e451348f-2273-4b45-b385-0a8b0184b4e1",
    "hrId": "112984432",
    "status": "PROCESSING_FINISHED",
    "uiStatus": "READY_FOR_PREVIEW",
    "runBy": {
      "firstName": "John",
      "lastName": "Doe"
    },
    "progress": {
      "current": 20000,
      "total": 20000
    },
    "startedDate": "2018-11-20T23:26:44.000",
    "completedDate": "2018-11-21T00:38:44.000"
  }'),
('4030c4ee-5578-4727-a77f-86a7a39d1960', '{
    "jobProfileInfo": {
      "id": "295e28b4-aea2-4458-9073-385a31e1da05",
      "name": "Authority updates",
      "dataType": "MARC"
    },
    "subordinationType": "PARENT_SINGLE",
    "sourcePath": "import8.marc",
    "fileName": "import8.marc",
    "id": "4030c4ee-5578-4727-a77f-86a7a39d1960",
    "hrId": "112974998",
    "status": "PROCESSING_FINISHED",
    "uiStatus": "READY_FOR_PREVIEW",
    "runBy": {
      "firstName": "John",
      "lastName": "Lennon"
    },
    "progress": {
      "current": 1000,
      "total": 1000
    },
    "startedDate": "2018-11-20T23:38:44.000",
    "completedDate": "2018-11-20T23:50:44.000"
  }'),
('80632ccd-0967-4e8c-8951-d9f448a68da1', '{
    "jobProfileInfo": {
      "id": "4d1b5024-2c49-42bd-b781-4330d14cefb0",
      "name": "Standard BIB Import",
      "dataType": "MARC"
    },
    "subordinationType": "PARENT_SINGLE",
    "sourcePath": "importBIB017.marc",
    "fileName": "importBIB017.marc",
    "id": "80632ccd-0967-4e8c-8951-d9f448a68da1",
    "hrId": "222984498",
    "status": "PROCESSING_FINISHED",
    "uiStatus": "READY_FOR_PREVIEW",
    "runBy": {
      "firstName": "Caleb",
      "lastName": "Hunter"
    },
    "progress": {
      "current": 24000,
      "total": 24000
    },
    "startedDate": "2018-10-09T22:26:12.000",
    "completedDate": "2018-11-21T00:01:17.000"
  }'),
('4680c2b1-2002-42d0-9480-4dd8025780b4', '{
    "jobProfileInfo": {
      "id": "b32e79bc-01d9-4d31-bc08-a3621fcfc1aa",
      "name": "BIB Import from Boston",
      "dataType": "MARC"
    },
    "subordinationType": "PARENT_SINGLE",
    "sourcePath": "importBoston.marc",
    "fileName": "importBoston.marc",
    "id": "4680c2b1-2002-42d0-9480-4dd8025780b4",
    "hrId": "143274991",
    "status": "PROCESSING_FINISHED",
    "uiStatus": "READY_FOR_PREVIEW",
    "runBy": {
      "firstName": "Taylor",
      "lastName": "Edwards"
    },
    "progress": {
      "current": 1000,
      "total": 1000
    },
    "startedDate": "2018-11-20T23:38:44.000",
    "completedDate": "2018-11-20T23:50:44.000"
  }') ON CONFLICT DO NOTHING;
