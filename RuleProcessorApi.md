### Introduction
The process of converting a MARC record into an Instance object is usually called **MARC-to-Instance mapping**. 

Conversion logic is defined by mapping rules and these rules are described in JSON. Mapping rules basically have functions for data normalization (trimming leading whitespaces, removing slashes, removing ending punctuation) and the assigning target Inventory fields to the values of the incoming MARC record.
#### *This document describes structure of rules, flags, real use cases and REST API to work with.*
#
### What is a mapping rule
Basically, a mapping rule is a simple key-value JSON element. The key serves a MARC record's field (tag). The value is a rule configuration.
```json
Rule:
{
  "001":[
    {
      "target":"hrid",
      "description":"The human readable ID"
    }
  ]
}
```
This rule belongs to the "001" field and handles all the "001" fields from incoming record. It takes value from "001" field and puts it into Instance "hrid" field.

SRM has own schema for Inventory Instance. The schema describes target fields, data types, restrictions and other internal details of Instance. Please, be careful while writing rules - you can put into "target" only fields from the schema, take a look at the  [Instance schema](https://github.com/folio-org/data-import-processing-core/blob/master/ramls/instance.json) for clear understanding. If the "target" field is specified wrong then the RuleProcessor does not take this rule for mapping.

##### NOTE:  MARC records and mapping rules below have been taken intentionally just for demonstration purposes.
#
#### Normalization functions
In most cases the record value needs to be normalized before being imported into an Instance field, because MARC record data is often raw and mixed. For this purpose we have to use such structure:
 ```json
MARC Record: "001": "393/89/3"
```
```json
Rule:
  "001":[
    {
      "target":"hrid",
      "description":"The human readable ID",
      "rules":[
        {
          "conditions":[
            {
              "type":"remove_substring",
              "parameter":{
                "substring":"/"
              }
            }
          ]
        }
      ]
    }
  ]
```
`remove_substring` is normalization function that removes the given substring from the field’s value. The function is just doing a job and returns a string that ends up in the Instance “hrid” field.
An outcome Instance looks like this in Json:
```json
Instance: 
{
  "hrid": "393893"
}
```

[Here](https://github.com/folio-org/data-import-processing-core/blob/master/src/main/java/org/folio/processing/mapping/defaultmapper/processor/functions/NormalizationFunction.java) are all of the formatting functions defined. The most useful ones are: `trim, capitalize, remove_ending_punc`.

In most cases there are subfields present within MARC fields. This is important for mapping. Here is an example of a MARC “250” field that contains a “subfield a”, a “subfield b”, and a “subfield 6.”
 ```json
MARC Record: "250":{"ind1":"", "ind2":"", "subfields":[ { "a":" fifth ed." }, { "b":"Editor in chief Lord Mackay of Clashfern. " } , {"6":"880-02"}]}
```
```json
Rule:
  "250":[
    {
      "target":"edition",
      "description":"Edition",
      "subfield":["a", "b"],
      "rules":[
        {
          "conditions":[
            {
              "type":"capitalize, trim"
            }
          ]
        }
      ]
    }
  ]
```
The RuleProcessor triggers the rule above only for each allowed subfield ("a" and "b"), that causes a call for normalization functions ("capitalize" and "trim"). Subfield "6" is not allowed, the RuleProcessor just skips calling normalization functions for "6" subfield. 
The result is concatenated in one string and written to the Instance "edition" field. An outcome Instance looks like this in Json:
```json
Instance:
{
  "edition": "Fifth ed. Editor in chief Lord Mackay of Clashfern."
}
```

#### Mapping for complex fields
Sometimes you can encounter Instance fields with sub-fields, for example, Electronic access (sub-fields: linked text, public note, uri), Contributors (name, type, primary), Publication (place, publisher, date of publication).
To map "264" record into the "place of publication" we have to use period-delimited syntax: `"target": "publication.place"`. Period serves a sub-field delimiter to reach a "place" sub-field of the "publication" field.
 We can write the mapping rules for the Publication field as shown below:
 ```json
MARC Record: "264":{"subfields":[{"a":"Chicago, Illinois :"}, {"b":"The HistoryMakers,"}, {"c":"[2016]"}], "ind1":" ", "ind2":"1"}
```
```json
Rule:
"264": [
    {
      "target": "publication.place",
      "description": "Place of publication",
      "subfield": ["a"],
      "rules": []
    },
    {
      "target": "publication.publisher",
      "description": "Publisher of publication",
      "subfield": ["b"],
      "rules": []
    },
    {
      "target": "publication.dateOfPublication",
      "description": "Date of publication",
      "subfield": ["c"],
      "rules": []
    }
]
```
An outcome Instance looks like this in Json:
```json
Instance:
{
  "publication":[
    {
      "place":"Chicago, Illinois :",
      "publisher":"The HistoryMakers,",
      "dateOfPublication":"[2016]"
    }
  ]
}
```

If there are repeated MARC fields in a single record, then the Instance will receive several elements in its target field. To skip mapping for repeated fields and take only the first occurrence, we can use the `ignoreSubsequentFields` flag:
 ```json
MARC Record:
"336":{"subfields":[{"a":"text"}, {"b":"txt"}, {"2":"rdacontent"}], "ind1":" ", "ind2":" "},  ...
"336":{"subfields":[{"a":"performed music"}, {"b":"prm"}, {"2":"rdacontent"}], "ind1":" ", "ind2":" "}
```
```json
Rule:
"336": [
    {
      "target": "instanceTypeId",
      "description": "Instance type ID",
      "ignoreSubsequentFields": true,
      "subfield": ["b"],
      "rules": []
    }
  ]
```
An outcome Instance looks like this in Json:
```json
Instance:
{
  "instanceTypeId": "txt"
}
```

#### Multiple objects from one field
Usually, the [Rule Processor](https://github.com/folio-org/data-import-processing-core/blob/master/src/main/java/org/folio/processing/mapping/defaultmapper/processor/Processor.java) creates only one instance of the 'target' field for each record field. What if we need to create several objects from single record field ?
##### New object for group of sub-fields
In the example below we map several 'publication' elements from a single "264" record field. To do so we have to wrap a rule structure into an `entity`. Let's consider the example below:
 ```json
MARC Record:
  "264": {
    "subfields":[
         {"a":"Chicago, Illinois :"}, 
         {"b":"The HistoryMakers,"}, 
         {"c":"[2016]"}, 
         {"f":"Nashville, Tennessee"}, 
         {"g":"Revenant Records"}, 
         {"h":"[2015]"}
    ], 
    "ind1":" ", 
    "ind2":"1"
    }
```
```json
Rule:
  "264": [
    {
      "entity": [
        {
          "target": "publication.place",
          "description": "Place of publication",
          "subfield": ["a"],
          "rules": []
        },
        {
          "target": "publication.publisher",
          "description": "Publisher of publication",
          "subfield": ["b"],
          "rules": []
        },
        {
          "target": "publication.dateOfPublication",
          "description": "Date of publication",
          "subfield": ["c"],
          "rules": []
        }
      ]
    },
    {
      "entity": [
        {
          "target": "publication.place",
          "description": "Place of publication",
          "subfield": ["f"],
          "rules": []
        },
        {
          "target": "publication.publisher",
          "description": "Publisher of publication",
          "subfield": ["g"],
          "rules": []
        },
        {
          "target": "publication.dateOfPublication",
          "description": "Date of publication",
          "subfield": ["h"],
          "rules": []
        }
      ]
    }
  ]
```
An outcome Instance looks like this in Json:
```json
Instance:
{
  "publication":[
    {
      "place":"Chicago, Illinois :",
      "publisher":"The HistoryMakers,",
      "dateOfPublication":"[2016]"
    },
    {
      "place":"Nashville, Tennessee :",
      "publisher":"Revenant Records,",
      "dateOfPublication":"[2015]"
    }
  ]
}
```
##### New object per repeated sub-fields
If there are several repeated sub-fields in one single record, then `entity` will concatenate them. To create a new object per each sub-field even if they are repeated, we can use `entityPerRepeatedSubfield` flag:
 ```json
MARC Record:
  "264": {
    "subfields":[
         {"a":"Chicago, Illinois :"},
         {"a":"Nashville, Tennessee"},
         {"f": "Austin Texas"}
    ], 
    "ind1":" ", 
    "ind2":"1"
    }
```
```json
Rule:
  "264": [
    {
      "entityPerRepeatedSubfield": true,
      "entity": [
        {
          "target": "publication.place",
          "description": "Place of publication",
          "subfield": ["a", "f"],
          "rules": []
        },
        {
          "target": "publication.publisher",
          "description": "Publisher of publication",
          "subfield": ["a", "f"],
          "rules": [
            {
              "conditions": [],
              "value": "STUB publisher"
            }
          ]
        },
        {
          "target": "publication.dateOfPublication",
          "description": "Date of publication",
          "subfield": ["a", "f"],
          "rules": [
            {
              "conditions": [],
              "value": "STUB date"
            }
          ]
        }
      ]
    }
  ]
```
An outcome Instance looks like this in Json:
```json
Instance:
{
  "publication":[
    {
      "place":"Chicago, Illinois :",
      "publisher":"STUB publisher,",
      "dateOfPublication":"STUB date"
    },
    {
      "place":"Nashville, Tennessee :",
      "publisher":"STUB publisher",
      "dateOfPublication":"STUB date"
    },
    {
      "place":"Austin Texas",
      "publisher":"STUB publisher",
      "dateOfPublication":"STUB date"
    }
  ]
}
```

#### Processing rules on concatenated data
By default rules run on the data in a single sub-field. In order to concatenate un-normalized data, and run the rules on the concatenated data add the following field: `applyRulesOnConcatedData: true`. This can be used when punctuation should only be removed from the end of a concatenated string.
```json
Rule:
"500": [
    {
      "subfield": ["a"],
      "applyRulesOnConcatenatedData": true
    }
]
```

#### Delimiting sub-fields
Grouping sub-fields `"subfield": [ "a", "f"]` will concatenate (space delimited) the values in those sub-fields and place the result in the target. However, if there is a need to declare different delimiters per set of sub-fields, the following can be declared using the `subFieldDelimiter` array:
```json
MARC Record:
  "264": {
    "subfields":[
         {"a":"Chicago, Illinois"},
         {"a":"Nashville, Tennessee"},
         {"f": "Austin Texas"}
    ], 
    "ind1":" ", 
    "ind2":"1"
    }
```
```json
Rule: 
  "265": [
    {
      "target": "publication.place",
      "subfield": ["a","f"],
      "description": "Place of publication",
      "subFieldDelimiter": [
        {
          "value": " & ",
          "subfields": ["a","f"]
        }
      ],
      "rules": []
    }
  ]
```
An outcome Instance looks like this in Json:
```json
Instance: 
{
  "publication":[
    {
      "place":"Chicago, Illinois & Nashville, Tennessee & Austin Texas"
    }
  ]
}
```

####  Required sub-fields concatenation
If there is a need to concat special subfields to processed one, its needed to add to the rule `subfieldsToConcat` array
and list subfields names to be concatenated and use `concat_subfields_by_name` normalization function:
```json
MARC Record:
  "024": {
    "subfields":[
         {"a":"Chicago, Illinois"},
         {"c":"Nashville, Tennessee"},
         {"f": "Austin Texas"}
    ], 
    "ind1":"2", 
    "ind2":"1"
    }
```
```json
  Rule:
  "024": [{
    "target": "identifiers.value",
    "description": "Invalid UPC",
    "subfield": ["a"],
    "rules": [{
      "conditions": [{
        "type": "concat_subfields_by_name",
          "parameter": {
            "subfieldsToConcat": [
              "f"
            ]
          }
        }]
      }]
  }]
```
An outcome Instance looks like this in Json:
```json
Instance: 
{
  "identifiers":[
    {
      "value":"Chicago, Illinois Austin Texas"
    }
  ]
}
```

In case you have to stop the concatenation before some subfields, its needed to add the parameter `subfieldsToStopConcat`
and a list of subfields before which the concatenation should stop:
```json
MARC Record:
  "020": {
    "subfields":[
         {"a":"a9780471622673"},
         {"c":"(acid-free paper)"},
         {"z": "z0471622672"},
         {"q":"(acid-free paper)"}
    ], 
    "ind1":" ", 
    "ind2":" "
    }
```
```json
  Rule:
  "020": [{
    "target": "identifiers.value",
    "description": "Valid ISBN",
    "subfield": ["a"],
    "rules": [{
      "conditions": [{
        "type": "concat_subfields_by_name, remove_ending_punc, trim",
          "parameter": {
            "subfieldsToConcat": [
              "c", "q"
            ],
            "subfieldsToStopConcat": [
              "z"  
            ]
          }
        }]
      }]
  }]
```

An outcome Instance looks like this in Json:
```json
Instance: 
{
  "identifiers":[
    {
      "value":"a9780471622673 (acid-free paper)"
    }
  ]
}
```

#### Required sub-fields
Sometimes the existence of a MARC subfield will dictate whether or not a target field is presented in Inventory. We use `requiredSubfield` to define the required subfield needed to trigger the appearance of a target field. In this example, the presence of an 020 subfield "z" in a MARC record is needed in order for the target field, “Invalid ISBN” to appear in the Inventory record.
```json
MARC Record:
{
  "020":{
    "subfields":[
      {
        "z":"9780190494889"
      },
      {
        "q":"hardcover ;"
      },
      {
        "c":"alkaline paper"
      }
    ],
    "ind1":" ",
    "ind2":" "
  }
}
```
```json
Rule:
"020": [
    {
      "entity": [
        {
          "target": "identifiers.value",
          "description": "Invalid ISBN",
          "subfield": ["z","q","c"],
          "requiredSubfield": ["z"],
          "rules": []
        }
      ]
    }
  ]
```

- If there is no "z" in record sub-fields then target field does not appear at all.
- If there is "z" among record sub-fields then target field gets filled by all the `["z","q","c"].`

#### Exclusive sub-fields
Sometimes the existence of a MARC subfield will dictate whether or not a target field is presented in Inventory. We use `exclusiveSubfield` to define the exclusive subfield needed to NOT trigger the appearance of a target field. In this example, the presence of an 100 subfield "t" in a MARC record is needed in order for the target field, “personalName” to not appear in the Inventory record, even if other subfields are present.
```json
MARC Record:
{
  "200":{
    "subfields":[
      {
        "a":"9780190494889"
      },
      {
        "b":"hardcover ;"
      },
      {
        "t":"alkaline paper"
      }
    ],
    "ind1":" ",
    "ind2":" "
  }
}
```
```json
Rule:
"100": [
    {
      "entity": [
        {
          "target": "personalName",
          "subfield": ["a","b"],
          "exclusiveSubfield": ["t"],
          "rules": []
        },
        {
          "target": "personalNameTitle",
          "subfield": ["t"],
          "rules": []
      } 
      ]
    }
  ]
```

- If there is "t" among record sub-fields then target "personalName" field does not appear at all.
- If there is no "t" in record sub-fields then target field gets filled by all the `["a","b"].`


#### Field replacement rules
There are "fieldReplacement" properties. They need for changing field number for specific fields, based on field replacement rules.
For example, if 880 field need to be changed on 711 based on some condition, "fieldReplacement" properties can help with it.
Example:
```json
Rule:
"880": [
{
"target": "",
"description": "Field for target post-processing based on first 3 digits in subfield 6",
"subfield": [
"6"
],
"fieldReplacementBy3Digits" : true,
"fieldReplacementRule" : [
{
"sourceDigits": "100",
"targetField": "700"
},
{
"sourceDigits": "110",
"targetField": "710"
},
{
"sourceDigits": "111",
"targetField": "711"
},
{
"sourceDigits": "245",
"targetField": "246"
}
]
}
]
```
`fieldReplacementBy3Digits` property indicates that "fieldReplacement"-logic should be applied here. (There can be added and implemented other properties, not only `...3Digits`)
`fieldReplacementRule` property contains some additional data for "fieldReplacement"-logic.

####  Processing record fields using indicators
If there is a need to use matches for indicators of record fields and rules, its needed to add an object `indicators` to a rule
which process a record field:

```json
Indicators object to process:
"indicators": {
"ind1": "1",
"ind2": "*"
}
```

```json
MARC Record:
  "024": {
    "subfields":[
         {"a":"Chicago, Illinois"},
         {"c":"Nashville, Tennessee"},
         {"f": "Austin Texas"}
    ], 
    "ind1":"2", 
    "ind2":"1"
    }
```
```json
  Rule:
  "024": [{
    "indicators": {
    "ind1": "2",
    "ind2": "*"
    },
    {
    "target": "identifiers.value",
    "description": "Invalid UPC",
    "subfield": ["a"],
    "rules": [{
      "conditions": [{
        "type": "concat_subfields_by_name",
          "parameter": {
            "subfieldsToConcat": [
              "f"
            ]
          }
        }]
      }]
  }}]
```
An outcome Instance looks like this in Json:
```json
Instance: 
{
  "identifiers":[
    {
      "value":"Chicago, Illinois Austin Texas"
    }
  ]
}
```
In our case, indicator value of record field "ind1" matched with indicator "ind1" value of the rules.
Indicator value of record field "ind2" is "1" but it also matched because the rule indicator "ind2" value is 
"*" - wildcard indicator, so any record field indicator value matches to it. In case of such match, 
the rule will be proceeded.

This mechanism executes in Processor, in data-import-processing-core, while parsing these rules.

####  Alternative mapping with specific subfield and target
If there is a need to use an alternative target and subfield if the result of the running function is empty, there can be used "alternativeMapping" rule:
which process a record field:

```json
Alternative rules:
"alternativeMapping": {
"target": "contributors.contributorTypeText",
"subfield": [
"e"
],
"ignoreSubsequentSubfields": true
},
"applyRulesOnConcatenatedData": true
},
```
##### Note: 
For the function "set_contributor_type_id_by_code_or_name" there should be set additional parameters: "contributorCodeSubfield" and "contributorNameSubfield" for searching from subfield "4" as code for Contributors and from subfield "e" as name for Contributors.
Also, there was added a brand new function called "trim_punctuation". It will provide the same functionality as the "trim"+"trim_period" functions. 
Moreover, it provides **additional mechanism for the punctuations**:
If ending punctuation of the last mapped subfield of the field is a period or comma, then it will be removed, **EXCEPT**
1.If the last mapped text ends with a single letter and then a period (e.g. Brown, Sterling K.), then ending period will not removed.
2.If the last mapped text ends with a period, followed by a comma, (e.g. Brown, Sterling K,.), then the comma will be removed, but the period will leave.
3.If the last mapped text ends with a hyphen (e.g. Kaluuya, Daniel, 1989-), then the hyphen will not removed.

```json
  Rule:
"710": [
{
"entity": [
{
"target": "contributors.authorityId",
"description": "Authority ID that controlling the contributor",
"applyRulesOnConcatenatedData": true,
"subfield": [
"9"
]
},
{
"target": "contributors.contributorNameTypeId",
"description": "Type for Corporate Name",
"applyRulesOnConcatenatedData": true,
"subfield": [
],
"rules": [
{
"conditions": [
{
"type": "set_contributor_name_type_id",
"parameter": {
"name": "Corporate name"
}
}
]
}
]
},
{
"rules": [
{
"conditions": [
{
"type": "set_contributor_type_id_by_code_or_name",
"parameter": {
"contributorCodeSubfield": "4",
"contributorNameSubfield": "e"
}
}
]
}
],
"target": "contributors.contributorTypeId",
"subfield": [
"4",
"e"
],
"description": "Type of contributor",
"alternativeMapping": {
"target": "contributors.contributorTypeText",
"subfield": [
"e"
],
"ignoreSubsequentSubfields": true
},
"applyRulesOnConcatenatedData": true
},
{
"target": "contributors.primary",
"description": "Primary contributor",
"applyRulesOnConcatenatedData": true,
"subfield": [
],
"rules": [
{
"conditions": [],
"value": "false"
}
]
},
{
"target": "contributors.name",
"description": "Corporate Name",
"subfield": [
"a",
"b",
"c",
"d",
"f",
"g",
"k",
"l",
"n",
"p",
"t",
"u"
],
"applyRulesOnConcatenatedData": true,
"rules": [
{
"conditions": [
{
"type": "trim_punctuation",
}
]
}
]
}
]
}
],
```
##### **NOTE**: Regarding ending punctuation - if the mapped text ends with (".", ",", ";") then it will be(the last symbol) removed for the matching with Contributor Type.

####  Map single JsonObject
If there is a need to map not arrays or string but json object with simple fields inside (strings), there can be used "createSingleObject" rule, which will create a single json object with specified fields:

```json
Building Dates json object:
{
"target": "dates.dateTypeId",
"description": "Date type ID",
"subfield": [],
"createSingleObject": true,
"rules": [
{
"conditions": [
{
"type": "set_date_type_id"
}
]
}
]
},
{
"target": "dates.date1",
"description": "Date 1",
"subfield": [],
"createSingleObject": true,
"rules": [
{
"conditions": [
{
"type": "char_select",
"parameter": {
"from": 7,
"to": 11
}
}
]
}
]
},
{
"target": "dates.date2",
"description": "Date 2",
"subfield": [],
"createSingleObject": true,
"rules": [
{
"conditions": [
{
"type": "char_select",
"parameter": {
"from": 11,
"to": 15
}
}
]
}
]
}
```
#
### REST API
When the source-record-manager starts up, it performs initialization for default mapping rules for given tenant.
There are 3 REST methods to work with rules.

| Method | URL | Content type | Description |
| :------------ | :------------ | :------------ | :------------ |
|**GET**| /mapping-rules/{recordType} |application/json | Get rules for given tenant by record type |
|**PUT**| /mapping-rules/{recordType} | application/json | Update rules for given tenant |
|**PUT**| /mapping-rules/{recordType}/restore | application/json | Restore rules to default |

An endpoint support recordType path parameter with 3 possible values:
* marc-bib
* marc-holdings
* marc-authority (only GET)

Before working with API make sure you have an HTTP token that is required for sending requests. If you have already logged in the system using UI, just copy token from `Apps/Settings/Developer/Set token/Authentication token` field.
Also you can log in the system using CLI tools, response returns `x-okapi-token` header with HTTP token.
```
Login request:
curl -w '\n' -X POST \
  --header "Content-Type: application/json" \
  --header "x-okapi-tenant: {tenant}" \
  --data @credentials.json \
  https://folio-snapshot-load-okapi.dev.folio.org/bl-users/login

  credentials.json: 
  {
    "username": "{username}",
    "password": "{password}"
  }
```
#
To get rules you can send a request using GET method
```
curl -w '\n' -X GET \
  --header "Content-type: application/json" \ 
  --header "x-okapi-tenant: {tenant}" \
  --header "x-okapi-token: {token}" \
  https://folio-snapshot-load-okapi.dev.folio.org/mapping-rules
```
A response returns existing rules:
```
{
    "001": [
        {
            "rules": [],
            "target": "hrid",
            "subfield": [],
            "description": "The human readable ID"
        }
    ],
    "008": [
        {
            "rules":
...
}
```

If you would like to update rules just get existing rules using GET method and combine it with your updates, use PUT method:
```
curl -w '\n' -X PUT \
  --header "Content-type: application/json" \
  --header "Accept: text/plain, application/json" \
  --header "x-okapi-tenant: {tenant}" \
  --header "x-okapi-token: {token}" \
  --data @rules.json \
  https://folio-snapshot-load-okapi.dev.folio.org/mapping-rules
```
rules.json with updated list of subfields for 001 :
```
{
    "001": [
        {
            "rules": [],
            "target": "hrid",
            "subfield": ["a", "b", "c"],
            "description": "The human readable ID"
        }
    ],
    "008": [
        {
            "rules":
...
}
```
A response returns updated rules, content should be the same you sent in body of the PUT request:
```
{
    "001": [
        {
            "rules": [],
            "target": "hrid",
            "subfield": ["a", "b", "c"],
            "description": "The human readable ID"
        }
    ],
    "008": [
        {
            "rules":
...
}
```
Before sending an updates, please, make sure your file has valid JSON format, otherwise the system returns response with 400 error code (Bad Request).
To validate JSON file there are online free tools: [Json Formatter](https://jsonformatter.curiousconcept.com).

To revert the current state of rules to default, as it was at the system startup, use PUT method with 'restore' suffix:
```
curl -w '\n' -X PUT \
  --header "Content-type: application/json" \
  --header "Accept: text/plain, application/json" \
  --header "x-okapi-tenant: {tenant}" \
  --header "x-okapi-token: {token}" \
  https://folio-snapshot-load-okapi.dev.folio.org/mapping-rules/restore
```
A response returns rules in default state:
```
{
    "001": [
        {
            "rules": [],
            "target": "hrid",
            "subfield": [],
            "description": "The human readable ID"
        }
    ],
    "008": [
        {
            "rules":
...
}
```
