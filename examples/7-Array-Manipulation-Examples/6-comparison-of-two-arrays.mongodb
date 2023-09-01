db = db.getSiblingDB("book-comparison-of-two-arrays");
db.dropDatabase();

// Insert 5 records into the deployments collection
db.deployments.insertMany([
  {
    "name": "ProdServer",
    "beforeTimestamp": ISODate("2022-01-01T00:00:00Z"),
    "afterTimestamp": ISODate("2022-01-02T00:00:00Z"),
    "beforeConfig": {
      "vcpus": 8,
      "ram": 128,
      "storage": 512,
      "state": "running",
    },
    "afterConfig": {
      "vcpus": 16,
      "ram": 256,
      "storage": 512,
      "state": "running",
    },    
  },
  {
    "name": "QAServer",
    "beforeTimestamp": ISODate("2022-01-01T00:00:00Z"),
    "afterTimestamp": ISODate("2022-01-02T00:00:00Z"),
    "beforeConfig": {
      "vcpus": 4,
      "ram": 64,
      "storage": 512,
      "state": "paused",
    },
    "afterConfig": {
      "vcpus": 4,
      "ram": 64,
      "storage": 256,
      "state": "running",
      "extraParams": "disableTLS;disableCerts;"
    },    
  },
  {
    "name": "LoadTestServer",
    "beforeTimestamp": ISODate("2022-01-01T00:00:00Z"),
    "beforeConfig": {
      "vcpus": 8,
      "ram": 128,
      "storage": 256,
      "state": "running",
    },
  },
  {
    "name": "IntegrationServer",
    "beforeTimestamp": ISODate("2022-01-01T00:00:00Z"),
    "afterTimestamp": ISODate("2022-01-02T00:00:00Z"),
    "beforeConfig": {
      "vcpus": 4,
      "ram": 32,
      "storage": 64,
      "state": "running",
    },
    "afterConfig": {
      "vcpus": 4,
      "ram": 32,
      "storage": 64,
      "state": "running",
    },
  },
  {
    "name": "DevServer",
    "afterTimestamp": ISODate("2022-01-02T00:00:00Z"),
    "afterConfig": {
      "vcpus": 2,
      "ram": 16,
      "storage": 64,
      "state": "running",
    },
  },
]);

//
// Define the macro functions
//

// Macro function to generate a complex expression to get all the unique
// keys from two sub-documents returned as an array of the unique keys
function getArrayOfTwoSubdocsKeysNoDups(firstArrayRef, secondArrayRef) {
  return {
    "$setUnion": {
      "$concatArrays": [
        {"$map": {
          "input": {"$objectToArray": firstArrayRef},
          "in": "$$this.k",
        }},
        {"$map": {
          "input": {"$objectToArray": secondArrayRef},
          "in": "$$this.k",
        }},
      ]
    }
  };
}

// Macro function to generate a complex expression to get the value of a
// field in a document where the field's name is only known at runtime 
function getDynamicField(obj, fieldname) {
  return {
    "$first": [ 
      {"$map": { 
        "input": {
          "$filter": { 
            "input": {"$objectToArray": obj}, 
            "as": "currObj",
            "cond": {"$eq": ["$$currObj.k", fieldname]},
            "limit": 1
          }
        }, 
        "in": "$$this.v" 
      }}, 
    ]
  };
}

// Define the pipeline
var pipeline = [
  // Compare two different arrays in the same document & get the differences
  {"$set": {
    "differences": {
      "$reduce": {
        "input": getArrayOfTwoSubdocsKeysNoDups("$beforeConfig", 
                                                "$afterConfig"),
        "initialValue": [],
        "in": {
          "$concatArrays": [
            "$$value",
            {"$cond": {
              "if": {
                "$ne": [
                  getDynamicField("$beforeConfig", "$$this"),
                  getDynamicField("$afterConfig", "$$this"),
                ]
              },
              "then": [{
                "field": "$$this",
                "change": {
                  "$concat": [
                    {"$ifNull": [
                      {"$toString": 
                         getDynamicField("$beforeConfig", "$$this")},
                      "<not-set>"
                    ]},
                    " --> ",
                    {"$ifNull": [
                      {"$toString":
                         getDynamicField("$afterConfig", "$$this")}, 
                      "<not-set>"
                    ]},
                  ]
                },
              }],
              "else": [],
            }}
          ]
        }
      }
    },
  }},

  // Add 'status' field & only show 'differences' field if differences exist
  {"$set": {
    // Set 'status' to ADDED, REMOVED, MODIFIED or UNCHANGED accordingly
    "status": {
      "$switch": {        
        "branches": [
          {
            "case": {
              "$and": [
                {"$in": [{"$type": "$differences"}, ["missing", "null"]]},
                {"$in": [{"$type": "$beforeConfig"}, ["missing", "null"]]},
              ]
            },
            "then": "ADDED"
          },
          {
            "case": {
              "$and": [
                {"$in": [{"$type": "$differences"}, ["missing", "null"]]},
                {"$in": [{"$type": "$afterConfig"}, ["missing", "null"]]},
              ]
            },
            "then": "REMOVED"
          },
          {"case": 
            {"$lte": [{"$size": "$differences"}, 0]},
            "then": "UNCHANGED"
          },
          {"case": 
            {"$gt":  [{"$size": "$differences"}, 0]}, 
            "then": "MODIFIED"
          },
        ],
        "default": "UNKNOWN",
      }
    },

    // If there are diffs, keep the differences field, otherwise remove it
    "differences": {
      "$cond": [
        {"$or": [
          {"$in": [{"$type": "$differences"}, ["missing", "null"]]},
          {"$lte": [{"$size": "$differences"}, 0]},
        ]},
        "$$REMOVE", 
        "$differences"
      ]
    },
  }},         

  // Remove unwanted fields
  {"$unset": [
    "_id",
    "beforeTimestamp",
    "afterTimestamp",
    "beforeConfig",
    "afterConfig",
  ]},
];   

// Execute the aggregation and print the result
var result = db.deployments.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
