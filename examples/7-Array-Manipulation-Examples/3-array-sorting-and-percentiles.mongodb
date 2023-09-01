db = db.getSiblingDB("book-array-sort-percentiles");
db.dropDatabase();

// Insert 7 records into the performance_test_results collection
db.performance_test_results.insertMany([
  {
    "testRun": 1,
    "datetime": ISODate("2021-08-01T22:51:27.638Z"),
    "responseTimesMillis": [
      62, 97, 59, 104, 97, 71, 62, 115, 82, 87,
    ],
  },
  {
    "testRun": 2,
    "datetime": ISODate("2021-08-01T22:56:32.272Z"),
    "responseTimesMillis": [
      34, 63, 51, 104, 87, 63, 64, 86, 105, 51, 73,
      78, 59, 108, 65, 58, 69, 106, 87, 93, 65,
    ],
  },
  {
    "testRun": 3,
    "datetime": ISODate("2021-08-01T23:01:08.908Z"),
    "responseTimesMillis": [
      56, 72, 83, 95, 107, 83, 85,
    ],
  },
  {
    "testRun": 4,
    "datetime": ISODate("2021-08-01T23:17:33.526Z"),
    "responseTimesMillis": [
      78, 67, 107, 110,
    ],
  },
  {
    "testRun": 5,
    "datetime": ISODate("2021-08-01T23:24:39.998Z"),
    "responseTimesMillis": [
      75, 91, 75, 87, 99, 88, 55, 72, 99, 102,
    ],
  },
  {
    "testRun": 6,
    "datetime": ISODate("2021-08-01T23:27:52.272Z"),
    "responseTimesMillis": [
      88, 89,
    ],
  },
  {
    "testRun": 7,
    "datetime": ISODate("2021-08-01T23:31:59.917Z"),
    "responseTimesMillis": [
      101,
    ],
  },
]);


//
// Define the two macro functions
//

// Macro function to generate a complex aggregation expression for sorting
// an array - this function isn't required for MongoDB version 5.2+ due to
// the new $sortArray operator
function sortArray(sourceArrayField) {
  return {
    // GENERATE NEW ARRAY TO CONTAIN ELEMENTS FROM SOURCE ARRAY BUT SORTED
    "$reduce": {
      "input": sourceArrayField, 
      "initialValue": [],  // 1st VERSION OF TEMP SORTED ARRAY WILL BE EMPTY
      "in": {
        "$let": {
          "vars": {  // CAPTURE $$this & $$value FROM OUTER $reduce
            "resultArray": "$$value",
            "currentSourceArrayElement": "$$this"
          },   
          "in": {
            "$let": {
              "vars": { 
                // FIND EACH SOURCE ARRAY'S CURRENT ELEM POS IN SORTED ARRAY
                "targetArrayPosition": {
                  "$reduce": { 
                    "input": {"$range": [0, {"$size": "$$resultArray"}]},
                    "initialValue": {  // INIT SORTED POS TO BE LAST ELEMNT
                      "$size": "$$resultArray"
                    },
                    "in": {  // LOOP THRU "0,1,2..."
                      "$cond": [ 
                        {"$lt": [
                          "$$currentSourceArrayElement",
                          {"$arrayElemAt": ["$$resultArray", "$$this"]}
                        ]}, 
                        {"$min": ["$$value", "$$this"]}, // IF NOT YET FOUND
                        "$$value"  // RETAIN INIT VAL AS NOT YET FOUND POSTN
                      ]
                    }
                  }
                }
              },
              "in": {
                // BUILD NEW ARRAY BY SLICING OLDER 1 INSERT NEW ELM BETWEEN
                "$concatArrays": [
                  {"$cond": [  // RETAIN THE EXISTING 1st PART OF NEW ARRAY
                    {"$eq": [0, "$$targetArrayPosition"]}, 
                    [],
                    {"$slice": ["$$resultArray", 0, "$$targetArrayPosition"]},
                  ]},
                  ["$$currentSourceArrayElement"],  // PULL IN NEW ELEMENT
                  {"$cond": [  // RETAIN EXISTING LAST PART OF NEW ARRAY
                    {"$gt": [{"$size": "$$resultArray"}, 0]}, 
                    {"$slice": [
                      "$$resultArray",
                      "$$targetArrayPosition",
                      {"$size": "$$resultArray"}
                    ]},
                    [],
                  ]},
                ]
              } 
            }
          }
        }
      }
    }      
  };
}

// Macro function to find nth percentile element of sorted version of array
function arrayElemAtPercentile(sourceArrayField, percentile) {
  return {    
    "$let": {
      "vars": {
        "sortedArray": sortArray(sourceArrayField), 
        // Comment out the line above & uncomment lines below if MDB 5.2+
        //"sortedArray": {
        //  "$sortArray": {"input": sourceArrayField, "sortBy": 1}
        //},        
      },
      "in": {         
        "$arrayElemAt": [  // FIND ELEMENT OF ARRAY AT NTH PERCENTILE POS
          "$$sortedArray",
          {"$subtract": [  // ARRAY IS 0-INDEX BASED: SUBTRACT 1 TO GET POS
            {"$ceil":  // FIND NTH ELEME IN ARRAY ROUNDED UP TO NEAREST INT
              {"$multiply": [
                {"$divide": [percentile, 100]},
                {"$size": "$$sortedArray"}
              ]}
            },
            1
          ]}
        ]
      }
    }
  };
}

// Define the pipeline
var pipeline = [
  // Capture new fields for the ordered array + various percentiles
  {"$set": {
    "sortedResponseTimesMillis": sortArray("$responseTimesMillis"),
    // Comment out the line above & uncomment lines below if MDB 5.2+
    //"sortedResponseTimesMillis": {
    //  "$sortArray": {"input": "$responseTimesMillis", "sortBy": 1}
    //},
    "medianTimeMillis":
      arrayElemAtPercentile("$responseTimesMillis", 50),
    "ninetiethPercentileTimeMillis": 
      arrayElemAtPercentile("$responseTimesMillis", 90),
  }},

  // Only show results for tests with slow latencies
  // (i.e. 90th%-ile responses >100ms)
  {"$match": {
    "ninetiethPercentileTimeMillis": {"$gt": 100},
  }},

  // Exclude unrequired fields from each record
  {"$unset": [
    "_id",
    "datetime",
    "responseTimesMillis",
  ]},    
];

// Execute the aggregation and print the result
var result = db.performance_test_results.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
