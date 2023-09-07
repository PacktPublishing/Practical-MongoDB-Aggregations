db = db.getSiblingDB("book-jagged-array-condense");
db.dropDatabase();

// Insert 2 records into the drug trials results collection
db.drugTrialResults.insertMany([
  {
    "trial_id": 111111111,
    "trial_name": "Clinical Trial for Drug A",
    "drug_name": "Drug A",
    "patientsWithAdministeredSessions": [
      [30.3, 40.4, 50.3], // Patient 1 sessions (effectiveness scores)
      [55.5, 65.2, 75.4, 85.3], // Patient 2 sessions (effectiveness scores)
      [50.3, 66.4, 65.5, 87.6, 86.7], // Patient 3 sessions (scores)
      [40.1, 50.2, 60.1, 70.3], // Patient 4 sessions (effectiveness scores)
      [45.6, 55.7, 55.6, 75.5, 85.3, 91.8], // Patient 5 sessions (scores)
      [41.5, 40.2, 55.5, 65.3], // Patient 6 sessions (effectiveness scores)
      [38.4, 39.2, 47.9, 55.2], // Patient 7 sessions (effectiveness scores)
    ]
  },
  {
    "trial_id": 222222222,
    "trial_name": "Clinical Trial for Drug B",
    "drug_name": "Drug B",
    "patientsWithAdministeredSessions": [
      [87.7, 80.6, 70.5, 60.4], // Patient 1 sessions (effectiveness scores)
      [81.3, 83.4, 75.2], // Patient 2 sessions (effectiveness scores)
      [70.3, 60.1], // Patient 3 sessions (effectiveness scores)
      [75.5, 65.8, 55.7], // Patient 4 sessions (effectiveness scores)
      [60.4, 60.6, 40.4, 30.3], // Patient 5 sessions (effectiveness scores)
    ]
  },
]);

//
// Define the macro functions
//

// Macro function to generate a compound expression to get 
// the size of the largest sub-array witin in an array
function getLargestSubArraySize(arrayOfSubArraysRef) {
  return {
    "$reduce": { 
      "input": arrayOfSubArraysRef, 
      "initialValue": 0,
      "in": {
        "$cond": [
          {"$gt": [{"$size": "$$this"}, "$$value"]},
          {"$size": "$$this"},
          "$$value"
        ]
      },
    }, 
  };
}

// Macro function to generate a compound expression to condense
// the sub-arrays in an array into a single array of sums and counts
function condenseSubArraysToSumsAndCounts(arrayOfSubArraysRef) {
  return {
    "$map": {  // Loop with counter "index" up to largest sub-array's length
      "input": {"$range": [0, getLargestSubArraySize(arrayOfSubArraysRef)]},
      "as": "index",
      "in": {              
        "$reduce": {  // Loop with ref "this" thru each outer array member
          "input": {
            "$range": [0, {"$size": arrayOfSubArraysRef}]
          },
          "initialValue": {
            "sum": 0,
            "count": "",
          },
          "in": {
            // Total up the Nth sessions across all the patients
            "sum": {
              "$sum": [
                "$$value.sum",
                {"$arrayElemAt": [
                  {"$arrayElemAt": [arrayOfSubArraysRef, "$$this"]},
                  "$$index"
                ]}
              ]
            },
            // Count number of patients who conducted the Nth session
            "count": {
              "$sum": [
                "$$value.count", 
                {"$cond": [
                  {"$isNumber": {
                    "$arrayElemAt": [
                      {"$arrayElemAt": [arrayOfSubArraysRef, "$$this"]},
                      "$$index"
                    ]
                  }},
                  1, 
                  0,
                ]},                
              ]
            },
          },      
        },
      },
    },    
  };
}

// Define the pipeline
var pipeline = [
  {"$set": {    
    "sessionsAverageEffectiveness": {
      "$let": {
        "vars": {
          // Condense the sub-arrays into a single array of sums and counts
          "sessionsWithSumCountOfEffectiveness": 
            condenseSubArraysToSumsAndCounts(
              "$patientsWithAdministeredSessions"),
        },
        "in": {
          // Create avg score for Nth session for all participating patients
          "$map": {
            "input": "$$sessionsWithSumCountOfEffectiveness",
            "in": {"$divide": ["$$this.sum", "$$this.count"]}
          }
        }
      },
    },
  }},

  {"$unset": [
    "_id",
    "patientsWithAdministeredSessions",
  ]},
];


// Execute the aggregation and print the result
var result = db.drugTrialResults.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
