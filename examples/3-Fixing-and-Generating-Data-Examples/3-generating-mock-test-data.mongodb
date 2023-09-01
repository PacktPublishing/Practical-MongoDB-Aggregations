db = db.getSiblingDB("book-generate-mock-data");
db.dropDatabase();

// Insert 6 records into the 'source' collection
db.source.insertMany([
  {
    "key": "A",
    "datetime": ISODate("2009-04-27T00:00:00.000Z"), 
    "progress": 1
  },
  {
    "key": "B",
    "datetime": ISODate("2009-04-27T00:00:00.000Z"),
    "progress": 1
  },
  {
    "key": "C",
    "datetime": ISODate("2009-04-27T00:00:00.000Z"), 
    "progress": 1
  },
  {
    "key": "D", 
    "datetime": ISODate("2009-04-27T00:00:00.000Z"), 
    "progress": 1
  },
  {
    "key": "A", 
    "datetime": ISODate("2023-07-31T06:59:59.000Z"), 
    "progress": 9
  },
  {
    "key": "B", 
    "datetime": ISODate("2023-07-31T06:59:59.000Z"), 
    "progress": 9
  },
  {
    "key": "C", 
    "datetime": ISODate("2023-07-31T06:59:59.000Z"), 
    "progress": 9
  },
  {
    "key": "D", 
    "datetime": ISODate("2023-07-31T06:59:59.000Z"), 
    "progress": 9
  },
]);

// Define the pipeline
var pipeline = [
  // Add new records every hour between the current first datatime
  // to current last datetime for each existing key value
  {"$densify": {
    "field": "datetime", 
    "partitionByFields": ["key"], 
    "range": {"bounds": "full", "step": 1, "unit": "hour"},
  }},

  // For the exsiting records, where 'progress' field is not set add a field
  // with a value that porgressively increases for each existing key
  {"$fill": {
    "sortBy": {"datetime": 1},      
    "partitionBy": {"key": "$key"},
    "output": {
      "progress": {"method": "linear"}
    },
  }},

  {"$set": {
    // Set score field to be a random number
    "score": {"$rand": {}},    

    // Set prefernece field to one of 3 values randomly
    "preference": {
      "$let": {
        "vars": {
          "values": ["RED", "YELLOW", "BLUE"],
        },   
        "in": {   
          "$arrayElemAt": [
            "$$values", 
            {"$floor": {"$multiply": [{"$size": "$$values"}, {"$rand": {}}]}},
          ]
        }
      }        
    },
  }},

  {"$merge": {
    "into": "destination",
  }},    
];

// Execute the aggregation and print the result
db.source.aggregate(pipeline);
var result = db.destination.find();
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
