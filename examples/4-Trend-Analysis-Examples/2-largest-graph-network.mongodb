db = db.getSiblingDB("book-largest-graph-network");
db.dropDatabase();

// Create index on field which each graph traversal hop will connect to
db.users.createIndex({"name": 1});

// Insert records into the users collection
db.users.insertMany([
  {"name": "Paul", "followed_by": []},
  {"name": "Toni", "followed_by": ["Paul"]},
  {"name": "Janet", "followed_by": ["Paul", "Toni"]},
  {"name": "David", "followed_by": ["Janet", "Paul", "Toni"]},
  {"name": "Fiona", "followed_by": ["David", "Paul"]},
  {"name": "Bob", "followed_by": ["Janet"]},
  {"name": "Carl", "followed_by": ["Fiona"]},
  {"name": "Sarah", "followed_by": ["Carl", "Paul"]},
  {"name": "Carol", "followed_by": ["Helen", "Sarah"]},
  {"name": "Helen", "followed_by": ["Paul"]},
]);

// Define the pipeline
var pipeline = [
  // For each social network user, traverse their 'followed_by' people list
  {"$graphLookup": {
    "from": "users",
    "startWith": "$followed_by",
    "connectFromField": "followed_by",
    "connectToField": "name",
    "depthField": "depth",
    "as": "extended_network",
  }},

  // Add new accumulating fields
  {"$set": {
    // Count the extended connection reach
    "network_reach": {
      "$size": "$extended_network"
    },

    // Gather the list of the extended connections' names
    "extended_connections": {
      "$map": {
        "input": "$extended_network",
        "as": "connection",
        "in": "$$connection.name", // Get name field from each element
      }
    },    
  }},
    
  // Omit unwanted fields
  {"$unset": [
    "_id",
    "followed_by",
    "extended_network",
  ]},   
  
  // Sort by person with greatest network reach first, in descending order
  {"$sort": {
    "network_reach": -1,
  }},   
];

// Execute the aggregation and print the result
var result = db.users.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output

