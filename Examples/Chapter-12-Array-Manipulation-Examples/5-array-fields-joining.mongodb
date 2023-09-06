db = db.getSiblingDB("book-array-fields-joining");
db.dropDatabase();

// Insert 2 records into the users collection
db.users.insertMany([
  {
    "firstName": "Alice",
    "lastName": "Jones",
    "dateOfBirth": ISODate("1985-07-21T00:00:00Z"),
    "hobbies": {
      "music": "Playing the guitar",
      "reading": "Science Fiction books",
      "gaming": "Video games, especially RPGs",
      "sports": "Long-distance running",
      "traveling": "Visiting exotic places",
      "cooking": "Trying out new recipes",
    },      
    "moodFavourites": {
      "sad": ["music"],
      "happy": ["sports"],
      "chilling": ["music", "cooking"],
    },
  },
  {
    "firstName": "Sam",
    "lastName": "Brown",
    "dateOfBirth": ISODate("1993-12-01T00:00:00Z"),
    "hobbies": {
      "cycling": "Mountain biking",
      "writing": "Poetry and short stories",
      "knitting": "Knitting scarves and hats",
      "hiking": "Hiking in the mountains",
      "volunteering": "Helping at the local animal shelter",
      "music": "Listening to Jazz",
      "photography": "Nature photography",
      "gardening": "Growing herbs and vegetables",
      "yoga": "Practicing Hatha Yoga",
      "cinema": "Watching classic movies",
    },
    "moodFavourites": {
      "happy": ["gardening", "cycling"],
      "sad": ["knitting"],
    },
  },
]);

//
// Define the macro functions
//

// Macro function to generate a complex expression to get array values of
// named fields in sub-doc where each field's name is only known at runtime
function getValuesOfNamedFieldsAsArray(obj, fieldnames) {
  return {
    "$map": { 
      "input": {
        "$filter": { 
          "input": {"$objectToArray": obj}, 
          "as": "currElem",
          "cond": {"$in": ["$$currElem.k", fieldnames]},
        }
      }, 
      "in": "$$this.v" 
    }, 
  };
}

// Define the pipeline
var pipeline = [
  // Set field with activities each user likes doing according to their mood
  {"$set": {
    "moodActivities": {      
      "$arrayToObject": {
        "$map": { 
          "input": {"$objectToArray": "$moodFavourites"},
          "in": {              
            "k": "$$this.k",
            "v": getValuesOfNamedFieldsAsArray("$hobbies", "$$this.v"),
          }
        }, 
      }
    }
  }},

  // Remove unwanted fields  
  {"$unset": [
    "_id",
    "hobbies",
    "moodFavourites",
  ]},  
];

// Execute the aggregation and print the result
var result = db.users.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
