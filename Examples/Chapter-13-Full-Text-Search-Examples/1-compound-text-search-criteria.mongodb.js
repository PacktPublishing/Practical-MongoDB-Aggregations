db = db.getSiblingDB("book-compound-text-search");
db.products.remove({});

// Insert 7 records into the products collection
db.products.insertMany([
  {
    "name": "The Road",
    "category": "DVD",
    "description": "In a dangerous post-apocalyptic world, a dying father protects his surviving son as they try to reach the coast",
  },
  {
    "name": "The Day Of The Triffids",
    "category": "BOOK",
    "description": "Post-apocalyptic disaster where most people are blinded by a meteor shower and then die at the hands of a new type of plant",
  },
  {
    "name": "The Road",
    "category": "BOOK",
    "description": "In a dangerous post-apocalyptic world, a dying father protects his surviving son as they try to reach the coast",
  },  
  {
    "name": "The Day the Earth Caught Fire",
    "category": "DVD",
    "description": "A series of nuclear explosions cause fires and earthquakes to ravage cities, with some of those that survive trying to rescue the post-apocalyptic world",
  },
  {
    "name": "28 Days Later",
    "category": "DVD",
    "description": "A caged chimp infected with a virus is freed from a lab, and the infection spreads to people who become zombie-like with just a few surviving in a post-apocalyptic country",
  },  
  {
    "name": "Don't Look Up",
    "category": "DVD",
    "description": "Pre-apocalyptic situation where some astronomers warn humankind of an approaching comet that will destroy planet Earth",
  },
  {
    "name": "Thirteen Days",
    "category": "DVD",
    "description": "Based on the true story of the Cuban nuclear misile threat, crisis is averted at the last minute and the workd survives",
  },
]); 

// Define the pipeline
var pipeline = [
  // Search DVDs where desc must contain "apocalyptic" but not "zombie"
  {"$search": {
    "index": "default",    
    "compound": {
      "must": [
        {"text": {
          "path": "description",
          "query": "apocalyptic",
        }},
      ],
      "should": [
        {"text": {
          "path": "description",
          "query": "nuclear survives",
        }},
      ],
      "mustNot": [
        {"text": {
          "path": "description",
          "query": "zombie",
        }},
      ],
      "filter": [
        {"text": {
          "path": "category",
          "query": "DVD",
        }},      
      ],
    }
  }},

  // Capture search relevancy score in the output and omit the _id field
  {"$set": {
    "score": {"$meta": "searchScore"},
    "_id": "$$REMOVE",
  }},
];

// Execute the aggregation and print the result
var result = db.products.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
