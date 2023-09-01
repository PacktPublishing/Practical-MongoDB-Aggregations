var dbName = "book-role-programmatic-restricted-view";
db = db.getSiblingDB(dbName);
db.dropDatabase();
db.dropAllRoles();
db.dropAllUsers();

// Create 3 roles to use for programmatic access control
db.createRole({"role": "Receptionist", "roles": [], "privileges": []});
db.createRole({"role": "Nurse", "roles": [], "privileges": []});
db.createRole({"role": "Doctor", "roles": [], "privileges": []});

// Create 3 users where each user will have a different role
db.createUser({
  "user": "front-desk",
  "pwd": "abc123",
  "roles": [
    {"role": "Receptionist", "db": dbName},
  ]
});
db.createUser({
  "user": "nurse-station",
  "pwd": "xyz789",
  "roles": [
    {"role": "Nurse", "db": dbName},
  ]
});
db.createUser({
  "user": "exam-room",
  "pwd": "mno456",
  "roles": [
    {"role": "Doctor", "db": dbName},
  ]
});

// Insert 4 records into the patients collection
db.patients.insertMany([
  {
    "id": "D40230",
    "first_name": "Chelsea",
    "last_Name": "Chow",
    "birth_date": ISODate("1984-11-07T10:12:00Z"),
    "weight": 145,
    "medication": ["Insulin", "Methotrexate"],
  },
  {
    "id": "R83165",
    "first_name": "Pharrell",
    "last_Name": "Phillips",
    "birth_date": ISODate("1993-05-30T19:44:00Z"),
    "weight": 137,
    "medication": ["Fluoxetine"],
  },  
  {
    "id": "X24046",
    "first_name": "Billy",
    "last_Name": "Boaty",
    "birth_date": ISODate("1976-02-07T23:58:00Z"),
    "weight": 223,
    "medication": [],
  },
  {
    "id": "P53212",
    "first_name": "Yazz",
    "last_Name": "Yodeler",
    "birth_date": ISODate("1999-12-25T12:51:00Z"),
    "weight": 156,
    "medication": ["Tylenol", "Naproxen"],
  }, 
]);

// Define the pipeline
var pipeline = [
  {"$set": {
    // Exclude weight if user does not have right role
    "weight": {
      "$cond": {
        "if": {
          "$eq": [
            {"$setIntersection": [
              "$$USER_ROLES.role",
              ["Doctor", "Nurse"]
            ]},
            []
          ]
        },
        "then": "$$REMOVE",
        "else": "$weight"
      }
    },
      
    // Exclude weight if user does not have right role
    "medication": {
      "$cond": {
        "if": {
          "$eq": [
            {"$setIntersection": 
              ["$$USER_ROLES.role",
              ["Doctor"]
            ]},
            []
          ]
        },
        "then": "$$REMOVE",
        "else": "$medication"
      }
    },

    // Always exclude _id
    "_id": "$$REMOVE",
  }},
]

// Define the view
db.createView("patients_view", "patients", pipeline);

// Execute the view as different users one at a time and print the result for each
db.auth("front-desk", "abc123");
var result = db.patients_view.find();
printjson(result);  // MongoDB Shell script output
db.auth("nurse-station", "xyz789");
var result = db.patients_view.find();
printjson(result);  // MongoDB Shell script output
db.auth("exam-room", "mno456");
var result = db.patients_view.find();
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
