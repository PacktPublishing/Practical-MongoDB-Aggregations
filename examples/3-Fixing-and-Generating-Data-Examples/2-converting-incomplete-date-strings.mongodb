db = db.getSiblingDB("book-convert-incomplete-dates");
db.dropDatabase();

// Insert records into the payments collection
db.payments.insertMany([
  {
    "account": "010101",
    "paymentDate": "01-JAN-20 01.01.01.123000000",
    "amount": 1.01
  },
  {
    "account": "020202",
    "paymentDate": "02-FEB-20 02.02.02.456000000",
    "amount": 2.02
  },
  {
    "account": "030303",
    "paymentDate": "03-MAR-20 03.03.03.789000000",
    "amount": 3.03
  },
  {
    "account": "040404",
    "paymentDate": "04-APR-20 04.04.04.012000000",
    "amount": 4.04
  },
  {
    "account": "050505",
    "paymentDate": "05-MAY-20 05.05.05.345000000",
    "amount": 5.05
  },
  {
    "account": "060606",
    "paymentDate": "06-JUN-20 06.06.06.678000000",
    "amount": 6.06
  },
  {
    "account": "070707",
    "paymentDate": "07-JUL-20 07.07.07.901000000",
    "amount": 7.07
  },
  {
    "account": "080808",
    "paymentDate": "08-AUG-20 08.08.08.234000000",
    "amount": 8.08
  },
  {
    "account": "090909",
    "paymentDate": "09-SEP-20 09.09.09.567000000",
    "amount": 9.09
  },
  {
    "account": "101010",
    "paymentDate": "10-OCT-20 10.10.10.890000000",
    "amount": 10.10
  },
  {
    "account": "111111",
    "paymentDate": "11-NOV-20 11.11.11.111000000",
    "amount": 11.11
  },
  {
    "account": "121212",
    "paymentDate": "12-DEC-20 12.12.12.999000000",
    "amount": 12.12
  }
]);

// Define the pipeline
var pipeline = [
  // Change field from a string to a date, filling in the gaps
  {"$set": {
    "paymentDate": {    
      "$let": {
        "vars": {
          "txt": "$paymentDate",  // Assign "paymentDate" field to variable
          "month": {"$substrCP": ["$paymentDate", 3, 3]},  // Extract month
        },
        "in": { 
          "$dateFromString": {"format": "%d-%m-%Y %H.%M.%S.%L", "dateString":
            {"$concat": [
              {"$substrCP": ["$$txt", 0, 3]},  // Use 1st 3 chars in string
              {"$switch": {"branches": [  // Replace 3 chars with month num
                {"case": {"$eq": ["$$month", "JAN"]}, "then": "01"},
                {"case": {"$eq": ["$$month", "FEB"]}, "then": "02"},
                {"case": {"$eq": ["$$month", "MAR"]}, "then": "03"},
                {"case": {"$eq": ["$$month", "APR"]}, "then": "04"},
                {"case": {"$eq": ["$$month", "MAY"]}, "then": "05"},
                {"case": {"$eq": ["$$month", "JUN"]}, "then": "06"},
                {"case": {"$eq": ["$$month", "JUL"]}, "then": "07"},
                {"case": {"$eq": ["$$month", "AUG"]}, "then": "08"},
                {"case": {"$eq": ["$$month", "SEP"]}, "then": "09"},
                {"case": {"$eq": ["$$month", "OCT"]}, "then": "10"},
                {"case": {"$eq": ["$$month", "NOV"]}, "then": "11"},
                {"case": {"$eq": ["$$month", "DEC"]}, "then": "12"},
               ], "default": "ERROR"}},
              "-20",  // Add hyphen + hardcoded century 2 digits
              {"$substrCP": ["$$txt", 7, 15]}  // Ignore last 6 nanosecs
            ]
          }}                  
        }
      }        
    },             
  }},

  // Omit unwanted fields
  {"$unset": [
    "_id",
  ]},         
];

// Execute the aggregation and print the result
var result = db.payments.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
