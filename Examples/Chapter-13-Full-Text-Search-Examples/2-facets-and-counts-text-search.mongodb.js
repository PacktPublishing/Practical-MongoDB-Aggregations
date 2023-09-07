db = db.getSiblingDB("book-facets-text-search");
db.enquiries.remove({});

// Insert records into the enquiries collection
db.enquiries.insertMany([
  {
    "acountId": "9913183",
    "datetime": ISODate("2022-01-30T08:35:52Z"),
    "summary": "They just made a balance enquiry only - no other issues",
  },
  {
    "acountId": "9913183",
    "datetime": ISODate("2022-01-30T09:32:07Z"),
    "summary": "Reported suspected fraud - froze cards, initiated chargeback on the transaction",
  },
  {
    "acountId": "6830859",
    "datetime": ISODate("2022-01-30T10:25:37Z"),
    "summary": "Customer said they didn't make one of the transactions which could be fraud - passed on to the investigations team",
  },
  {
    "acountId": "9899216",
    "datetime": ISODate("2022-01-30T11:13:32Z"),
    "summary": "Struggling financially this month hence requiring extended overdraft - increased limit to 500 for 2 monts",
  },  
  {
    "acountId": "1766583",
    "datetime": ISODate("2022-01-30T10:56:53Z"),
    "summary": "Fraud reported - fradulent direct debit established 3 months ago - removed instruction and reported to crime team",
  },
  {
    "acountId": "9310399",
    "datetime": ISODate("2022-01-30T14:04:48Z"),
    "summary": "Customer rang on mobile whilst fraud call in progress on home phone to check if it was valid - advised to hang up",
  },
  {
    "acountId": "4542001",
    "datetime": ISODate("2022-01-30T16:55:46Z"),
    "summary": "Enquiring for loan - approved standard loan for 6000 over 4 years",
  },
  {
    "acountId": "7387756",
    "datetime": ISODate("2022-01-30T17:49:32Z"),
    "summary": "Froze customer account when they called in as multiple fraud transactions appearing even whilst call was active",
  },
  {
    "acountId": "3987992",
    "datetime": ISODate("2022-01-30T22:49:44Z"),
    "summary": "Customer called claiming fraud for a transaction which confirmed looks suspicious and so issued chargeback",
  },
  {
    "acountId": "7362872",
    "datetime": ISODate("2022-01-31T07:07:14Z"),
    "summary": "Worst case of fraud I've ever seen - customer lost millions - escalated to our high value team",
  },
]);

// Define the pipeline
var pipeline = [
  // For 1 day 'fraud' enquiries group into periods of the day counting them
  {"$searchMeta": {
    "index": "default",    
    "facet": {
      "operator": {
        "compound": {
          "must": [
            {"text": {
              "path": "summary",
              "query": "fraud",
            }},
          ],
          "filter": [
            {"range": {
              "path": "datetime",
              "gte": ISODate("2022-01-30"),
              "lt": ISODate("2022-01-31"),
            }},
          ],
        },
      },
      "facets": {        
        "fraudEnquiryPeriods": {
          "type": "date",
          "path": "datetime",
          "boundaries": [
            ISODate("2022-01-30T00:00:00.000Z"),
            ISODate("2022-01-30T06:00:00.000Z"),
            ISODate("2022-01-30T12:00:00.000Z"),
            ISODate("2022-01-30T18:00:00.000Z"),
            ISODate("2022-01-31T00:00:00.000Z"),
          ],
        }            
      }        
    }           
  }},
];

// Execute the aggregation and print the result
var result = db.enquiries.aggregate(pipeline);
printjson(result);  // MongoDB Shell script output
result;             // VSCode MongoDB Playground output
