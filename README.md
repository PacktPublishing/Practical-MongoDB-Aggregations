# Practical MongoDB Aggregations

Code examples for the second half of the book _Practical MongoDB Aggregations_, published by Packt and authored by Paul Done.

# Book Description

Officially endorsed by MongoDB, Inc., _Practical MongoDB Aggregations_ helps you unlock the full potential of the MongoDB aggregation framework, including the latest features of MongoDB 7.0. The book provides practical, easy-to-digest principles and approaches for increasing your effectiveness in developing aggregation pipelines, supported by examples for building pipelines to solve complex data manipulation and analytical tasks.

The book is customized for developers, architects, data analysts, data engineers, and data scientists with some familiarity with the aggregation framework. It begins by explaining the framework's architecture and then shows you how to build pipelines optimized for productivity and scale. 

Given the critical role arrays play in MongoDB's document model, the book delves into best practices for optimally manipulating arrays. The latter part of the book equips you with examples to solve common data processing challenges so you can apply the lessons you've learned to practical situations.

# Code Example Instructions

You can find the code for each example in a sub-folder of this repository's `examples` folder. 

You can run an example by copying and pasting the example's code into MongoDB Shell, running the file as a MongoDB Shell script, or running in VSCode using the MongoDB for VScode Playground.

The following command line shows you how to run the first example via the MongoDB Shell as a script: 

```
mongosh --quiet "mongodb+srv://myuser:mypasswd@mycluster.abc123.mongodb.net/" examples/1-Foundational-Examples/1-filtered-top-subset.mongodb.js
```

Before running this command, you must correct the username, password, and cluster domain name above to match your own environment.

# Book Author

Paul Done is one of two distinguished solutions architects at MongoDB, Inc., having been at MongoDB for 10 years. He has previously held roles in various software disciplines, including engineering, consulting, and pre-sales, at companies such as Oracle, Novell, and BEA Systems. Paul specializes in databases and middleware, focusing on resiliency, scalability, transactions, event processing, and applying evolvable data model approaches. He spent most of the early 2000s building Java EE (J2EE) transactional systems on WebLogic, integrated with relational databases such as Oracle RAC and messaging systems such as MQSeries.
