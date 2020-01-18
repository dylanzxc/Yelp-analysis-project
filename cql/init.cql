CREATE KEYSPACE yelp WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': 2
};

USE yelp;