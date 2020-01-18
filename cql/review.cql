CREATE TABLE yelp.review (
  review_id text PRIMARY KEY,
  user_id text,
  business_id text,
  stars float,
  review_date timestamp,
  text text,
  useful int,
  funny int,
  cool int,
);
