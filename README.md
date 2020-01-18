# cmpt732-yelp-analysis

CMPT732 Yelp Analysis Project

## Installation

 - Make

## How to run

 - The scripts need to run on our gateway to access Spark/Cassandra cluster
 - Step 1: Create Cassandra tables: make create_schema -f Makefile.production
 - Step 2: Copy data from AWS S3 to Hadoop HDFS: make prepare_data -f Makefile.production
 - Step 3: Load Yelp data from Hadoop HDFS to Cassandra tables: make load_data -f Makefile.production
 - Step 4: Do all analysis: make run_analyze -f Makefile.production
 - Step 5: Store results to Postgres DB: make store_data -f Makefile.production

## Web Frontends:

 - http://18.206.223.176:5000/public/dashboards/NgDgfClanpltU2wTMOsH88ARuOb63KI6y9JsD21S?org_slug=default
 - http://18.206.223.176:5000/public/dashboards/pDlkbZHjXlA8dYEjNVPrxf70fYP0odjaFxyucZ5r?org_slug=default&p_w9_num_city=50&p_w10_num_city=50&p_w11_num_row=100