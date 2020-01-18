SPARK_IMAGE_PREFIX = bde2020/spark
SPARK_BRANCH = 2.4.4-hadoop2.7
CASSANDRA_IMAGE = cassandra
CASSANDRA_BRANCH = 3.11.4
DATASET_LOCATION = s3a://big-dataset/yelp-dataset
PACKAGES = datastax:spark-cassandra-connector:2.4.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0

build_spark:
	docker pull ${SPARK_IMAGE_PREFIX}-base:${SPARK_BRANCH}
	docker pull ${SPARK_IMAGE_PREFIX}-master:${SPARK_BRANCH}
	docker pull ${SPARK_IMAGE_PREFIX}-worker:${SPARK_BRANCH}
	docker pull ${SPARK_IMAGE_PREFIX}-submit:${SPARK_BRANCH}
	docker pull ${SPARK_IMAGE_PREFIX}-python-template:${SPARK_BRANCH}

build_cassandra:
	docker pull ${CASSANDRA_IMAGE}:${CASSANDRA_BRANCH}

run_cassandra:
	docker-compose up -d cassandra

stop_cassandra:
	docker-compose stop cassandra

run_spark:
	docker-compose up -d spark-master
	docker-compose up -d spark-worker

stop_spark:
	docker-compose stop spark-worker	
	docker-compose stop spark-master

run_all: run_cassandra run_spark

stop_all: stop_spark stop_cassandra

init:
	docker-compose exec -w /root cassandra mkdir yelp-analysis
	docker-compose exec -w /root spark-master mkdir yelp-analysis
	docker-compose exec -w /root spark-master mkdir yelp-analysis/data

create_schema:
	docker cp cql cassandra:/root/yelp-analysis
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/init.cql
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/business.cql
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/user.cql
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/photo.cql
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/review.cql

drop_schema:
	docker cp cql cassandra:/root/yelp-analysis
	docker-compose exec -w /root/yelp-analysis cassandra /usr/bin/cqlsh --file cql/drop.cql

prepare_data:
	docker-compose cp data/yelp-dataset spark-master:/root/yelp-analysis/data/yelp-dataset

load_data:
	docker cp script spark-master:/root/yelp-analysis
	docker-compose exec -w /root/yelp-analysis spark-master time /spark/bin/spark-submit --conf spark.pyspark.python=python3 --packages ${PACKAGES} script/load_data.py ${DATASET_LOCATION} yelp

run_postgres:
	/usr/bin/docker stop postgres
	/usr/bin/docker rm postgres
	/usr/bin/docker run -p 15432:5432 --name postgres-datastore -v postgres-dbstore:/var/lib/postgresql/data -d postgres:9.5-alpine

run_redis:
	/usr/bin/docker stop redis
	/usr/bin/docker rm redis
	/usr/bin/docker run -p 6379:6379 --name redis -v redis-dbstore:/data -d redis:3-alpine

run_redash_server:
	/usr/bin/docker stop redash_server
	/usr/bin/docker rm redash_server
	/usr/bin/docker run -p 5000:5000 -p 5678:5678 --name redash_server \
	-e "REDASH_REDIS_URL=redis://172.31.34.21:6379/0" \
	-e "REDASH_DATABASE_URL=postgresql://postgres@172.31.34.21:15432/postgres" \
	-e "REDASH_RATELIMIT_ENABLED=false" \
	-d redash server

run_redash_scheduler:
	/usr/bin/docker stop redash_scheduler
	/usr/bin/docker rm redash_scheduler
	/usr/bin/docker run --name redash_scheduler \
	-e "REDASH_REDIS_URL=redis://172.31.34.21:6379/0" \
	-e "REDASH_DATABASE_URL=postgresql://postgres@172.31.34.21:15432/postgres" \
	-d redash scheduler

run_redash_worker:
	/usr/bin/docker stop redash_worker
	/usr/bin/docker rm redash_worker
	/usr/bin/docker run --name redash_worker \
	-e "REDASH_REDIS_URL=redis://172.31.34.21:6379/0" \
	-e "REDASH_DATABASE_URL=postgresql://postgres@172.31.34.21:15432/postgres" \
	-d redash worker

run_redash_celery_worker:
	/usr/bin/docker stop redash_celery-worker
	/usr/bin/docker rm redash_celery-worker
	/usr/bin/docker run --name redash_celery-worker \
	-e "REDASH_REDIS_URL=redis://172.31.34.21:6379/0" \
	-e "REDASH_DATABASE_URL=postgresql://postgres@172.31.34.21:15432/postgres" \
	-d redash celery_worker

run_redash: run_redash_server run_redash_scheduler run_redash_worker run_redash_celery_worker		