# homework-airflow
long-test-airflow

## Prerequisite
- Git
- Docker desktop

## Get Started
```bash
git clone https://github.com/devruji/homework-airflow.git

cd homework-airflow

docker-compose up airflow-init
docker-compose up -d
```

## Check instances
```bash
$ docker ps
CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                    PORTS                              NAMES
247ebe6cf87a   apache/airflow:2.3.4   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-worker_1
ed9b09fc84b1   apache/airflow:2.3.4   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-scheduler_1
7cb1fb603a98   apache/airflow:2.3.4   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:8080->8080/tcp             compose_airflow-webserver_1
74f3bbe506eb   postgres:13            "docker-entrypoint.s…"   18 minutes ago   Up 17 minutes (healthy)   5432/tcp                           compose_postgres_1
0bd6576d23cb   redis:latest           "docker-entrypoint.s…"   10 hours ago     Up 17 minutes (healthy)   0.0.0.0:6379->6379/tcp             compose_redis_1
```

*P.S. Result should look like above*

## Initial steps
- Open [http://localhost:8080](http://localhost:8080)
- Login with 
  - user: airflow
  - pwd: airflow
- Set-up database connection
  - con_id: postgres_con
  - con_type: postgres
  - host: postgres
  - schema: postgres
  - login: airflow
  - pwd: airflow
  - port 5432
  - Test connection, you will see pop-up message `Connection successfully tested`
- Go back to `DAGs` Page
  - Enable sunday_pipeline
  - Force trigger DAG
  - Take a look the result
- Done

## Cleaning up
To stop and delete containers, delete volumes with database data and download images, run:
```bash
docker-compose down --volumes --rmi all
```

