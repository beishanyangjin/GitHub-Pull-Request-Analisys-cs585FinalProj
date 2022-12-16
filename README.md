# cs585FinalProj

## Dataset

https://www.kaggle.com/datasets/stephangarland/ghtorrent-pull-requests

Get it from Kaggle and put it to /share folder

## How to run Spark + Hadoop images

Pull image

```sh
docker pull s1mplecc/spark-hadoop:3
```

start final/docker-compose.yml

```
docker-compose up -d
```

Start hadoop in the container

```sh
$ ./start-hadoop.sh
```

## Run MapReduce sample code

```sh
$ hdfs dfs -put share/words.txt /
$ hadoop jar share/bigdata-learning-0.0.1.jar example.mapreduce.WordCount /words.txt /output
```

## Run Spark sample code

```
$ spark-submit --master spark://master:7077 /opt/share/my_script.py
```

##Run final project

Upload local file to HDFS

```sh
$ hdfs dfs -put share/(local filename) /
```

Run Spark application

```
$ spark-submit --master spark://master:7077 /opt/share/PullRequests.py
```

## Web UI summary

| Web UI                      | Default website                 | Remark                                 |
|:---------------------------:|:----------------------:|:------------------------------------:|
| \* **Spark Application**           | http://localhost:4040  | Start by SparkContext，showing running Spark application |
| Spark Standalone Master     | http://localhost:8080  |  Show cluster state，and Spark app submitted by Standalone mode         |
| \* **HDFS NameNode**               | http://localhost:9870                   | HDFS page                         |
| \* **YARN ResourceManager**        | http://localhost:8088                   | Show  Spark app submitted to YARN       |
| YARN NodeManager            | http://localhost:8042 | Show working nodes config and running log                                     |
| MapReduce Job History | http://localhost:19888 | MapReduce history    |

