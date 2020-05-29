# Derivco Webinar - 2020-05-28

The files in this directory contain the various code snippets used within the webinar.

The code is intended for demonstration purposes only.

The main aim of this talk is to show what is possible with Kafka an KSQLDB and provide a starting point to go further.

There's a lot of awesome blogs doing the same, you can find them in the references below.

The webinar had two technical sessions:
1. Simple producer and consumer
2. Python Producer, KSQL, Notebook example

## Kafka Cluster
Before you get started, you will need a running Kafka cluster.
The code in the examples assume a local cluster on port 9092.

To get a local cluster up you can look at this quickstart:
https://ksqldb.io/quickstart.html

## 1. Simple Producer and Consumer
- The Producer and Consumer projects can be used for this.
- With the dotnet sdk installed, you can open the directory in a shell and type '''dotnet run''' to start the app.

## 2. ksqlDB and data analysis
- Start by running the python_producer
- Run the manual steps and queries in the Queries.txt file, one by one.
- You can now open the webinar_ui code in Visual Studio
- You can now run through the Python Jupyter notebook in VS Code or an editor of your choice.

# References and further reading
## Kafka:
https://kafka.apache.org/
https://kafka.apache.org/intro

## Confluent:
https://www.confluent.io/
https://www.confluent.io/download/
https://docs.confluent.io/current/

## Cheat Sheets:
https://github.com/lensesio/kafka-cheat-sheet
https://gist.github.com/sahilsk/d2a6ec384f5f2333e3fef40a581a97e1

## Books:
https://www.confluent.io/resources/kafka-the-definitive-guide/

## KSQLDB:
https://ksqldb.io/
https://docs.ksqldb.io/en/latest/

## Machine Learning with Kafka and KSQLDB
https://www.confluent.io/blog/build-deploy-scalable-machine-learning-production-apache-kafka/
https://github.com/kaiwaehner/python-jupyter-apache-kafka-ksql-tensorflow-keras

