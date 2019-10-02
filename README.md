# Kafka Connect JDBC Connector merged with Flatten feature

kafka-connect-jdbc-flatten is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from any JDBC-compatible database with a flatten feature that can be activated with 
config parameter "flatten": "true". Map and array structures are with this feature dereferenced and written to
their own target tables.

The connector extends the Confluent jdbc sink connector:
https://github.com/confluentinc/kafka-connect-jdbc

# License

This project is licensed under the [Confluent Community License](LICENSE).
