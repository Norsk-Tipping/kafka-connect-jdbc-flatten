/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.ConnectorDefinition;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.primitives.Longs.max;
import static org.junit.Assert.assertEquals;

public class JdbcSinkPerfTest extends EasyMockSupport {
  private static KafkaProducer<Object, Object> producer = null;
  private static ProducerRecord<Object, Object> record = null;
  private Properties props = new Properties();
  private AdminClient adminClient;
  private JMXConnector jmxc;
  private final MetricRegistry metrics = new MetricRegistry();
  private final Counter recordsRead = metrics.counter("recordsRead");
  private final Counter recordsReadNotYetFlushed = metrics.counter("recordsReadNotYetFlushed");
  private final Counter recordsProduced = metrics.counter("recordsProduced");
  private final Counter recordsInDb = metrics.counter("recordsInDb");
  private Thread t;
  private KafkaConnectClient connectClient;
  private final NetezzaHelper netezzaHelper = new NetezzaHelper("REPLACE");
  private final CsvReporter reporter = CsvReporter.forRegistry(metrics)
          .formatFor(Locale.US)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(new File("perfTest"));



  @Before
  public void setUp() throws IOException, SQLException {

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    props.put("schema.registry.url", "http://REPLACE");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "REPLACE");
    props.put("topics", "jdbcSinkPerfTestTopic");
    props.put("connectorName", "JDBCPerfTestSinkConnector");

    adminClient = AdminClient.create(props);
    CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic(props.getProperty("topics"), 1, (short)1)));
    result.values().forEach((k,v) -> {

      try {
        v.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        try {
          tearDown();
        } catch (IOException | SQLException e1) {
          e1.printStackTrace();
        }
      }
      System.out.println("Created topics: " + k);
    });
    final Configuration connectConfiguration = new Configuration("http://REPLACE");
    connectClient = new KafkaConnectClient(connectConfiguration);
    final ConnectorDefinition connectorDefinition = connectClient.addConnector(NewConnectorDefinition.newBuilder()
            .withName(props.getProperty("connectorName"))
            .withConfig("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector_Flatten")
            .withConfig("dialect.name", "NetezzaSqlDatabaseDialect")
            .withConfig("connection.password", "svc_kafka_test")
            .withConfig("tasks.max", 1)
            .withConfig("topics", props.getProperty("topics"))
            .withConfig("value.converter.schema.registry.url", "http://REPLACE")
            .withConfig("auto.evolve", "false")
            .withConfig("connection.user", "SVC_KAFKA_TEST")
            .withConfig("auto.create", "true")
            .withConfig("value.converter", "io.confluent.connect.avro.AvroConverter")
            .withConfig("connection.url", "jdbc:netezza://REPLACE")
            .withConfig("insert.mode", "insert")
            .withConfig("pk.mode", "none")
            .withConfig("key.converter", "io.confluent.connect.avro.AvroConverter")
            .withConfig("key.converter.schema.registry.url", "REPLACE")
            .withConfig("db.timezone", "Europe/Oslo")
            .withConfig("batch.size", "50000")
            .withConfig("consumer.override.max.poll.records", 100000)
            .withConfig("consumer.override.fetch.max.bytes" , 100000000)
            .withConfig("errors.log.enable", true)
            .withConfig("errors.log.include.messages", true)
            .withConfig("flatten", true)
            .withConfig("flatten.uppercase", true)
            .withConfig("flatten.coordinates", true)
            .withConfig("flatten.rename_fields",
                    "rootrecord.childrecord.childstring1:cs1, rootrecord.childrecord.childstring2:cs2, "
                            + "rootrecord.rootstring1:rs1, rootrecord.rootstring2:rs2")
            .withConfig("flatten.rename_tables", "JDBCSINKPERFTESTTOPIC_NESTEDTESTVALUE:perftest_nestedvalue, "

                    + "JDBCSINKPERFTESTTOPIC_NESTEDTESTVALUE_ROOTRECORD_CHILDRECORD_CHILDLIST:perftest_childlist")
            .build()
    );


    netezzaHelper.setUp();

    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://REPLACE/jmxrmi");
    jmxc = JMXConnectorFactory.connect(url, null);
    jmxc.connect();
    System.out.println("Set up JMX connection to Kafka Connect");

    final InputStream flattened_key_avro_file = JdbcSinkPerfTest.class.getClassLoader()
            .getResourceAsStream("avro/key.avsc");
    final InputStream flattened_value_avro_file = JdbcSinkPerfTest.class.getClassLoader()
            .getResourceAsStream("avro/value.avsc");
    System.out.println(flattened_value_avro_file.toString());
    final Schema keySchema = new Schema.Parser().parse(flattened_key_avro_file);
    final Schema valueSchema = new Schema.Parser().parse(flattened_value_avro_file);

    producer = new KafkaProducer<>(props);
    GenericRecordBuilder keyBuilder = new GenericRecordBuilder(keySchema);
    final GenericRecord key = keyBuilder.set("INT", 923221).build();
    GenericRecordBuilder valueBuilder = new GenericRecordBuilder(valueSchema);
    Schema rootrecordSchema = valueSchema.getField("ROOTRECORD").schema().getTypes().get(1);
    GenericRecordBuilder rootrecordBuilder = new GenericRecordBuilder(rootrecordSchema);
    //Schema rootElementsrecordSchema = rootrecordSchema.getField("ROOTELEMENTS").schema();
    //GenericRecordBuilder rootElementsrecordBuilder = new GenericRecordBuilder(rootElementsrecordSchema);
    Schema childrecordSchema = rootrecordSchema.getField("CHILDRECORD").schema().getTypes().get(1);
    GenericRecordBuilder childrecordBuilder = new GenericRecordBuilder(childrecordSchema);
    //Schema childElementsrecordSchema = childrecordSchema.getField("CHILDELEMENTS").schema();
    //GenericRecordBuilder childElementsrecordBuilder = new GenericRecordBuilder(childElementsrecordSchema);
    Schema childLevel3ElementsrecordSchema = childrecordSchema.getField("CHILDLIST").schema().getTypes().get(1).getElementType();
    GenericRecordBuilder childLevel3ElementsrecordBuilder = new GenericRecordBuilder(childLevel3ElementsrecordSchema);
    ArrayList<GenericRecord> childList = new ArrayList<>();

    final GenericRecord childLevel3Elementsrecord = childLevel3ElementsrecordBuilder
            .set("CHILDLEVEL3STRING1", "CHILDLEVEL3STRING1VALUE")
            .set("CHILDLEVEL3STRING2", "CHILDLEVEL3STRING2VALUE").build();
    final GenericRecord childLevel3Elementsrecord2 = childLevel3ElementsrecordBuilder
            .set("CHILDLEVEL3STRING1", "CHILDLEVEL3STRING1VALUE2")
            .set("CHILDLEVEL3STRING2", "CHILDLEVEL3STRING2VALUE2").build();
    childList.add(childLevel3Elementsrecord);
    childList.add(childLevel3Elementsrecord2);
    final GenericRecord childElementsrecord = childrecordBuilder
            .set("CHILDSTRING1", "CHILDSTRING1VALUE")
            .set("CHILDSTRING2", "CHILDSTRING2VALUE")
            .set("CHILDLIST", childList).build();
    //final GenericRecord childrecord = childrecordBuilder.set("CHILDRECORD", childElementsrecord).build();
    final GenericRecord rootElementsrecord = rootrecordBuilder.set("ROOTSTRING1", "ROOTSTRING1VALUE")
            .set("ROOTSTRING2", "ROOTSTRING2VALUE")
            .set("CHILDRECORD", childElementsrecord).build();
    //final GenericRecord rootrecord = rootrecordBuilder.set()
    final GenericRecord value = valueBuilder.set("INT", 923221)
            .set("ROOTRECORD", rootElementsrecord).build();

    record = new ProducerRecord<Object, Object>(props.getProperty("topics"),
            key, value);
  }

  @After
  public void tearDown() throws IOException, SQLException {
    t.interrupt();
    producer.close();
    System.out.println("Closed producer");
    jmxc.close();
    System.out.println("Closed jmx connection");
    connectClient.deleteConnector(props.getProperty("connectorName"));
    System.out.println("Removed connector");
    DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(props.getProperty("topics")));
    result.values().forEach((k,v) -> {

      try {
        v.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
        try {
          tearDown();
        } catch (IOException | SQLException e1) {
          e1.printStackTrace();
        }
      }
      System.out.println("Deleted topics: " + k);
    });
    adminClient.deleteConsumerGroups(Arrays.asList("connect-JDBCPerfTestSinkConnector"));
    System.out.println("Removed consumer group");
    netezzaHelper.deleteTable("PERFTEST_CHILDLIST");
    netezzaHelper.deleteTable("PERFTEST_NESTEDVALUE");
    //System.out.println("Deleted tables");
    netezzaHelper.tearDown();
    System.out.println("Closed database connection");
    metrics.getMeters().values().forEach(m -> {
      System.out.println(m.getCount());
    });
    reporter.report();
    reporter.stop();
  }

  @Test
  public void performanceTestSinkConnectorAvro() throws IOException, SQLException {
    reporter.start(1, TimeUnit.SECONDS);
    Runnable runnable = () -> {

      while (true) {
        try {
          ObjectName  destConfigName = new ObjectName(
                  "kafka.connect:type=sink-task-metrics,connector=JDBCPerfTestSinkConnector,task=0");

          //  Create array of attribute names
          String  attrNames[] =
                  {"sink-record-read-total",
                          "sink-record-active-count"
                  };

          //  Get attributes
          AttributeList attrList = jmxc.getMBeanServerConnection().getAttributes(destConfigName, attrNames);

          //  Extract and print attribute values

          Double attrValue;

          attrValue = (Double) attrList.asList().get(0).getValue();
          System.out.println( "The total number of records read from Kafka by this task " +
                  "belonging to the named sink connector in this worker, since the task was " +
                  "last restarted.: " + attrValue.toString() );
          recordsRead.inc(attrValue.longValue() - recordsRead.getCount());

          attrValue = (Double) attrList.asList().get(1).getValue();
          System.out.println( "The number of records that have been read from Kafka but " +
                  "not yet completely committed/flushed/acknowledged by the sink task.: " + attrValue.toString() );
          recordsReadNotYetFlushed.inc(attrValue.longValue() - recordsReadNotYetFlushed.getCount());

          Thread.sleep(1000);
        } catch (InterruptedException | IOException | InstanceNotFoundException
                | MalformedObjectNameException | ReflectionException e) {
        }
      }
    };
    t = new Thread(runnable);
    t.start();

    int throughputPerSecond = 6000;
    int iterationDurationSeconds = 60;
    long messagesSent = 0;
    for (int j = 1; j < iterationDurationSeconds + 1; j++) {
      long startIterationMs = System.currentTimeMillis();


      for (int i = 0; i < throughputPerSecond && System.currentTimeMillis() - startIterationMs < 1000; i++) {
        producer.send(record);
        messagesSent++;
      }
      System.out.println("Iteration number: " + j);
      System.out.println("Iteration start time millis: " + startIterationMs);
      System.out.println("Messages sent: " + messagesSent);
      recordsProduced.inc(messagesSent - recordsProduced.getCount());
      System.out.println("Waiting for remaining time within iteration for milliseconds: "
              + String.valueOf(1000 - (System.currentTimeMillis() - startIterationMs)));
      System.out.println();


      try {
        Thread.sleep(max(1000 - (System.currentTimeMillis() - startIterationMs), 0));
      } catch (InterruptedException e) {
        e.printStackTrace();
        tearDown();
      }



   /* public static byte[] datumToByteArray(Schema schema, GenericRecord datum) throws IOException {
     GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
     try {
        Encoder e = EncoderFactory.get().binaryEncoder(os, null);
        writer.write(datum, e);
       e.flush();
       byte[] byteData = os.toByteArray();
       return byteData;
     } finally {
        os.close();
     }
   }*/
    }
    final long finalMessagesSent = messagesSent;
    // Using Lambda Expression
    CompletableFuture<Boolean> loadingFuture = CompletableFuture.supplyAsync(() -> {
      boolean loadReady = false;
      while (!loadReady) {
        try {
          System.out.println("Consumergroup info on Broker: " +
                  adminClient.listConsumerGroupOffsets("connect-JDBCPerfTestSinkConnector")
                          .partitionsToOffsetAndMetadata().get().values().toString());
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
          try {
            tearDown();
          } catch (IOException | SQLException e1) {
            e1.printStackTrace();
          }
        }
        AtomicLong loadedRecords = new AtomicLong(0);
        try {
          netezzaHelper.select(
                  "SELECT COUNT(*) AS LOADEDCOUNT FROM PERFTEST_NESTEDVALUE",
                  rs -> loadedRecords.set(rs.getInt("LOADEDCOUNT")));
        } catch (SQLException e) {
          e.printStackTrace();
          try {
            tearDown();
          } catch (IOException | SQLException e1) {
            e1.printStackTrace();
          }
        }
        if (loadedRecords.get() >= finalMessagesSent) {
          loadReady = true;
          System.out.println("Produced records: " + finalMessagesSent);
          System.out.println("Loaded records: " + loadedRecords.get());
          recordsInDb.inc(loadedRecords.get() - recordsInDb.getCount());
          continue;
        }
        System.out.println("Produced records: " + finalMessagesSent);
        System.out.println("Loaded records: " + loadedRecords.get());
        recordsInDb.inc(loadedRecords.get() - recordsInDb.getCount());
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
          try {
            tearDown();
          } catch (IOException | SQLException e1) {
            e1.printStackTrace();
          }
        }
      }
      return loadReady;
    });
    Boolean result = null;
    try {
      result = loadingFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      tearDown();
    }
    System.out.println("loading of database through connector complete: " + result);
  }
}