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

package no.norsktipping.kafka.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static no.norsktipping.kafka.connect.jdbc.sink.PostgresHelper.tablesUsed;
import static org.junit.Assert.assertEquals;

public class JdbcSinkTaskTestPostgresMap extends EasyMockSupport {
  private final PostgresHelper postgresHelper = new PostgresHelper("JdbcSinkTaskTestPostgres");


  private static final Schema MAP = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().defaultValue(null)
          .build();

  private static final Schema MAPSTRUCT = SchemaBuilder.struct()
          .name("com.example.Address")
          .field("street", Schema.STRING_SCHEMA)
          .field("number", Schema.INT8_SCHEMA)
          .build();

  private static final Schema MAPWITHSTRUCT = SchemaBuilder.map(Schema.STRING_SCHEMA, MAPSTRUCT).optional().defaultValue(null)
          .build();


  private static final Schema SCHEMAWITHPRIMITIVEMAP = SchemaBuilder.struct().name("com.example.Person").version(1)
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("age", Schema.OPTIONAL_INT32_SCHEMA)
          .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("short", Schema.OPTIONAL_INT16_SCHEMA)
          .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
          .field("long", Schema.OPTIONAL_INT64_SCHEMA)
          .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("modified", Timestamp.SCHEMA)
          .field("address", MAP)
          .build();

  private static final Schema SCHEMAWITHSTRUCTMAP = SchemaBuilder.struct().name("com.example.Person").version(1)
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("age", Schema.OPTIONAL_INT32_SCHEMA)
          .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("short", Schema.OPTIONAL_INT16_SCHEMA)
          .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
          .field("long", Schema.OPTIONAL_INT64_SCHEMA)
          .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("modified", Timestamp.SCHEMA)
          .field("address", MAPWITHSTRUCT)
          .build();


  @Before
  public void setUp() throws IOException, SQLException {
    postgresHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    postgresHelper.tearDown();
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordWithMapWithPrimitives() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "none");
    //props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "true");
    //props.put("flatten.rename_fields", "person.address.key:mapkey, person.address.address:mapvalue");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final HashMap<String, String> addresses = new HashMap<>();

    addresses.put("street", "Måsabekkvegen");
    addresses.put("number", "12");

    final Struct struct = new Struct(SCHEMAWITHPRIMITIVEMAP)
            .put("firstname", "Alex")
            .put("lastname", "Smith")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("byte", (byte) -32)
            .put("long", 12425436L)
            .put("float", (float) 2356.3)
            .put("double", -2436546.56457)
            .put("age", 21)
            .put("modified", new Date(1474661402123L))
            .put("address", addresses );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMAWITHPRIMITIVEMAP.name().substring(SCHEMAWITHPRIMITIVEMAP.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMAWITHPRIMITIVEMAP, struct, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct.getString("firstname"), rs.getString("firstName"));
                        assertEquals(struct.getString("lastname"), rs.getString("lastName"));
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                        assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        int i = 1;
                        while(rs.next()){
                          assertEquals(topic, rs.getString("kafkatopic"));
                          assertEquals(1, rs.getInt("kafkapartition"));
                          assertEquals(42, rs.getLong("kafkaoffset"));
                          assertEquals((struct.getMap("address")
                                  .get(rs.getString("person_address_key"))), rs.getString("address"));
                          i--;
                        }
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordWithMapWithStruct() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "none");
    //props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct addressesStruct = new Struct(MAPSTRUCT)
        .put("street", "Måsabekkvegen")
        .put("number", (byte) 12);

    final HashMap<String, Struct> addresses = new HashMap<>();
    addresses.put("address", addressesStruct);

    final Struct struct = new Struct(SCHEMAWITHSTRUCTMAP)
            .put("firstname", "Alex")
            .put("lastname", "Smith")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("byte", (byte) -32)
            .put("long", 12425436L)
            .put("float", (float) 2356.3)
            .put("double", -2436546.56457)
            .put("age", 21)
            .put("modified", new Date(1474661402123L))
            .put("address", addresses );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMAWITHSTRUCTMAP.name().substring(SCHEMAWITHSTRUCTMAP.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMAWITHSTRUCTMAP, struct, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct.getString("firstname"), rs.getString("firstName"));
                        assertEquals(struct.getString("lastname"), rs.getString("lastName"));
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                        assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        int i = 1;
                        while(rs.next()){
                          assertEquals(topic, rs.getString("kafkatopic"));
                          assertEquals(1, rs.getInt("kafkapartition"));
                          assertEquals(42, rs.getLong("kafkaoffset"));
                          assertEquals((struct.getMap("address")
                                  .get(rs.getString("person_address_key"))), rs.getString("address"));
                          i--;
                        }
                      }
                    }
            )
    );
  }
}