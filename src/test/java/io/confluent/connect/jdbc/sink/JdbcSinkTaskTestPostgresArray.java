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

import static io.confluent.connect.jdbc.sink.PostgresHelper.tablesUsed;
import static org.junit.Assert.*;

public class JdbcSinkTaskTestPostgresArray extends EasyMockSupport {
  private final PostgresHelper postgresHelper = new PostgresHelper("JdbcSinkTaskTestPostgres");

  private static final Schema ARRAYSTRUCT = SchemaBuilder.struct()
          .name("com.example.Address")
          .field("street", Schema.STRING_SCHEMA)
          .field("number", Schema.INT8_SCHEMA)
          .build();

  private static final Schema ARRAY = SchemaBuilder.array(ARRAYSTRUCT).optional().defaultValue(null)
          .build();
  private static final Schema ARRAY2 = SchemaBuilder.array(ARRAY).optional().defaultValue(null)
          .build();

  private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person").version(1)
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
          .field("address", ARRAY)
          .build();

  private static final Schema SCHEMA2 = SchemaBuilder.struct().name("com.example.Person").version(2)
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
          .field("address", ARRAY2)
          .build();

  private static final Schema SCHEMAX = SchemaBuilder.struct().name("com.example.Subrecord1").version(1)
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA).optional()
          .field("short", Schema.OPTIONAL_INT16_SCHEMA).optional()
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
          .field("modified", Timestamp.SCHEMA)
          .build();

  private static final Schema SCHEMAY= SchemaBuilder.struct().name("com.example.Subrecord2").version(1)
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("modified", Timestamp.SCHEMA)
          .field("record2again", SCHEMAX).optional().defaultValue(null)
          .build();

  private static final Schema SCHEMA3 = SchemaBuilder.struct().name("com.example.Person").version(1)
          .field("firstname", Schema.STRING_SCHEMA)
          .field("record1", SCHEMAX)
          .field("record2", SCHEMAY)
          .build();

  private static final Schema KEYSCHEMA = SchemaBuilder.struct().name("com.example.PersonKey").version(1)
          .field("keyInt", Schema.INT32_SCHEMA)
          .field("keyName", Schema.STRING_SCHEMA)
          .build();



  private static final Schema RECORDTHREE = SchemaBuilder.struct().name("com.example.Record3").version(1)
          .field("string", Schema.STRING_SCHEMA)
          .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();
  private static final Schema RECORDFOUR = SchemaBuilder.struct().name("com.example.Record4").version(1)
          .field("long", Schema.OPTIONAL_INT64_SCHEMA)
          .field("modified", Timestamp.SCHEMA)
          .build();
  private static final Schema ARRAYTHREE = SchemaBuilder.array(RECORDFOUR).optional().defaultValue(null)
          .build();
  private static final Schema RECORDTWO = SchemaBuilder.struct().name("com.example.Record2").version(1)
          .field("string", Schema.STRING_SCHEMA)
          .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("array3", ARRAYTHREE)
          .build();
  private static final Schema ARRAYONE = SchemaBuilder.array(Schema.INT32_SCHEMA).optional().defaultValue(null)
          .build();
  private static final Schema ARRAYTWO = SchemaBuilder.array(RECORDTHREE).optional().defaultValue(null)
          .build();
  private static final Schema MAINRECORD = SchemaBuilder.struct().name("com.example.Mainrecord").version(1)
          .field("string1", Schema.STRING_SCHEMA)
          .field("string2", Schema.STRING_SCHEMA)
          .field("array1", ARRAYONE)
          .field("array2", ARRAYTWO)
          .field("record2", RECORDTWO)
          .build();

  private static final Schema SALESKEY = SchemaBuilder.struct().name("salesKey")
          .field("salesNo", Schema.STRING_SCHEMA)
          .field("customerNo", Schema.STRING_SCHEMA)
          .build();

  private static final Schema EMPLOYEE = SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("departmentNo", Schema.STRING_SCHEMA)
          .field("mobile", Schema.STRING_SCHEMA)
          .build();

  private static final Schema STAFF = SchemaBuilder.struct()
          .field("supportType", Schema.STRING_SCHEMA)
          .field("employee", EMPLOYEE)
          .build();

  private static final Schema PRODUCTCODES = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().defaultValue(null)
          .build();

  private static final Schema PAYMENT = SchemaBuilder.struct()
          .field("sumPayed", Schema.STRING_SCHEMA)
          .field("id", Schema.STRING_SCHEMA)
          .field("productCodes", PRODUCTCODES)
          .build();

  private static final Schema STAFFARRAY = SchemaBuilder.array(STAFF).optional().defaultValue(null)
          .build();

  private static final Schema SALESINFO = SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("staff", STAFFARRAY)
          .build();

  private static final Schema SALESEVENT = SchemaBuilder.struct().name("salesEvent")
          .field("payment", PAYMENT)
          .field("companyNo", Schema.STRING_SCHEMA)
          .field("salesInfo", SALESINFO)
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
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndNullInArray() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "kafka");
    //props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct = new Struct(SCHEMA)
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
            .put("address", null);

    final String topic = "atopic";
    String tableName = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    tablesUsed.add(tableName);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("__connect_topic"));
                        assertEquals(1, rs.getInt("__connect_partition"));
                        assertEquals(42, rs.getLong("__connect_offset"));
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
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInArray() throws Exception {
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

    final List<Struct> addresses = new ArrayList<Struct>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses.add(address1);
    addresses.add(address2);

    final Struct struct = new Struct(SCHEMA)
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
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
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
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getInt8("number"), (Byte) rs.getByte("number"));
                          i--;
                        }
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInArrayNullValues() throws Exception {
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

    final List<Struct> addresses = new ArrayList<Struct>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses.add(address1);
    addresses.add(address2);

    final Struct struct = new Struct(SCHEMA)
            .put("firstname", "Alex")
            .put("lastname", "Smith")
            .put("float", (float) 2356.3)
            .put("age", 21)
            .put("modified", new Date(1474661402123L))
            .put("address", addresses );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
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
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
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
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getInt8("number"), (Byte) rs.getByte("number"));
                          i--;
                        }
                      }
                    }
            )
    );
  }


  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArray() throws Exception {
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

    final List<Struct> addresses1 = new ArrayList<>();
    final List<Struct> addresses2 = new ArrayList<>();
    final List<List> addressesarray = new ArrayList<>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses1.add(address1);
    addresses2.add(address2);
    addressesarray.add(addresses1);
    addressesarray.add(addresses2);

    final Struct struct = new Struct(SCHEMA2)
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
            .put("address", addressesarray );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA2, struct, 42)
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
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getInt8("number"), (Byte) rs.getByte("number"));
                          i--;
                        }
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInRecord() throws Exception {
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
    props.put("flatten.uppercase", "false");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct1 = new Struct(SCHEMAX)
            .put("firstname", "Alex")
            .put("lastname", "Alexis")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("double", -2436546.56457)
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMAY)
            .put("firstname", "John")
            .put("lastname", "Fernandes")
            .put("modified", new Date(1474661402123L));

    final Struct struct3 = new Struct(SCHEMA3)
            .put("firstname", "John")
            .put("record1", struct1)
            .put("record2", struct2);

    final String topic = "atopic";
    String tableName = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    tablesUsed.add(tableName);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA3, struct3, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct3.getString("firstname"), rs.getString("person_firstname"));
                        assertEquals(struct3.getStruct("record2").getString("firstname"),
                                rs.getString("person_record2_firstName"));
                        assertEquals(struct3.getStruct("record2").getString("lastname"),
                                rs.getString("person_record2_lastName"));
                        assertEquals(struct3.getStruct("record1").getBoolean("bool"),
                                rs.getBoolean("person_record1_bool"));
                        assertEquals(struct3.getStruct("record1").getInt16("short").shortValue(),
                                rs.getShort("person_record1_short"));
                        assertEquals(struct3.getStruct("record1").getString("firstname"),
                                rs.getString("person_record1_firstName"));
                        assertEquals(struct3.getStruct("record1").getString("lastname"),
                                rs.getString("person_record1_lastName"));
                        assertEquals(struct3.getStruct("record1").getFloat64("double"),
                                rs.getDouble("person_record1_double"), 0.01);

                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "person_record2_modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp.getTime());
                        java.sql.Timestamp dbTimestamp2 = rs.getTimestamp(
                                "person_record1_modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp2.getTime());
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInRecordInRecord() throws Exception {
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
    props.put("flatten.uppercase", "false");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct1 = new Struct(SCHEMAX)
            .put("firstname", "Alex")
            .put("lastname", "Alexis")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("double", -2436546.56457)
            .put("modified", new Date(1474661402123L));

    final Struct struct2again = new Struct(SCHEMAX)
            .put("firstname", "JohnsBrother")
            .put("lastname", "FernandesFamilyMemberAdditional")
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMAY)
            .put("firstname", "John")
            .put("lastname", "Fernandes")
            .put("modified", new Date(1474661402123L))
            .put("record2again", struct2again);

    final Struct struct3 = new Struct(SCHEMA3)
            .put("firstname", "John")
            .put("record1", struct1)
            .put("record2", struct2);

    final String topic = "atopic";
    String tableName = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    tablesUsed.add(tableName);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA3, struct3, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct3.getString("firstname"), rs.getString("person_firstname"));
                        assertEquals(struct3.getStruct("record2").getString("firstname"),
                                rs.getString("person_record2_firstname"));
                        assertEquals(struct3.getStruct("record2").getString("lastname"),
                                rs.getString("person_record2_lastName"));
                        assertEquals(struct3.getStruct("record1").getBoolean("bool"),
                                rs.getBoolean("person_record1_bool"));
                        assertEquals(struct3.getStruct("record1").getInt16("short").shortValue(),
                                rs.getShort("person_record1_short"));
                        assertEquals(struct3.getStruct("record1").getString("firstname"),
                                rs.getString("person_record1_firstName"));
                        assertEquals(struct3.getStruct("record1").getString("lastname"),
                                rs.getString("person_record1_lastName"));
                        assertEquals(struct3.getStruct("record1").getFloat64("double"),
                                rs.getDouble("person_record1_double"), 0.01);

                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "person_record2_modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp.getTime());

                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("firstname"),
                                rs.getString("person_record2_record2again_firstName"));
                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("lastname"),
                                rs.getString("person_record2_record2again_lastname"));
                        java.sql.Timestamp dbTimestamp3 = rs.getTimestamp(
                                "person_record2_record2again_modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record2").getStruct("record2again")
                                .get("modified")).getTime(), dbTimestamp3.getTime());

                        java.sql.Timestamp dbTimestamp2 = rs.getTimestamp(
                                "person_record1_modified",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp2.getTime());
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInRecordInRecordRenameRecords() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("flatten.rename_tables", "atopic_person:a_p, atopic_record1:a_r1, atopic_record2_record2again: a_r2_r2again");
    props.put("flatten.rename_fields",
            "person.timestamp_type:t_t, " +
            "person.firstname:fn,  " +
            "person.record1.modified:r1m, " +
            "person.record1.bool:b, " +
            "person.record1.double:r1d, " +
            "person.record1.short:r1s, " +
            "person.record2.firstname:r2fn, " +
            "person.record2.lastname:r2ln, " +
            "person.record2.modified:r2m, " +
            "person.record1.lastname:r1ln, " +
            "person.record1.firstname:r1fn, " +
            "person.record2.record2again.modified:r2am, " +
            "person.record2.record2again.lastname:r2aln, " +
            "person.record2.record2again.firstname:r2afn ");
    props.put("pk.mode", "none");
    //props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("flatten.uppercase", "false");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct1 = new Struct(SCHEMAX)
            .put("firstname", "Alex")
            .put("lastname", "Alexis")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("double", -2436546.56457)
            .put("modified", new Date(1474661402123L));

    final Struct struct2again = new Struct(SCHEMAX)
            .put("firstname", "JohnsBrother")
            .put("lastname", "FernandesFamilyMemberAdditional")
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMAY)
            .put("firstname", "John")
            .put("lastname", "Fernandes")
            .put("modified", new Date(1474661402123L))
            .put("record2again", struct2again);

    final Struct struct3 = new Struct(SCHEMA3)
            .put("firstname", "John")
            .put("record1", struct1)
            .put("record2", struct2);


    final String topic = "atopic";
    String tableName = "a_p";
    tablesUsed.add(tableName);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA3, struct3, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct3.getString("firstname"), rs.getString("fn"));
                        assertEquals(struct3.getStruct("record2").getString("firstname"),
                                rs.getString("r2fn"));
                        assertEquals(struct3.getStruct("record2").getString("lastname"),
                                rs.getString("r2ln"));
                        assertEquals(struct3.getStruct("record1").getBoolean("bool"),
                                rs.getBoolean("b"));
                        assertEquals(struct3.getStruct("record1").getInt16("short").shortValue(),
                                rs.getShort("r1s"));
                        assertEquals(struct3.getStruct("record1").getString("firstname"),
                                rs.getString("r1fn"));
                        assertEquals(struct3.getStruct("record1").getString("lastname"),
                                rs.getString("r1ln"));
                        assertEquals(struct3.getStruct("record1").getFloat64("double"),
                                rs.getDouble("r1d"), 0.01);

                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "r2m",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp.getTime());

                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("firstname"),
                                rs.getString("r2afn"));
                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("lastname"),
                                rs.getString("r2aln"));
                        java.sql.Timestamp dbTimestamp3 = rs.getTimestamp(
                                "r2am",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record2").getStruct("record2again")
                                .get("modified")).getTime(), dbTimestamp3.getTime());

                        java.sql.Timestamp dbTimestamp2 = rs.getTimestamp(
                                "r1m",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record1").get("modified")).getTime(), dbTimestamp2.getTime());
                      }
                    }
            )
    );
  }
  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndRecordInRecordInRecordRenameRecordsWhitelistSubContainers() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("flatten.containers.whitelist", "person, person.record2.record2again");
    props.put("flatten.rename_tables", "atopic_person:a_p, atopic_record1:a_r1, atopic_record2_record2again: a_r2_r2again");
    props.put(       "flatten.rename_fields",
                    "person.timestamp_type:t_t, " +
                    "person.firstname:fn,  " +
                    "person.record1.modified:r1m, " +
                    "person.record1.bool:b, " +
                    "person.record1.double:r1d, " +
                    "person.record1.short:r1s, " +
                    "person.record2.firstname:r2fn, " +
                    "person.record2.lastname:r2ln, " +
                    "person.record2.modified:r2m, " +
                    "person.record1.lastname:r1ln, " +
                    "person.record1.firstname:r1fn, " +
                    "person.record2.record2again.modified:r2am, " +
                    "person.record2.record2again.lastname:r2aln, " +
                    "person.record2.record2again.firstname:r2afn ");
    props.put("pk.mode", "none");
    //props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("flatten.uppercase", "false");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct struct1 = new Struct(SCHEMAX)
            .put("firstname", "Alex")
            .put("lastname", "Alexis")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("double", -2436546.56457)
            .put("modified", new Date(1474661402123L));

    final Struct struct2again = new Struct(SCHEMAX)
            .put("firstname", "JohnsBrother")
            .put("lastname", "FernandesFamilyMemberAdditional")
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMAY)
            .put("firstname", "John")
            .put("lastname", "Fernandes")
            .put("modified", new Date(1474661402123L))
            .put("record2again", struct2again);

    final Struct struct3 = new Struct(SCHEMA3)
            .put("firstname", "John")
            .put("record1", struct1)
            .put("record2", struct2);


    final String topic = "atopic";
    String tableName = "a_p";
    tablesUsed.add(tableName);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA3, struct3, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafkatopic"));
                        assertEquals(1, rs.getInt("kafkapartition"));
                        assertEquals(42, rs.getLong("kafkaoffset"));
                        assertEquals(struct3.getString("firstname"), rs.getString("fn"));
                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("firstname"),
                                rs.getString("r2afn"));
                        assertEquals(struct3.getStruct("record2").getStruct("record2again").getString("lastname"),
                                rs.getString("r2aln"));
                        java.sql.Timestamp dbTimestamp3 = rs.getTimestamp(
                                "r2am",
                                DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((Date) struct3.getStruct("record2").getStruct("record2again")
                                .get("modified")).getTime(), dbTimestamp3.getTime());

                      }
                    }
            )
    );
  }
  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeys() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "none");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "false");

    props.put("flatten.pk_propagate_value_fields", "person.float, person.modified");
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> addresses1 = new ArrayList<>();
    final List<Struct> addresses2 = new ArrayList<>();
    final List<List> addressesarray = new ArrayList<>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses1.add(address1);
    addresses2.add(address2);
    addressesarray.add(addresses1);
    addressesarray.add(addresses2);

    final Struct struct = new Struct(SCHEMA2)
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
            .put("address", addressesarray );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA2, struct, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(struct.getString("firstname"), rs.getString("firstname"));
                        assertEquals(struct.getString("lastname"), rs.getString("lastname"));
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                        assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                        java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                "person_modified",
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
                          assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                          java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                  "person_modified",
                                  DateTimeUtils.getTimeZoneCalendar(timeZone)
                          );
                          assertEquals(((Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getInt8("number"), (Byte) rs.getByte("number"));
                          i--;
                        }
                      }
                    }
            )
    );
  }


  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeysFromValuePropagated() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "flatten");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "false");
    props.put("pk.fields", "person.address.address.address.street");
    props.put("flatten.pk_propagate_value_fields", "person.float, person.modified");
    props.put(      "flatten.rename_fields",
                    "person.address.address.address.street:street, " +
                    "person.modified:modified");
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> addresses1 = new ArrayList<>();
    final List<Struct> addresses2 = new ArrayList<>();
    final List<List> addressesarray = new ArrayList<>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses1.add(address1);
    addresses2.add(address2);
    addressesarray.add(addresses1);
    addressesarray.add(addresses2);

    final Struct struct = new Struct(SCHEMA2)
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
            .put("address", addressesarray );

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address_address";
    tablesUsed.add(tableName2);
    tablesUsed.add(tableName1);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA2, struct, 42)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(struct.getString("firstname"), rs.getString("firstname"));
                        assertEquals(struct.getString("lastname"), rs.getString("lastname"));
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
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
                          assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                          java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                  "modified",
                                  DateTimeUtils.getTimeZoneCalendar(timeZone)
                          );
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) ((ArrayList) struct.getArray("address")
                                  .get(i)).get(0)).getInt8("number"), (Byte) rs.getByte("number"));
                          i--;
                        }
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeysFromKeyPropagated() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "flatten");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "false");
    props.put("pk.fields", "person.address.address.street, personkey.keyint");
    props.put("flatten.pk_propagate_value_fields", "person.float, person.modified");
    props.put(      "flatten.rename_fields",
                    "person.address.address.street:street, " +
                    "person.modified:modified");
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> addresses = new ArrayList<>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses.add(address1);
    addresses.add(address2);

    final Struct struct = new Struct(SCHEMA)
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

    final Struct keyStruct1 = new Struct(KEYSCHEMA)
            .put("keyInt", 13)
            .put("keyName", "KeyString1");

    final Struct keyStruct2 = new Struct(KEYSCHEMA)
            .put("keyInt", 14)
            .put("keyName", "KeyString2");

    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, SCHEMA, struct, 42)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct2, SCHEMA, struct, 43)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        int i = 0;
                        while (rs.next()) {
                          assertEquals(struct.getString("firstname"), rs.getString("firstname"));
                          assertEquals(struct.getString("lastname"), rs.getString("lastname"));
                          assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                          assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                          assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                          assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                          assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                          assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                          assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                          java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                  "modified",
                                  DateTimeUtils.getTimeZoneCalendar(timeZone)
                          );
                          assertEquals(((Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                          i++;
                        }
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
                          if (i == 2) {
                            i=0;
                          }
                          assertEquals(struct.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                          java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                  "modified",
                                  DateTimeUtils.getTimeZoneCalendar(timeZone)
                          );
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getString("street"), rs.getString("street"));
                          assertEquals(((Struct) struct.getArray("address")
                                  .get(i)).getInt8("number"), (Byte) rs.getByte("number"));
                          i++;
                        }
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                          assertEquals(rs.getInt(1), 4);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 2);
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeysFromKeyPropagatedUpsert() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "flatten");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "false");
    props.put("pk.fields", "person.address.address.street, personkey.keyint");
    props.put("flatten.pk_propagate_value_fields", "person.float, person.modified");
    props.put(      "flatten.rename_fields",
            "person.address.address.street:street, " +
                    "person.modified:modified");
    props.put("insert.mode", "upsert");
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> addresses = new ArrayList<>();

    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 9);

    addresses.add(address1);
    addresses.add(address2);

    final List<Struct> addressesUpsert = new ArrayList<>();

    final Struct address1Upsert = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number",(byte) 100);
    final Struct address2Upsert = new Struct(ARRAYSTRUCT)
            .put("street", "Måsabekkvegen")
            .put("number",(byte) 100);

    addressesUpsert.add(address1Upsert);
    addressesUpsert.add(address2Upsert);

    final Struct struct = new Struct(SCHEMA)
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

    final Struct structUpsert = new Struct(SCHEMA)
            .put("firstname", "UpdatedAlex")
            .put("lastname", "UpdatedSmith")
            .put("bool", false)
            .put("short", (short) 0)
            .put("byte", (byte) 0)
            .put("long", 0L)
            .put("float", (float) 2356.3)
            .put("double", 0.1)
            .put("age", 0)
            .put("modified", new Date(1474661402123L))
            .put("address", addresses );

    final Struct structUpsertWithArrayUpsert = new Struct(SCHEMA)
            .put("firstname", "UpdatedAlex")
            .put("lastname", "UpdatedSmith")
            .put("bool", false)
            .put("short", (short) 0)
            .put("byte", (byte) 0)
            .put("long", 0L)
            .put("float", (float) 2356.3)
            .put("double", 0.1)
            .put("age", 0)
            .put("modified", new Date(1474661402123L))
            .put("address", addressesUpsert );

    final Struct keyStruct1 = new Struct(KEYSCHEMA)
            .put("keyInt", 13)
            .put("keyName", "KeyString1");

    final Struct keyStruct2 = new Struct(KEYSCHEMA)
            .put("keyInt", 14)
            .put("keyName", "KeyString2");



    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, SCHEMA, struct, 42)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct2, SCHEMA, struct, 43)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, SCHEMA, structUpsert, 44)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct2, SCHEMA, structUpsertWithArrayUpsert, 45)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT * FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        int i = 0;
                        while (rs.next()) {
                          assertEquals(structUpsert.getString("firstname"), rs.getString("firstname"));
                          assertEquals(structUpsert.getString("lastname"), rs.getString("lastname"));
                          assertEquals(structUpsert.getBoolean("bool"), rs.getBoolean("bool"));
                          assertEquals(structUpsert.getInt8("byte").byteValue(), rs.getByte("byte"));
                          assertEquals(structUpsert.getInt16("short").shortValue(), rs.getShort("short"));
                          assertEquals(structUpsert.getInt32("age").intValue(), rs.getInt("age"));
                          assertEquals(structUpsert.getInt64("long").longValue(), rs.getLong("long"));
                          assertEquals(structUpsert.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                          assertEquals(structUpsert.getFloat64("double"), rs.getDouble("double"), 0.01);
                          java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                  "modified",
                                  DateTimeUtils.getTimeZoneCalendar(timeZone)
                          );
                          assertEquals(((Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                          i++;
                        }
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
                        int j = 1;
                        while(rs.next()){
                          if (i == 2) {
                            i=0;
                          }
                          if (j <= 1) {
                            assertEquals(structUpsert.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                            java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                    "modified",
                                    DateTimeUtils.getTimeZoneCalendar(timeZone)
                            );
                            assertEquals(((Struct) structUpsert.getArray("address")
                                    .get(i)).getString("street"), rs.getString("street"));
                            assertEquals(((Struct) structUpsert.getArray("address")
                                    .get(i)).getInt8("number"), (Byte) rs.getByte("number"));
                          }
                          if (j > 1) {
                            assertEquals(structUpsertWithArrayUpsert.getFloat32("float"), rs.getFloat("person_float"), 0.01);
                            java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                                    "modified",
                                    DateTimeUtils.getTimeZoneCalendar(timeZone)
                            );
                            assertEquals(((Struct) structUpsertWithArrayUpsert.getArray("address")
                                    .get(i)).getString("street"), rs.getString("street"));
                            assertEquals(((Struct) structUpsertWithArrayUpsert.getArray("address")
                                    .get(i)).getInt8("number"), (Byte) rs.getByte("number"));
                          }

                          j++;
                          i++;
                        }
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 4);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 2);
                      }
                    }
            )
    );
  }


  @Test
  public void putLargerStructureUpsert() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "flatten");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");
    props.put("flatten.coordinates", "true");
    props.put("flatten.uppercase", "false");
    props.put("insert.mode", "upsert");
    props.put("flatten.pk_propagate_value_fields", "mainrecord.string1, mainrecord.array2.array2.float, mainrecord.record2.array3.array3.long");
    props.put("pk.fields", "personkey.keyint");
    props.put("flatten.rename_tables", "biggerstruct_mainrecord:biggerstruct_mr, biggerstruct_mainrecord_array1:biggerstruct_array1" +
            ",biggerstruct_mainrecord_array2:biggerstruct_array2,biggerstruct_mainrecord_record2_array3:biggerstruct_array3");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);
    final Struct keyStruct1 = new Struct(KEYSCHEMA)
            .put("keyInt", 13)
            .put("keyName", "KeyString1");
    final Struct struct3_1 = new Struct(RECORDTHREE)
            .put("string", "string1value")
            .put("float", (float) 2356.3)
            .put("double", 0.1)
            ;
    final Struct struct3_2 = new Struct(RECORDTHREE)
            .put("string", "abcde")
            .put("float", (float) 98232.3)
            .put("double", 4.1)
            ;
    final ArrayList<Integer> array1 = new ArrayList<>(Arrays.asList(12, 199));
    final ArrayList<Struct> array2 = new ArrayList<>( Arrays.asList(struct3_1, struct3_2));
    final Struct struct4_1 = new Struct(RECORDFOUR)
            .put("long", 123L)
            .put("modified", new Date(1473444402123L))
            ;
    final Struct struct4_2 = new Struct(RECORDFOUR)
            .put("long", 456L)
            .put("modified", new Date(1476661765323L))
            ;
    final Struct struct4_3 = new Struct(RECORDFOUR)
            .put("long", 789L)
            .put("modified", new Date(1428261402123L))
            ;
    final ArrayList<Struct> array3 = new ArrayList<>( Arrays.asList(struct4_1, struct4_2, struct4_3));
    final Struct record2 = new Struct(RECORDTWO)
            .put("string", "sfsf")
            .put("float", (float) 999923.3)
            .put("double", 0.1)
            .put("array3", array3)
            ;
    final Struct mainrecord = new Struct(MAINRECORD)
            .put("string1", "pk")
            .put("string2", "string2value")
            .put("array1", array1)
            .put("array2", array2)
            .put("record2", record2);
    final String topic = "biggerstruct";

    final Struct struct3_1b = new Struct(RECORDTHREE)
            .put("string", "changed")
            .put("float", (float) 2356.3)
            .put("double", 0.0)
            ;
    final Struct struct3_2b = new Struct(RECORDTHREE)
            .put("string", "changed")
            .put("float", (float) 98232.3)
            .put("double", 0.0)
            ;
    final ArrayList<Integer> array1b = new ArrayList<>(Arrays.asList(0, 0));
    final ArrayList<Struct> array2b = new ArrayList<>( Arrays.asList(struct3_1b, struct3_2b));
    final Struct struct4_1b = new Struct(RECORDFOUR)
            .put("long", 123L)
            .put("modified", new Date(1423444402123L))
            ;
    final Struct struct4_2b = new Struct(RECORDFOUR)
            .put("long", 456L)
            .put("modified", new Date(1474661765323L))
            ;
    final Struct struct4_3b = new Struct(RECORDFOUR)
            .put("long", 789L)
            .put("modified", new Date(1422261402123L))
            ;
    final ArrayList<Struct> array3b = new ArrayList<>( Arrays.asList(struct4_1b, struct4_2b, struct4_3b));
    final Struct record2b = new Struct(RECORDTWO)
            .put("string", "changed")
            .put("float", (float) 0.0)
            .put("double", 0.0)
            .put("array3", array3)
            ;
    final Struct mainrecordb = new Struct(MAINRECORD)
            .put("string1", "pk")
            .put("string2", "changed")
            .put("array1", array1b)
            .put("array2", array2b)
            .put("record2", record2b);

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, MAINRECORD, mainrecord, 42)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, MAINRECORD, mainrecordb, 43)
    ));

  }
  @Test
  public void putSalesExample() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("flatten.coordinates", "true");
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    String timeZoneID = "Europe/Oslo";
    props.put("db.timezone", timeZoneID);
    props.put("flatten", "true");


    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final Struct salesKey1 = new Struct(SALESKEY)
            .put("salesNo", "132323")
            .put("customerNo", "9789789");

    final Struct employee1_1 = new Struct(EMPLOYEE)
            .put("id", "232323")
            .put("departmentNo", "34334")
            .put("mobile", "+47 232334")
            ;

    final Struct staff1_1 = new Struct(STAFF)
            .put("supportType", "marketing")
            .put("employee", employee1_1)
            ;
    final Struct employee1_2 = new Struct(EMPLOYEE)
            .put("id", "3442")
            .put("departmentNo", "2781")
            .put("mobile", "+47 990332")
            ;
    final Struct staff1_2 = new Struct(STAFF)
            .put("supportType", "sales")
            .put("employee", employee1_2)
            ;

    final ArrayList<Struct> staffArray1 = new ArrayList<>(Arrays.asList(staff1_1, staff1_2));

    final Struct salesInfo1 = new Struct(SALESINFO)
            .put("id", "1112")
            .put("staff", staffArray1)
            ;

    final ArrayList<String> productcodes = new ArrayList<>( Arrays.asList("codeX", "codeY", "codeZ"));

    final Struct payment1 = new Struct(PAYMENT)
            .put("sumPayed", "1009.05")
            .put("id", "XZ-ZZSD23")
            .put("productCodes", productcodes)
            ;

    final Struct salesEvent1 = new Struct(SALESEVENT)
            .put("payment", payment1)
            .put("companyNo", "NO-122")
            .put("salesInfo", salesInfo1)
            ;

    final String topic = "Sales";

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, SALESKEY, salesKey1, SALESEVENT, salesEvent1, 1)
    ));

  }
}