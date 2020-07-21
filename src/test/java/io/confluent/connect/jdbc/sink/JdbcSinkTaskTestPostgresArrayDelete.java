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
import static org.junit.Assert.assertEquals;

public class JdbcSinkTaskTestPostgresArrayDelete extends EasyMockSupport {
  private final PostgresHelper postgresHelper = new PostgresHelper("JdbcSinkTaskTestPostgres");

  private static final Schema ARRAYSTRUCT = SchemaBuilder.struct()
          .name("com.example.Address")
          .field("street", Schema.STRING_SCHEMA)
          .field("number", Schema.INT8_SCHEMA)
          .build();

  private static final Schema ARRAY = SchemaBuilder.array(ARRAYSTRUCT).optional().defaultValue(null)
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

  private static final Schema KEYSCHEMA = SchemaBuilder.struct().name("com.example.PersonKey").version(1)
          .field("keyInt", Schema.INT32_SCHEMA)
          .field("keyName", Schema.STRING_SCHEMA)
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
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeysFromKeyDelete() throws Exception {
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
    props.put("pk.fields", "personkey.keyint, person.address.address.street");
    props.put(      "flatten.rename_fields",
            "person.address.address.street:street, " +
                    "person.modified:modified");
    props.put("delete.enabled", "true");
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


    final String topic = "atopic";
    String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    String tableName2 = topic + "_" + "person_address";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, SCHEMA, struct, 42)
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
                          assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
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
                        assertEquals(rs.getInt(1), 2);
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
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );

    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, null, null, 43)
    ));
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
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
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
  }

  @Test
  public void putPropagatesToDbWithAutoCreateAndPkModeKafkaAndArrayInArrayAndPKKeysFromKeyDeleteAfterConnectorRestart() throws Exception {
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
    props.put("pk.fields", "personkey.keyint, person.address.address.street");
    props.put(      "flatten.rename_fields",
            "person.address.address.street:street, " +
                    "person.modified:modified");
    props.put("flatten.rename_tables", "atopic_person:p, atopic_person_address:pa");
    props.put("delete.enabled", "true");
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


    final String topic = "atopic";
    //String tableName1 = topic + "_" + SCHEMA.name().substring(SCHEMA.name().lastIndexOf(".")+1).toLowerCase();
    //String tableName2 = topic + "_" + "person_address";
    String tableName1 = "p";
    String tableName2 = "pa";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, SCHEMA, struct, 42)
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
                          assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
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
                        assertEquals(rs.getInt(1), 2);
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
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    task.stop();
    task.start(props);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, null, null, 43)
    ));
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
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
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
  }



  @Test
  public void putLargerStructureDelete() throws Exception {

    final Schema RECORDTHREE = SchemaBuilder.struct().name("com.example.Record3").version(1)
            .field("string", Schema.STRING_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();
    final Schema RECORDFOUR = SchemaBuilder.struct().name("com.example.Record4").version(1)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("modified", Timestamp.SCHEMA)
            .build();
    final Schema ARRAYTHREE = SchemaBuilder.array(RECORDFOUR).optional().defaultValue(null)
            .build();
    final Schema RECORDTWO = SchemaBuilder.struct().name("com.example.Record2").version(1)
            .field("string", Schema.STRING_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("array3", ARRAYTHREE)
            .build();
    final Schema ARRAYONE = SchemaBuilder.array(Schema.INT32_SCHEMA).optional().defaultValue(null)
            .build();
    final Schema ARRAYTWO = SchemaBuilder.array(RECORDTHREE).optional().defaultValue(null)
            .build();
    final Schema MAINRECORD = SchemaBuilder.struct().name("com.example.Mainrecord").version(1)
            .field("string1", Schema.STRING_SCHEMA)
            .field("string2", Schema.STRING_SCHEMA)
            .field("array1", ARRAYONE)
            .field("array2", ARRAYTWO)
            .field("record2", RECORDTWO)
            .build();
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
    props.put("pk.fields", "personkey.keyint, mainrecord.array1.array1");
    props.put("delete.enabled", "true");
    props.put("flatten.rename_tables", "biggerstruct_mainrecord:biggerstruct_mr, biggerstruct_mainrecord_array1:biggerstruct_array1" +
            ",biggerstruct_mainrecord_array2:biggerstruct_array2,biggerstruct_mainrecord_record2_array3:biggerstruct_array3");

    final Struct keyStruct1 = new Struct(KEYSCHEMA)
            .put("keyInt", 13)
            .put("keyName", "KeyString1");

    final String topic = "biggerstruct";
    String tableName1 = "biggerstruct_mr";
    String tableName2 = "biggerstruct_array1";
    String tableName3 = "biggerstruct_array2";
    String tableName4 = "biggerstruct_array3";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    tablesUsed.add(tableName3);
    tablesUsed.add(tableName4);
    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, MAINRECORD, mainrecord, 44)
    ));
    task.stop();
    task.start(props);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, KEYSCHEMA, keyStruct1, null, null, 45)
    ));
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
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
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName3 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName4 + "\"",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );

  }

  private static final Schema CARS1RECORD = SchemaBuilder.struct()
          .field("n1", Schema.STRING_SCHEMA)
          .field("n2", Schema.STRING_SCHEMA)
          .field("n3", Schema.STRING_SCHEMA)
          .build();

  private static final Schema CARS2RECORD = SchemaBuilder.struct()
          .field("p1", Schema.STRING_SCHEMA)
          .field("p2", Schema.STRING_SCHEMA)
          .field("p3", Schema.STRING_SCHEMA)
          .build();

  private static final Schema CARS1ARRAY = SchemaBuilder.array(CARS1RECORD).name("col1")
          .build();

  private static final Schema CARS2ARRAY = SchemaBuilder.array(CARS2RECORD).name("col2")
          .build();

  private static final Schema JDBC_CAR_RECORD = SchemaBuilder.struct()
          .field("name", Schema.STRING_SCHEMA)
          .field("age", Schema.STRING_SCHEMA)
          .field("cars1", CARS1ARRAY)
          .field("cars2", CARS2ARRAY)
          .build();

  private static final Schema JDBC_CAR_RECORD_KEY = SchemaBuilder.struct().name("CarRecordKey").version(1)
          .field("keyInt", Schema.INT32_SCHEMA)
          .build();

  @Test
  public void putJDBCRecordCarsDelete() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("flatten", "true");
    props.put("pk.mode", "flatten");
    props.put("flatten.coordinates", "true");
    props.put("flatten.uppercase", "true");
    props.put("pk.fields", "CarRecordKey.keyInt");
    props.put("insert.mode", "upsert");
    props.put("flatten.pk_propagate_value_fields", "ROOT.age, ROOT.name,ROOT.CARS1.CARS1.n1,ROOT.CARS2.CARS2.p1");
    props.put("flatten.rename_tables", "TestTopic_ROOT:Table01ROOT,TestTopic_ROOT_CARS1:Table02CAR1,TestTopic_ROOT_CARS2:Table03CAR2");
    props.put("delete.enabled", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> cars1arrayFirst = new ArrayList<>();
    final List<Struct> cars2arrayFirst = new ArrayList<>();
    final Struct cars1recordFirst = new Struct(CARS1RECORD)
            .put("n1", "Ford")
            .put("n2", "BMW")
            .put("n3", "Audi");
    final Struct cars2recordFirst = new Struct(CARS2RECORD)
            .put("p1", "Toyota")
            .put("p2", "Renault")
            .put("p3", "Marutti");
    cars1arrayFirst.add(cars1recordFirst);
    cars2arrayFirst.add(cars2recordFirst);
    final Struct jdbcCarRecordFirst = new Struct(JDBC_CAR_RECORD)
            .put("name", "John")
            .put("age", "30")
            .put("cars1", cars1arrayFirst )
            .put("cars2", cars2arrayFirst )
            ;
    final Struct jdbcCarRecordKeyFirst = new Struct(JDBC_CAR_RECORD_KEY)
            .put("keyInt", 30);

    final List<Struct> cars1arraySecond = new ArrayList<>();
    final List<Struct> cars2arraySecond = new ArrayList<>();
    final Struct cars1recordSecond = new Struct(CARS1RECORD)
            .put("n1", "Volvo")
            .put("n2", "Ferrari")
            .put("n3", "Mitsubishi");
    final Struct cars2recordSecond = new Struct(CARS2RECORD)
            .put("p1", "Mercedes")
            .put("p2", "Peugeot")
            .put("p3", "Tesla");
    cars1arraySecond.add(cars1recordSecond);
    cars2arraySecond.add(cars2recordSecond);
    final Struct jdbcCarRecordSecond = new Struct(JDBC_CAR_RECORD)
            .put("name", "Donald")
            .put("age", "65")
            .put("cars1", cars1arraySecond )
            .put("cars2", cars2arraySecond )
            ;
    final Struct jdbcCarRecordKeySecond = new Struct(JDBC_CAR_RECORD_KEY)
            .put("keyInt", 65);

    final Struct jdbcCarNullValueFirst = new Struct(JDBC_CAR_RECORD_KEY)
            .put("keyInt", 30);
    final Struct jdbcCarNullValueSecond = new Struct(JDBC_CAR_RECORD_KEY)
            .put("keyInt", 65);


    final String topic = "TestTopic";
    String tableName1 = "TABLE01ROOT";
    String tableName2 = "TABLE02CAR1";
    String tableName3 = "TABLE03CAR2";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    tablesUsed.add(tableName3);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, JDBC_CAR_RECORD_KEY, jdbcCarRecordKeyFirst, JDBC_CAR_RECORD, jdbcCarRecordFirst, 1)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, JDBC_CAR_RECORD_KEY, jdbcCarRecordKeySecond, JDBC_CAR_RECORD, jdbcCarRecordSecond, 2)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 30",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 65",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 30",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 65",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, JDBC_CAR_RECORD_KEY, jdbcCarNullValueFirst, null, null, 3)
    ));
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 30",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 65",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 30",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"CARRECORDKEY_KEYINT\" = 65",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
  }

  @Test
  public void putJDBCRecordCarsDeletePrimitiveKey() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", postgresHelper.postgreSQL());
    props.put("connection.user", "postgres");
    props.put("connection.password", "password123");
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("flatten", "true");
    props.put("pk.mode", "flatten");
    props.put("flatten.coordinates", "true");
    props.put("flatten.uppercase", "true");
    props.put("pk.fields", "root.key");
    props.put("insert.mode", "upsert");
    props.put("flatten.pk_propagate_value_fields", "ROOT.age, ROOT.name,ROOT.CARS1.CARS1.n1,ROOT.CARS2.CARS2.p1");
    props.put("flatten.rename_tables", "TestTopic_ROOT:Table01ROOT,TestTopic_ROOT_CARS1:Table02CAR1,TestTopic_ROOT_CARS2:Table03CAR2");
    props.put("delete.enabled", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    final List<Struct> cars1arrayFirst = new ArrayList<>();
    final List<Struct> cars2arrayFirst = new ArrayList<>();
    final Struct cars1recordFirst = new Struct(CARS1RECORD)
            .put("n1", "Ford")
            .put("n2", "BMW")
            .put("n3", "Audi");
    final Struct cars2recordFirst = new Struct(CARS2RECORD)
            .put("p1", "Toyota")
            .put("p2", "Renault")
            .put("p3", "Marutti");
    cars1arrayFirst.add(cars1recordFirst);
    cars2arrayFirst.add(cars2recordFirst);
    final Struct jdbcCarRecordFirst = new Struct(JDBC_CAR_RECORD)
            .put("name", "John")
            .put("age", "30")
            .put("cars1", cars1arrayFirst )
            .put("cars2", cars2arrayFirst )
            ;
    final String jdbcCarRecordKeyFirst = "30";

    final List<Struct> cars1arraySecond = new ArrayList<>();
    final List<Struct> cars2arraySecond = new ArrayList<>();
    final Struct cars1recordSecond = new Struct(CARS1RECORD)
            .put("n1", "Volvo")
            .put("n2", "Ferrari")
            .put("n3", "Mitsubishi");
    final Struct cars2recordSecond = new Struct(CARS2RECORD)
            .put("p1", "Mercedes")
            .put("p2", "Peugeot")
            .put("p3", "Tesla");
    cars1arraySecond.add(cars1recordSecond);
    cars2arraySecond.add(cars2recordSecond);
    final Struct jdbcCarRecordSecond = new Struct(JDBC_CAR_RECORD)
            .put("name", "Donald")
            .put("age", "65")
            .put("cars1", cars1arraySecond )
            .put("cars2", cars2arraySecond )
            ;
    final String jdbcCarRecordKeySecond = "65";

    final String jdbcCarNullValueFirst = "30";
    final String jdbcCarNullValueSecond = "65";


    final String topic = "TestTopic";
    String tableName1 = "TABLE01ROOT";
    String tableName2 = "TABLE02CAR1";
    String tableName3 = "TABLE03CAR2";
    tablesUsed.add(tableName1);
    tablesUsed.add(tableName2);
    tablesUsed.add(tableName3);
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, Schema.STRING_SCHEMA, jdbcCarRecordKeyFirst, JDBC_CAR_RECORD, jdbcCarRecordFirst, 1)
    ));
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, Schema.STRING_SCHEMA, jdbcCarRecordKeySecond, JDBC_CAR_RECORD, jdbcCarRecordSecond, 2)
    ));

    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"ROOT_KEY\" = '30'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"ROOT_KEY\" = '65'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '30'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '65'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    task.put(Collections.singleton(
            new SinkRecord(topic, 1, Schema.STRING_SCHEMA, jdbcCarNullValueFirst, null, null, 3)
    ));
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"ROOT_KEY\" = '30'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"ROOT_KEY\" = '65'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '30'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 0);
                      }
                    }
            )
    );
    assertEquals(
            1,
            postgresHelper.select(
                    "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '65'",
                    new PostgresHelper.ResultSetReadCallback() {
                      @Override
                      public void read(ResultSet rs) throws SQLException {
                        assertEquals(rs.getInt(1), 1);
                      }
                    }
            )
    );
  }

    @Test
    public void putJDBCRecordCarsDeleteWithUpsertPrimitiveKeyInsertAndDeleteInSameBatch() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", postgresHelper.postgreSQL());
        props.put("connection.user", "postgres");
        props.put("connection.password", "password123");
        props.put("auto.create", "true");
        props.put("auto.evolve", "true");
        props.put("flatten", "true");
        props.put("pk.mode", "flatten");
        props.put("flatten.coordinates", "true");
        props.put("flatten.uppercase", "true");
        props.put("pk.fields", "root.key");
        props.put("insert.mode", "upsert");
        props.put("flatten.pk_propagate_value_fields", "ROOT.age, ROOT.name,ROOT.CARS1.CARS1.n1,ROOT.CARS2.CARS2.p1");
        props.put("flatten.rename_tables", "TestTopic_ROOT:Table01ROOT,TestTopic_ROOT_CARS1:Table02CAR1,TestTopic_ROOT_CARS2:Table03CAR2");
        props.put("delete.enabled", "true");

        JdbcSinkTask task = new JdbcSinkTask();
        task.initialize(mock(SinkTaskContext.class));
        task.start(props);

        final List<Struct> cars1arrayFirst = new ArrayList<>();
        final List<Struct> cars2arrayFirst = new ArrayList<>();
        final Struct cars1recordFirst = new Struct(CARS1RECORD)
                .put("n1", "Ford")
                .put("n2", "BMW")
                .put("n3", "Audi");
        final Struct cars2recordFirst = new Struct(CARS2RECORD)
                .put("p1", "Toyota")
                .put("p2", "Renault")
                .put("p3", "Marutti");
        cars1arrayFirst.add(cars1recordFirst);
        cars2arrayFirst.add(cars2recordFirst);
        final Struct jdbcCarRecordFirst = new Struct(JDBC_CAR_RECORD)
                .put("name", "Johny")
                .put("age", "30")
                .put("cars1", cars1arrayFirst )
                .put("cars2", cars2arrayFirst )
                ;
        final String jdbcCarRecordKeyFirst = "13";
        final String jdbcCarNullValueFirst = "13";

        final String topic = "TestTopic";
        String tableName1 = "TABLE01ROOT";
        String tableName2 = "TABLE02CAR1";
        String tableName3 = "TABLE03CAR2";
        tablesUsed.add(tableName1);
        tablesUsed.add(tableName2);
        tablesUsed.add(tableName3);
        task.put(Arrays.asList(
                new SinkRecord(topic, 1, Schema.STRING_SCHEMA, jdbcCarRecordKeyFirst, JDBC_CAR_RECORD, jdbcCarRecordFirst, 1),
                new SinkRecord(topic, 1, Schema.STRING_SCHEMA, jdbcCarNullValueFirst, null, null, 2)
                ));

        assertEquals(
                1,
                postgresHelper.select(
                        "SELECT COUNT(*) FROM " + "\"" + tableName1 + "\" WHERE \"ROOT_KEY\" = '13'",
                        new PostgresHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(rs.getInt(1), 0);
                            }
                        }
                )
        );
        assertEquals(
                1,
                postgresHelper.select(
                        "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '13'",
                        new PostgresHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(rs.getInt(1), 0);
                            }
                        }
                )
        );
        assertEquals(
                1,
                postgresHelper.select(
                        "SELECT COUNT(*) FROM " + "\"" + tableName2 + "\" WHERE \"ROOT_KEY\" = '13'",
                        new PostgresHelper.ResultSetReadCallback() {
                            @Override
                            public void read(ResultSet rs) throws SQLException {
                                assertEquals(rs.getInt(1), 0);
                            }
                        }
                )
        );
    }

}