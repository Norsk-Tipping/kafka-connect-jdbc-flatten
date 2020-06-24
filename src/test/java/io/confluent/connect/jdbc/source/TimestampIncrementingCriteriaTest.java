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

package io.confluent.connect.jdbc.source;

import java.time.ZoneOffset;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class TimestampIncrementingCriteriaTest {

  private static final TableId TABLE_ID = new TableId(null, null,"myTable");
  private static final ColumnId INCREMENTING_COLUMN = new ColumnId(TABLE_ID, "id");
  private static final ColumnId TS1_COLUMN = new ColumnId(TABLE_ID, "ts1");
  private static final ColumnId TS2_COLUMN = new ColumnId(TABLE_ID, "ts2");
  private static final List<ColumnId> TS_COLUMNS = Arrays.asList(TS1_COLUMN, TS2_COLUMN);

  private IdentifierRules rules;
  private QuoteMethod identifierQuoting;
  private ExpressionBuilder builder;
  private TimestampIncrementingCriteria criteria;
  private TimestampIncrementingCriteria criteriaInc;
  private TimestampIncrementingCriteria criteriaTs;
  private TimestampIncrementingCriteria criteriaIncTs;
  private Schema schema;
  private Struct record;
  private TimeZone utcTimeZone = TimeZone.getTimeZone(ZoneOffset.UTC);

  @Before
  public void beforeEach() {
    criteria = new TimestampIncrementingCriteria(null, null, utcTimeZone);
    criteriaInc = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, null, utcTimeZone);
    criteriaTs = new TimestampIncrementingCriteria(null, TS_COLUMNS, utcTimeZone);
    criteriaIncTs = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, TS_COLUMNS, utcTimeZone);
    identifierQuoting = null;
    rules = null;
    builder = null;
  }

  protected void assertExtractedOffset(long expected, Schema schema, Struct record) {
    TimestampIncrementingCriteria criteria = null;
    if (schema.field(INCREMENTING_COLUMN.name()) != null) {
      if (schema.field(TS1_COLUMN.name()) != null) {
        criteria = criteriaIncTs;
      } else {
        criteria = criteriaInc;
      }
    } else if (schema.field(TS1_COLUMN.name()) != null) {
      criteria = criteriaTs;
    } else {
      criteria = this.criteria;
    }
    TimestampIncrementingOffset offset = criteria.extractValues(schema, record, null);
    assertEquals(expected, offset.getIncrementingOffset());
  }

  @Test
  public void extractIntOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractLongOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    record = new Struct(schema).put("id", 42L);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(42));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal("42.42"));
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractWithIncColumn() throws SQLException {
    schema = SchemaBuilder.struct()
                          .field("id", SchemaBuilder.INT32_SCHEMA)
                          .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
                          .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
                          .build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = DataException.class)
  public void extractWithIncColumnNotExisting() throws Exception {
    schema = SchemaBuilder.struct()
            .field("real-id", SchemaBuilder.INT32_SCHEMA)
            .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
            .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
            .build();
    record = new Struct(schema).put("real-id", 42);
    criteriaIncTs.extractValues(schema, record, null);
  }

  @Test
  public void createIncrementingWhereClause() {
    builder = builder();
    criteriaInc.incrementingWhereClause(builder);
    assertEquals(
        " WHERE \"myTable\".\"id\" > ? ORDER BY \"myTable\".\"id\" ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaInc.incrementingWhereClause(builder);
    assertEquals(
        " WHERE myTable.id > ? ORDER BY myTable.id ASC",
        builder.toString()
    );
  }

  @Test
  public void createTimestampWhereClause() {
    builder = builder();
    criteriaTs.timestampWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") > ? "
        + "AND "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") < ? "
        + "ORDER BY "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") "
        + "ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaTs.timestampWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(myTable.ts1,myTable.ts2) > ? "
        + "AND "
        + "COALESCE(myTable.ts1,myTable.ts2) < ? "
        + "ORDER BY "
        + "COALESCE(myTable.ts1,myTable.ts2) "
        + "ASC",
        builder.toString()
    );
  }

  @Test
  public void createTimestampIncrementingWhereClause() {
    builder = builder();
    criteriaIncTs.timestampIncrementingWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") < ? "
        + "AND ("
        + "(COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") = ? AND \"myTable\".\"id\" > ?) "
        + "OR "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") > ?) "
        + "ORDER BY COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\"),"
        + "\"myTable\".\"id\" ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaIncTs.timestampIncrementingWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(myTable.ts1,myTable.ts2) < ? "
        + "AND ("
        + "(COALESCE(myTable.ts1,myTable.ts2) = ? AND myTable.id > ?) "
        + "OR "
        + "COALESCE(myTable.ts1,myTable.ts2) > ?) "
        + "ORDER BY COALESCE(myTable.ts1,myTable.ts2),"
        + "myTable.id ASC",
        builder.toString()
    );
  }

  protected ExpressionBuilder builder() {
    ExpressionBuilder result = new ExpressionBuilder(rules);
    result.setQuoteIdentifiers(identifierQuoting);
    return result;
  }
}