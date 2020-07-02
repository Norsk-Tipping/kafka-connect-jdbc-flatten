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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode.UPSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private boolean deletesInBatch = false;
  private boolean upsertDeletesInBatch = false;
  private boolean updatesInBatch = false;
  private final HashSet<Object> keys;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
    this.keys = new HashSet<>();
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    //Tombstone record
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    }
    //Same value schema as for previous record
    else if (Objects.equals(valueSchema, record.valueSchema())) {
      //Tombstone record
      if ((deletesInBatch && config.deleteEnabled) ||
              (record.value() == null && config.flatten && config.insertMode == UPSERT && keys.contains(record.key()))) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    }
    //Different value schema as for previous record
    else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged /*|| updateStatementBinder == null*/) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());
      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
      //FLATTEN:
      if (config.flatten && config.pkMode == JdbcSinkConfig.PrimaryKeyMode.FLATTEN) {
        fieldsMetadata = FieldsMetadata.extract(
                tableId.tableName(),
                config.pkMode,
                schemaPair,
                record.headers(),
                config.deleteEnabled,
                config.insertMode
        );
      }
      else {
        fieldsMetadata = FieldsMetadata.extract(
                tableId.tableName(),
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                schemaPair
        );
      }

      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();

      log.debug(
          "{} sql: {} deleteSql: {} meta: {}",
          config.insertMode,
          insertSql,
          deleteSql,
          fieldsMetadata
      );
      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      //FLATTEN:
      if (config.flatten && config.insertMode == UPSERT) {
        updateStatementBinder = dbDialect.statementBinder(
                updatePreparedStatement,
                config.pkMode,
                schemaPair,
                fieldsMetadata,
                INSERT
        );
      }
      else {
        updateStatementBinder = dbDialect.statementBinder(
                updatePreparedStatement,
                config.pkMode,
                schemaPair,
                fieldsMetadata,
                config.insertMode
        );
      }
      //FLATTEN:
      if (nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata,
            config.insertMode
        );
      }
    }
    records.add(record);
    if (record.value() != null) { updatesInBatch = true; }
    else {
      if (record.valueSchema() != null && config.flatten && config.insertMode == UPSERT) {
        upsertDeletesInBatch = true;
        keys.add(record.key());
      }
    }

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public synchronized List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records for table {}", records.size(), tableId.tableName());
    for (SinkRecord record : records) {
      if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
        deleteStatementBinder.bindRecord(record);
      } else {
        updateStatementBinder.bindRecord(record);
      }
    }
    Optional<Long> totalUpdateCount = executeUpdates();
    long totalDeleteCount = executeDeletes();

    final long expectedCount = updateRecordCount();
    log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
        config.insertMode, records.size(), totalUpdateCount, totalDeleteCount
    );
    if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
        && config.insertMode == INSERT) {
      throw new ConnectException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          totalUpdateCount.get(),
          expectedCount
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
          "{} records:{} for table {}, but no count of the number of rows it affected is available",
          config.insertMode,
          records.size(),
          tableId.tableName()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    updatesInBatch = false;
    upsertDeletesInBatch = false;
    keys.clear();
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();
    try {
      if (config.flatten && config.insertMode == UPSERT) {
        if (upsertDeletesInBatch || deletesInBatch) {
          deletesInBatch = true;
          executeDeletes();
          deletesInBatch = false;
        }
      }
      if (updatesInBatch) {
        for (int updateCount : updatePreparedStatement.executeBatch()) {
          if (updateCount != Statement.SUCCESS_NO_INFO) {
            count = count.isPresent()
                    ? count.map(total -> total + updateCount)
                    : Optional.of((long) updateCount);
          }
        }
      }
    } catch (SQLException sqlException) {
      log.error("SQLException for table {}, deletesInBatch {}, updatesInBatch {}, deletePreparedStatement {}, updatePreparedStatement {}",
              tableId.tableName(), deletesInBatch, updatesInBatch, deletePreparedStatement, updatePreparedStatement);
      throw sqlException;
    }
    return count;
  }

  private long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement) && deletesInBatch) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  private long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                  + " primary key configuration",
              tableId
          ));
        }
        try {
          if (config.flatten) {
            return dbDialect.buildInsertStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
            );
          }
          else {
            return dbDialect.buildInsertStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
            );
          }
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled || (config.insertMode == UPSERT && config.flatten)) {
      switch (config.pkMode) {
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(
                tableId,
                asColumns(fieldsMetadata.keyFieldNames)
            );
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                "Deletes to table '%s' are not supported with the %s dialect.",
                tableId,
                dbDialect.name()
            ));
          }
          break;
        //FLATTEN:
        case FLATTEN:
          if (fieldsMetadata.keyFieldNames.isEmpty() || fieldsMetadata.keyFieldNamesInKey == null || fieldsMetadata.keyFieldNamesInKey.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNamesInKey)
            );
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                    "Deletes to table '%s' are not supported with the %s dialect.",
                    tableId,
                    dbDialect.name()
            ));
          }
          break;

        default:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }
}
