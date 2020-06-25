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

import io.confluent.connect.jdbc.sink.StreamFlatten.FlattenTransformation;
import io.confluent.connect.jdbc.sink.StreamFlatten.KeyCoordinate;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;
  private final FlattenTransformation flattenTransformation;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.flattenTransformation = new FlattenTransformation(config);
    this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();
    final ConcurrentMap<TableId, BufferedRecords> bufferByTable = new ConcurrentHashMap<>();
    for (SinkRecord record : records) {
      //FLATTEN:
      //If the flatten config property is true
      if (config.flatten) {
        log.debug("JdbcWriter.write Flatten enabled: {}", config.flatten);
        log.debug("JdbcWriter.write Value schema of record: {}", record.valueSchema());
        log.debug("JdbcWriter.write Value of record: {}", record.value());
        //Apply the transform function of FlattenTransformation to the record
        //and store each flattened record to its respective buffer
        try {
          flattenTransformation.transform(record)
            .forEach(fr -> {
            log.debug("JdbcWriter.write sink record after flattening: " + fr);
            //Not a delete
            if (fr.valueSchema() != null) {
              //get the TableId object for the given fullTableName
              final TableId renamedTableId;
              final TableId substructTableId;
              //As the schema name of flatten record gets set to the full path within the nested structure, combined with the topic up front this allows different buffers for each
              //flattened record schema. So for example one for each array contained in the structure.
              //This also allows for multiple schemas on a single topic without flushing on each subsequent connect record being a different schema on the same topic
              //e.g. support for subject name strategy
              String fullTableName = fr.topic().toLowerCase() + config.flattenDelimiter +
                      fr.valueSchema().name().replaceAll("\\.", config.flattenDelimiter).toLowerCase();
              //Replace dots that are used throughout FlattenTransformation to define paths within nested structures with configDelimiter to be database-friendly
              substructTableId = destinationTable(config.flattenUppercase ? fullTableName.toUpperCase() : fullTableName.toLowerCase());
              //if flatten.rename_tables contains table names to replace with a different
              //name, they get renamed to these names
              boolean renameMatch = false;
              if (config.flattentablenamesrenames.size() > 0) {
                log.debug("Table rename list {}", config.flattentablenamesrenames.toString());
                log.debug("Full table name {}", fullTableName);
                if (config.flattentablenamesrenames.containsKey(fullTableName)) {
                  renameMatch = true;
                }
                else {
                  log.debug("No table rename match found");
                }
              }
              if (renameMatch) {
                renamedTableId =  destinationTable(config.flattenUppercase ? config.flattentablenamesrenames.get(fullTableName).toUpperCase() :
                        config.flattentablenamesrenames.get(fullTableName).toLowerCase());
              }
              else {
                renamedTableId = substructTableId;
              }
              //Manage the BufferedRecords of the flattened record by its fullname as opposed to the renamed name.
              //This allows to identify table rename changes and the ability to map delete requests i.e. null values to the correct buffer
              BufferedRecords buffer = bufferByTable.get(substructTableId);
              //if no buffer exists yet for given target table a new BufferedRecords object
              //gets created
              if (buffer == null) {
                //Assign the renamedTableId to the BufferedRecords as this will eventually become the table name as opposed to the fullname
                buffer = new BufferedRecords(config, renamedTableId, dbDialect,
                        dbStructure, connection);
                //Manage the BufferedRecords of the flattened record by its fullname as opposed to the renamed name
                //This allows to identify table rename changes and the ability to map delete requests i.e. null values to the correct buffer
                bufferByTable.put(substructTableId, buffer);
              }
              //add the flattened record to the buffer
              try {
                buffer.add(fr);
              }
              catch (SQLException e) {
                log.error("Exception while adding record to BufferedRecords for table {}", substructTableId , e);
                throw new RuntimeException(e);
              }
            } else {
              //A delete
              //get the TableId object for the given fullTableName
                if (fr.valueSchema() == null && config.deleteEnabled  && config.pkMode == JdbcSinkConfig.PrimaryKeyMode.FLATTEN && !fr.headers().isEmpty()) {
                  //Use the topic name to find correct buffer
                  String tableNameStartsWith = config.flattenUppercase ? fr.topic().toUpperCase() : fr.topic().toLowerCase();
                  //If there is no buffer for tableId that matches the topic name
                  if (bufferByTable.keySet().stream().map(TableId::tableName).noneMatch(tn -> tn.toUpperCase().startsWith(tableNameStartsWith.toUpperCase()))) {
                    List<Pair<String, TableId>> tableIdList;
                    List<Pair<String, TableId>> renamedTableIdList = new ArrayList<>();
                    //Lookup in the database any tables that match with the topic name
                    try {
                      tableIdList = dbStructure.getTableDefns().searchTableId(connection, record.topic())
                              .stream().map(tableId -> Pair.with(config.flattenUppercase ? tableId.tableName().toUpperCase() : tableId.tableName().toLowerCase()
                                      , tableId)).collect(Collectors.toList());
                      config.flattentablenamesrenames.forEach((key, value) -> {
                        try {
                          String ucaseValue = config.flattenUppercase ? value.toUpperCase() : value.toLowerCase();
                          dbDialect.tableIds(connection).stream().filter(tableID -> tableID.tableName().equals(ucaseValue))
                                  .forEach(tableId -> renamedTableIdList.add(Pair.with(config.flattenUppercase ? key.replaceAll("\\.", config.flattenDelimiter).toUpperCase() :
                                          key.replaceAll("\\.", config.flattenDelimiter).toLowerCase(), tableId)));
                        } catch (SQLException e) {
                          log.error("Exception while using DBDialect to get existing tables", e);
                        }
                      });
                      log.debug("JdbcDbWriter.write renamedTableIdList: {}", renamedTableIdList);
                      renamedTableIdList.stream().filter(rTib -> !tableIdList.contains(rTib)).forEach(tableIdList::add);
                    } catch (java.net.ConnectException | SQLException e) {
                      log.error("Exception while using DBDialect to get existing tables for topic {}",record.topic(), e);
                      throw new RuntimeException(e);
                    }
                    //Impossible to construct DDL from null value
                    if (tableIdList.isEmpty()) {throw new ConnectException("Received null value for record key " + fr.key()
                            + " but this table schema has not yet been cached neither could it be found in Database Metadata");}
                      //If tableIdList is not empty, there exists a buffer and/or matching tables exist in the target db
                      tableIdList.forEach(ctid -> {
                        //For each mathing tableId found
                        //Create a buffer for each mathing table
                      BufferedRecords buffer = new BufferedRecords(config, destinationTable(ctid.getValue1().tableName()), dbDialect, dbStructure, connection);
                        try {
                          //Add the null value to each buffer to assure delete from all flattened tables
                          buffer.add(fr);
                        } catch (SQLException e) {
                          e.printStackTrace();
                          throw new RuntimeException(e);
                        }
                        bufferByTable.put(destinationTable(ctid.getValue0()), buffer);
                    });
                  }
                  //A null value was received for a but there are already matching buffers in place
                  else {
                    bufferByTable.keySet().stream().filter(tableId -> tableId.tableName().toUpperCase().startsWith(tableNameStartsWith.toUpperCase()))
                            .forEach(tableId -> {
                              try {
                                //Add the null value to each buffer to assure delete from all flattened tables
                                bufferByTable.get(tableId).add(fr);
                              } catch (SQLException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                              }

                            });
                  }
                }
                else {
                  throw new ConnectException("Record value schema is null, this requires " + JdbcSinkConfig.DELETE_ENABLED
                          + " and " + JdbcSinkConfig.PK_MODE + " = " + JdbcSinkConfig.PrimaryKeyMode.FLATTEN
                  + " and fields from the record key to be configured in " + JdbcSinkConfig.PK_FIELDS);
                }
            }
          });
        } catch(RuntimeException | ExecutionException e) {
          e.printStackTrace();
          throw new ConnectException(e.getCause());
        }
      }
      //Else, flatten is not enabled record gets processed without flattening
      else {
        final TableId tableId = destinationTable(record.topic());
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }
        buffer.add(record);
      }
    }
    for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
      TableId tableId = entry.getKey();
      BufferedRecords buffer = entry.getValue();
      log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
      buffer.flush();
      buffer.close();
    }
    connection.commit();
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
    flattenTransformation.clearInstructionCache();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
