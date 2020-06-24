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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;

import io.confluent.connect.jdbc.util.ConfigUtils;
import io.confluent.connect.jdbc.util.DatabaseDialectRecommender;
import io.confluent.connect.jdbc.util.DeleteEnabledRecommender;
import io.confluent.connect.jdbc.util.EnumRecommender;
import io.confluent.connect.jdbc.util.PrimaryKeyModeRecommender;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.StringUtils;
import io.confluent.connect.jdbc.util.TableType;
import io.confluent.connect.jdbc.util.TimeZoneValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

public class JdbcSinkConfig extends AbstractConfig {

  public enum InsertMode {
    INSERT,
    UPSERT,
    UPDATE;

  }

  public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE,
    FLATTEN;
  }

  public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
      Arrays.asList(
          "__connect_topic",
          "__connect_partition",
          "__connect_offset"
      )
  );

  public static final String CONNECTION_URL = JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
  private static final String CONNECTION_URL_DOC =
      "JDBC connection URL.\n"
          + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
          + "``jdbc:mysql://localhost/db_name``, "
          + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
          + "databaseName=db_name``";
  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER = JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
  private static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD =
      JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String TABLE_NAME_FORMAT = "table.name.format";
  private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
  private static final String TABLE_NAME_FORMAT_DOC =
      "A format string for the destination table name, which may contain '${topic}' as a "
      + "placeholder for the originating topic name.\n"
      + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name "
      + "'kafka_orders'.";
  private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the task.";
  private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion into the destination"
      + " table, when possible.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String DELETE_ENABLED = "delete.enabled";
  private static final String DELETE_ENABLED_DEFAULT = "false";
  private static final String DELETE_ENABLED_DOC =
      "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
      + "to be ``record_key``.";
  private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create the destination table based on record schema if it is "
      + "found to be missing by issuing ``CREATE``.";
  private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically add columns in the table schema when found to be missing relative "
      + "to the record schema by issuing ``ALTER``.";
  private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

  public static final String INSERT_MODE = "insert.mode";
  private static final String INSERT_MODE_DEFAULT = "insert";
  private static final String INSERT_MODE_DOC =
      "The insertion mode to use. Supported modes are:\n"
      + "``insert``\n"
      + "    Use standard SQL ``INSERT`` statements.\n"
      + "``upsert``\n"
      + "    Use the appropriate upsert semantics for the target database if it is supported by "
      + "the connector, e.g. ``INSERT OR IGNORE``.\n"
      + "``update``\n"
      + "    Use the appropriate update semantics for the target database if it is supported by "
      + "the connector, e.g. ``UPDATE``.";
  private static final String INSERT_MODE_DISPLAY = "Insert Mode";

  public static final String PK_FIELDS = "pk.fields";
  private static final String PK_FIELDS_DEFAULT = "";
  private static final String PK_FIELDS_DOC =
      "List of comma-separated primary key field names. The runtime interpretation of this config"
      + " depends on the ``pk.mode``:\n"
      + "``none``\n"
      + "    Ignored as no fields are used as primary key in this mode.\n"
      + "``kafka``\n"
      + "    Must be a trio representing the Kafka coordinates, defaults to ``"
      + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
      + "``record_key``\n"
      + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
      + " desired fields - for primitive key only a single field name must be configured.\n"
      + "``record_value``\n"
      + "    If empty, all fields from the value struct will be used, otherwise used to extract "
      + "the desired fields.";
  private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

  public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
      + "Supported modes are:\n"
      + "``none``\n"
      + "    No keys utilized.\n"
      + "``kafka``\n"
      + "    Kafka coordinates are used as the PK.\n"
      + "``record_key``\n"
      + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
      + "``record_value``\n"
      + "    Field(s) from the record value are used, which must be a struct."
      + "``flatten``\n"
      + "    Field(s) from the record value are used, which propagate down in an hierarchical structure.\n"
      + "    Field paths are specified in flatten.pk_propagate_value_fields.";
  private static final String PK_MODE_DISPLAY = "Primary Key Mode";

  public static final String FIELDS_WHITELIST = "fields.whitelist";
  private static final String FIELDS_WHITELIST_DEFAULT = "";
  private static final String FIELDS_WHITELIST_DOC =
      "List of comma-separated record value field names. If empty, all fields from the record "
      + "value are utilized, otherwise used to filter to the desired fields.\n"
      + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
      + "(s) form the primary key columns in the destination database,"
      + " while this configuration is applicable for the other columns.";
  private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  private static final String CONNECTION_GROUP = "Connection";
  private static final String WRITES_GROUP = "Writes";
  private static final String DATAMAPPING_GROUP = "Data Mapping";
  private static final String DDL_GROUP = "DDL Support";
  private static final String RETRIES_GROUP = "Retries";

  private static final String TRANSFORMATION_GROUP = "Transformation Support";

  public static final String DIALECT_NAME_CONFIG = "dialect.name";
  private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
  public static final String DIALECT_NAME_DEFAULT = "";
  private static final String DIALECT_NAME_DOC =
      "The name of the database dialect that should be used for this connector. By default this "
      + "is empty, and the connector automatically determines the dialect based upon the "
      + "JDBC connection URL. Use this if you want to override that behavior and use a "
      + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
      + "can be used.";

  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
  public static final String DB_TIMEZONE_DEFAULT = "UTC";
  private static final String DB_TIMEZONE_CONFIG_DOC =
      "Name of the JDBC timezone that should be used in the connector when "
      + "inserting time-based values. Defaults to UTC.";
  private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";

  public static final String QUOTE_SQL_IDENTIFIERS_CONFIG =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG;
  public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DEFAULT;
  public static final String QUOTE_SQL_IDENTIFIERS_DOC =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DOC;
  private static final String QUOTE_SQL_IDENTIFIERS_DISPLAY =
      JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_DISPLAY;

  public static final String TABLE_TYPES_CONFIG = "table.types";
  private static final String TABLE_TYPES_DISPLAY = "Table Types";
  public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
  private static final String TABLE_TYPES_DOC =
      "The comma-separated types of database tables to which the sink connector can write. "
      + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
      + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
      + "support writing to views, and when they do the the sink connector will fail if the "
      + "view definition does not match the records' schemas (regardless of ``"
      + AUTO_EVOLVE + "``).";

  private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
      EnumRecommender.in(QuoteMethod.values());

  private static final EnumRecommender TABLE_TYPES_RECOMMENDER =
          EnumRecommender.in(TableType.values());

  //FLATTEN:
  //Added flag to indicate flattening
  public static final String FLATTEN = "flatten";
  private static final String FLATTEN_DEFAULT = "false";
  private static final String FLATTEN_DOC =
          "Whether to automatically flatten arrays and write arrays to seperate"
                  + "destination tables based on array value schema.";
  private static final String FLATTEN_DISPLAY = "Flatten";
  //FLATTEN:
  //Added flag to indicate coordinates
  public static final String COORDINATES = "flatten.coordinates";
  private static final String COORDINATES_DEFAULT = "false";
  private static final String COORDINATES_DOC =
          "Whether to store topic, partition, offset in target tables when"
                  + " flatten is enabled"
                  + "destination tables based on array value schema.";
  private static final String COORDINATES_DISPLAY = "Coordinates";
  //FLATTEN:
  //Added flattencoordinatenames to set the required name for timestamp. partition,
  //offset, topic
  public static final String FIELDS_COORDINATES = "flatten.coordinates.fields";
  private static final String FIELDS_COORDINATES_DEFAULT = "kafkatopic, kafkapartition, "
          + "kafkaoffset, kafkatimestamp, kafkatimestamp_type";
  private static final String FIELDS_COORDINATES_DOC =
          "List of comma-separated kafka metadata field names. If empty, "
                  + "the defaults: kafkatopic, kafkapartition, kafkaoffset, "
                  + "kafkatimestamp will be used. Note that coordinates must be set to true "
                  + "to propagate the record metadata to the sink.";
  private static final String FIELDS_COORDINATES_DISPLAY = "Fields coordinates";
  //FLATTEN:
  //Added fields that allow to rename flattened fields
  public static final String RENAME_FIELDS = "flatten.rename_fields";
  private static final String RENAME_FIELDS_DEFAULT = "";
  private static final String RENAME_FIELDS_DOC =
          "Whether to rename fields when flattening is enabled with flatten";
  private static final String RENAME_FIELDS_DISPLAY = "Rename fields";
  //FLATTEN:
  //Added fields that allow to rename table names
  public static final String TABLES_RENAME = "flatten.rename_tables";
  private static final String TABLES_RENAME_DEFAULT = "";
  private static final String TABLES_RENAME_DOC =
          "Whether to rename tables with enlisted mapping pairs <old name>:<new name>";
  private static final String TABLES_RENAME_DISPLAY = "Rename tables";
  //FLATTEN:
  //Added flag to indicate uppercase or lowercase
  public static final String FLATTEN_UPPERCASE = "flatten.uppercase";
  private static final String FLATTEN_UPPERCASE_DEFAULT = "false";
  private static final String FLATTEN_UPPERCASE_DOC =
          "Whether to automatically apply uppercase to columns when flatten is enabled."
                  + " When set to false lowercase will be applied.";
  private static final String FLATTEN_UPPERCASE_DISPLAY = "Uppercase flattened fields";
  //FLATTEN:
  //Added flag to indicate the delimiter for flattened record fields
  public static final String FLATTEN_DELIMITER = "flatten.delimiter";
  private static final String FLATTEN_DELIMITER_DEFAULT = "_";
  private static final String FLATTEN_DELIMITER_DOC =
          "Symbol used to concatenate flattened record fields and table names when flatten is enabled."
                  + " By default _ will be used.";
  private static final String FLATTEN_DELIMITER_DISPLAY = "Symbol to concatenate flattened fields";
  //FLATTEN:
  //Added flag to indicate the pathnames of primary key fields that need to propagate down the hierarchy when flatten is enabled
  public static final String FLATTEN_PK_PROPAGATE_VALUE_FIELDS = "flatten.pk_propagate_value_fields";
  private static final String FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DEFAULT = "";
  private static final String FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DOC =
          "List of comma-separated primary key field paths (suffix of root schema, followed by '.' seperated subpath of fieldnames).\n" +
          "Listed fields will propagate down to any flattened subcontainers lower in the hierarchy path as primary key";
  private static final String FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DISPLAY = "Fields that need to propagate down the hierarchy as primary keys when flatten is enabled";
  //FLATTEN:
  public static final String FLATTEN_WHITELIST_CONTAINERS = "flatten.containers.whitelist";
  private static final String FLATTEN_WHITELIST_CONTAINERS_DEFAULT = "";
  private static final String FLATTEN_WHITELIST_CONTAINERS_DOC =
          "List of comma-separated record container paths. If empty, all containers and subcontainers are "
                  + "utilized, otherwise used to filter to the desired containers within the hierarchical value structure.\n";
  //FLATTEN:
  private static final String FLATTEN_WHITELIST_CONTAINERS_DISPLAY = "Fields Whitelist within hierarchical structure";
  public static final String FLATTEN_INSTRUCTION_CACHE_SIZE = "flatten.instruction_cache_size";
  private static final String FLATTEN_INSTRUCTION_CACHE_SIZE_DEFAULT = "100";
  private static final String FLATTEN_INSTRUCTION_CACHE_SIZE_DOC =
          "Size of instruction cache needed for flattening Connect schema into subschemas. Cache maintains a mapping between \n" +
                  "Connect Schema and instructions to flatten these to subschemas and substructures";
  //FLATTEN:
  private static final String FLATTEN_INSTRUCTION_CACHE_SIZE_DISPLAY = "Cache size for flattening instructions per Connect Schema";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
        // Connection
        .define(
            CONNECTION_URL,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USER,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_USER_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            CONNECTION_PASSWORD_DISPLAY
        )
        .define(
            DIALECT_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DIALECT_NAME_DEFAULT,
            DatabaseDialectRecommender.INSTANCE,
            ConfigDef.Importance.LOW,
            DIALECT_NAME_DOC,
            CONNECTION_GROUP,
            4,
            ConfigDef.Width.LONG,
            DIALECT_NAME_DISPLAY,
            DatabaseDialectRecommender.INSTANCE
        )
        // Writes
        .define(
            INSERT_MODE,
            ConfigDef.Type.STRING,
            INSERT_MODE_DEFAULT,
            EnumValidator.in(InsertMode.values()),
            ConfigDef.Importance.HIGH,
            INSERT_MODE_DOC,
            WRITES_GROUP,
            1,
            ConfigDef.Width.MEDIUM,
            INSERT_MODE_DISPLAY
        )
        .define(
            BATCH_SIZE,
            ConfigDef.Type.INT,
            BATCH_SIZE_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            BATCH_SIZE_DOC, WRITES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            BATCH_SIZE_DISPLAY
        )
        .define(
            DELETE_ENABLED,
            ConfigDef.Type.BOOLEAN,
            DELETE_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DELETE_ENABLED_DOC, WRITES_GROUP,
            3,
            ConfigDef.Width.SHORT,
            DELETE_ENABLED_DISPLAY,
            DeleteEnabledRecommender.INSTANCE
        )
        .define(
            TABLE_TYPES_CONFIG,
            ConfigDef.Type.LIST,
            TABLE_TYPES_DEFAULT,
            TABLE_TYPES_RECOMMENDER,
            ConfigDef.Importance.LOW,
            TABLE_TYPES_DOC,
            WRITES_GROUP,
            4,
            ConfigDef.Width.MEDIUM,
            TABLE_TYPES_DISPLAY
        )
        // Data Mapping
        .define(
            TABLE_NAME_FORMAT,
            ConfigDef.Type.STRING,
            TABLE_NAME_FORMAT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TABLE_NAME_FORMAT_DOC,
            DATAMAPPING_GROUP,
            1,
            ConfigDef.Width.LONG,
            TABLE_NAME_FORMAT_DISPLAY
        )
        .define(
            PK_MODE,
            ConfigDef.Type.STRING,
            PK_MODE_DEFAULT,
            EnumValidator.in(PrimaryKeyMode.values()),
            ConfigDef.Importance.HIGH,
            PK_MODE_DOC,
            DATAMAPPING_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            PK_MODE_DISPLAY,
            PrimaryKeyModeRecommender.INSTANCE
        )
        .define(
            PK_FIELDS,
            ConfigDef.Type.LIST,
            PK_FIELDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            PK_FIELDS_DOC,
            DATAMAPPING_GROUP,
            3,
            ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
        )
        .define(
            FIELDS_WHITELIST,
            ConfigDef.Type.LIST,
            FIELDS_WHITELIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_WHITELIST_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG,
            FIELDS_WHITELIST_DISPLAY
        ).define(
          DB_TIMEZONE_CONFIG,
          ConfigDef.Type.STRING,
          DB_TIMEZONE_DEFAULT,
          TimeZoneValidator.INSTANCE,
          ConfigDef.Importance.MEDIUM,
          DB_TIMEZONE_CONFIG_DOC,
          DATAMAPPING_GROUP,
          5,
          ConfigDef.Width.MEDIUM,
          DB_TIMEZONE_CONFIG_DISPLAY
        )
        // DDL
        .define(
            AUTO_CREATE,
            ConfigDef.Type.BOOLEAN,
            AUTO_CREATE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_CREATE_DOC, DDL_GROUP,
            1,
            ConfigDef.Width.SHORT,
            AUTO_CREATE_DISPLAY
        )
        .define(
            AUTO_EVOLVE,
            ConfigDef.Type.BOOLEAN,
            AUTO_EVOLVE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_EVOLVE_DOC, DDL_GROUP,
            2,
            ConfigDef.Width.SHORT,
            AUTO_EVOLVE_DISPLAY
        ).define(
            QUOTE_SQL_IDENTIFIERS_CONFIG,
            ConfigDef.Type.STRING,
            QUOTE_SQL_IDENTIFIERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DOC,
            DDL_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DISPLAY,
            QUOTE_METHOD_RECOMMENDER
        )
        // Retries
        .define(
            MAX_RETRIES,
            ConfigDef.Type.INT,
            MAX_RETRIES_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            MAX_RETRIES_DOC,
            RETRIES_GROUP,
            1,
            ConfigDef.Width.SHORT,
            MAX_RETRIES_DISPLAY
        )
        .define(
            RETRY_BACKOFF_MS,
            ConfigDef.Type.INT,
            RETRY_BACKOFF_MS_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            RETRY_BACKOFF_MS_DOC,
            RETRIES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            RETRY_BACKOFF_MS_DISPLAY
        ) //FLATTEN://Added flag to indicate flattening of arrays
        .define(
            FLATTEN,
            ConfigDef.Type.BOOLEAN,
            FLATTEN_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_DOC, TRANSFORMATION_GROUP,
            1,
            ConfigDef.Width.SHORT,
            FLATTEN_DISPLAY,
            Arrays.asList(COORDINATES, FLATTEN_UPPERCASE)
        )
          //FLATTEN://Added flag to indicate kafka coordinates without use of pkmode Kafka
        .define(
            COORDINATES,
            ConfigDef.Type.BOOLEAN,
            COORDINATES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            COORDINATES_DOC, TRANSFORMATION_GROUP,
            2,
            ConfigDef.Width.SHORT,
            COORDINATES_DISPLAY,
            Arrays.asList(FIELDS_COORDINATES)
        )
          //FLATTEN://Added list to override kafka coordinates names that propagate to
        //sink when coordinates enables
        .define(
            FIELDS_COORDINATES,
            ConfigDef.Type.LIST,
            FIELDS_COORDINATES_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_COORDINATES_DOC,
            TRANSFORMATION_GROUP,
            3,
            ConfigDef.Width.LONG,
            FIELDS_COORDINATES_DISPLAY
        )
          //FLATTEN://Added flag to indicate uppercase or lowercase for table names and columns
        .define(
            FLATTEN_UPPERCASE,
            ConfigDef.Type.BOOLEAN,
            FLATTEN_UPPERCASE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_UPPERCASE_DOC, TRANSFORMATION_GROUP,
            4,
            ConfigDef.Width.SHORT,
            FLATTEN_UPPERCASE_DISPLAY
        )
          //FLATTEN://Added list of record fields that should be renamed after flattening records
        .define(RENAME_FIELDS,
            ConfigDef.Type.LIST,
                RENAME_FIELDS_DEFAULT,
            new ConfigDef.Validator() {
              @Override
              public void ensureValid(String name, Object value) {
                parseToMap(name, (List<String>) value);
              }

              @Override
              public String toString() {
                return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
              }
            },
            ConfigDef.Importance.MEDIUM,
                RENAME_FIELDS_DOC,
            TRANSFORMATION_GROUP,
            5,
            ConfigDef.Width.LONG,
                RENAME_FIELDS_DISPLAY)
          //FLATTEN://Added list of table names that should be renamed after flattening records
        .define(TABLES_RENAME,
            ConfigDef.Type.LIST,
            TABLES_RENAME_DEFAULT,
            new ConfigDef.Validator() {
              @Override
              public void ensureValid(String name, Object value) {
                parseToMap(name, (List<String>) value);
              }

              @Override
              public String toString() {
                return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
              }
            },
            ConfigDef.Importance.MEDIUM,
            TABLES_RENAME_DOC,
            TRANSFORMATION_GROUP,
            6,
            ConfigDef.Width.LONG,
            TABLES_RENAME_DISPLAY)
          //FLATTEN://Added flag to indicate uppercase or lowercase for table names and columns
          .define(
            FLATTEN_DELIMITER,
            ConfigDef.Type.STRING,
            FLATTEN_DELIMITER_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_DELIMITER_DOC, TRANSFORMATION_GROUP,
              7,
            ConfigDef.Width.SHORT,
            FLATTEN_DELIMITER_DISPLAY
          )
          //FLATTEN://Added list of fields from the value that need to propagate doen the hierarchy structure as primary keys
          .define(
            FLATTEN_PK_PROPAGATE_VALUE_FIELDS,
            ConfigDef.Type.LIST,
            FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DOC, TRANSFORMATION_GROUP,
            8,
            ConfigDef.Width.LONG,
            FLATTEN_PK_PROPAGATE_VALUE_FIELDS_DISPLAY
          )
          //FLATTEN://Added list of fields to whitelist fields within hierarchy structure
          .define(
            FLATTEN_WHITELIST_CONTAINERS,
            ConfigDef.Type.LIST,
            FLATTEN_WHITELIST_CONTAINERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_WHITELIST_CONTAINERS_DOC, TRANSFORMATION_GROUP,
            9,
            ConfigDef.Width.LONG,
            FLATTEN_WHITELIST_CONTAINERS_DISPLAY
          )
          .define(
            FLATTEN_INSTRUCTION_CACHE_SIZE,
            ConfigDef.Type.INT,
            FLATTEN_INSTRUCTION_CACHE_SIZE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FLATTEN_INSTRUCTION_CACHE_SIZE_DOC, TRANSFORMATION_GROUP,
            10,
            ConfigDef.Width.SHORT,
            FLATTEN_INSTRUCTION_CACHE_SIZE_DISPLAY
          );

  public final String connectorName;
  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final String tableNameFormat;
  public final int batchSize;
  public final boolean deleteEnabled;
  public final int maxRetries;
  public final int retryBackoffMs;
  public final boolean autoCreate;
  public final boolean autoEvolve;
  public final InsertMode insertMode;
  public final PrimaryKeyMode pkMode;
  public final List<String> pkFields;
  public final Set<String> fieldsWhitelist;
  public final String dialectName;
  public final TimeZone timeZone;
  public final EnumSet<TableType> tableTypes;
  //FLATTEN: flatten boolean
  public final boolean flatten;
  //FLATTEN: coordinates boolean
  public final boolean flattencoordinates;
  //FLATTEN: coordinate names
  public final List<String> flattencoordinatenames;
  //FLATTEN: uppercase boolean
  public final boolean flattenUppercase;
  //FLATTEN: record field rename mappings
  public final Map<String, String> flattenfieldrenames;
  //FLATTEN: table field rename mappings
  public final Map<String, String> flattentablenamesrenames;
  //FLATTEN: delimiter symbol
  public final String flattenDelimiter;
  //FLATTEN: pk field paths
  public final List<String> flattenPkValueFields;
  //FLATTEN: whitelisted container
  public final List<String> flattenWhitelistContainers;
  //FLATTEN: instruction cache size of the cache per schama
  public final int flattenInstructionCacheSize;

  public JdbcSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectorName = ConfigUtils.connectorName(props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
    tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
    batchSize = getInt(BATCH_SIZE);
    deleteEnabled = getBoolean(DELETE_ENABLED);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    autoCreate = getBoolean(AUTO_CREATE);
    autoEvolve = getBoolean(AUTO_EVOLVE);
    insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
    pkFields = getList(PK_FIELDS);
    dialectName = getString(DIALECT_NAME_CONFIG);
    fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
    timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
  //FLATTEN:
    if (deleteEnabled && pkMode != PrimaryKeyMode.RECORD_KEY && pkMode != PrimaryKeyMode.FLATTEN) {
      throw new ConfigException(
          "Primary key mode must be 'record_key' or 'flatten' when delete support is enabled");
    }
    tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));
    //FLATTEN:
    //flatten arrays boolean
    flatten = getBoolean(FLATTEN);
    //FLATTEN:
    if (flatten && !(insertMode == InsertMode.UPSERT || insertMode == InsertMode.INSERT)) {
      throw new ConfigException(
              "Flatten only supports insert modes upsert and insert");
    }
    //FLATTEN:
    //coordinates boolean
    flattencoordinates = getBoolean(COORDINATES);
    //FLATTEN:
    //coordinates names
    flattencoordinatenames = getList(FIELDS_COORDINATES).stream().map(String::toLowerCase).collect(Collectors.toList());
    //FLATTEN:
    //uppercase or lowercase for columns (flattened fields) and table names boolean
    flattenUppercase = getBoolean(FLATTEN_UPPERCASE);
    //FLATTEN:
    //record field rename mappings
    flattenfieldrenames = parseToMap(RENAME_FIELDS, getList(RENAME_FIELDS));
    //FLATTEN:
    //table names rename mappings
    flattentablenamesrenames = parseToMap(TABLES_RENAME, getList(TABLES_RENAME));
    //FLATTEN:
    //delimiter for flattened fields
    flattenDelimiter = getString(FLATTEN_DELIMITER);
    //FLATTEN:
    //pkValueFields list
    flattenPkValueFields = getList(FLATTEN_PK_PROPAGATE_VALUE_FIELDS).stream().map(String::toLowerCase).collect(Collectors.toList());
    //FLATTEN:
    //container whitelist
    flattenWhitelistContainers = getList(FLATTEN_WHITELIST_CONTAINERS).stream().map(String::toLowerCase).collect(Collectors.toList());;
    //FLATTEN:
    //instruction cache size
    flattenInstructionCacheSize = getInt(FLATTEN_INSTRUCTION_CACHE_SIZE);
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }

  public String connectorName() {
    return connectorName;
  }

  public EnumSet<TableType> tableTypes() {
    return tableTypes;
  }

  public Set<String> tableTypeNames() {
    return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
  }

  private static class EnumValidator implements ConfigDef.Validator {
    private final List<String> canonicalValues;
    private final Set<String> validValues;

    private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
      this.canonicalValues = canonicalValues;
      this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
      final List<String> canonicalValues = new ArrayList<>(enumerators.length);
      final Set<String> validValues = new HashSet<>(enumerators.length * 2);
      for (E e : enumerators) {
        canonicalValues.add(e.toString().toLowerCase());
        validValues.add(e.toString().toUpperCase());
        validValues.add(e.toString().toLowerCase());
      }
      return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(String key, Object value) {
      if (!validValues.contains(value)) {
        throw new ConfigException(key, value, "Invalid enumerator");
      }
    }

    @Override
    public String toString() {
      return canonicalValues.toString();
    }
  }
  //FLATTEN:
  //Parse the configured rename mappings for record fields --ultimately the table column
  private static Map<String, String> parseToMap(String key, List<String> mappings) {
    Map<String, String> map;
    try {
      map = mappings.stream().map(s -> s.split(":")).collect(Collectors.toMap(s -> s[0].toLowerCase(), s -> s[1].toLowerCase()));
      return map;
    } catch (Exception e) {
      throw new ConfigException(key, mappings, "List can't be converted to map: " + e.getMessage());
    }
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }

}
