package io.confluent.connect.jdbc.sink.StreamFlatten;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//Class that maintains a cache of instructions per SchemaPair. When running method transform,
//these instructions will split a single SinkRecord into multiple SinkRecords depending on how many
//arrays and/or maps it contains: Maps and Arrays will this way end up in seperate target tables.
public class FlattenTransformation {
  private static final Logger log =
          LoggerFactory.getLogger(io.confluent.connect.jdbc.sink.JdbcDbWriter.class);
  //parsed jdbcsinkconfig file for configuration parameters
  private JdbcSinkConfig config;

  //Cache of 'flattening' instructions per SchemaPair.
  private final LoadingCache<SchemaPair, List<MainProcessingInstruction>> processingInstructionCache;

  //Set the JDBCSinkconfig parameters and initializze the cache.
  public FlattenTransformation(JdbcSinkConfig config) {
    this.config = config;
    this.processingInstructionCache = CacheBuilder.newBuilder()
            .maximumSize(config.flattenInstructionCacheSize)
            .build(
                    new CacheLoader<SchemaPair, List<MainProcessingInstruction>>() {
                      public List<MainProcessingInstruction> load(SchemaPair schemapair) throws Exception {
                        log.debug("FlattenTransformation.proessInstructionCache: No instructions cached " +
                                "for schema: \n key: {} \n value: {}", schemapair.keySchema, schemapair.valueSchema);
                        return getMainProcessingInstructions(schemapair);
                      }
                    });
  }

  //Allos the cache to be enptied on task restarts e.g. when configuration changes are made.
  //Changes to the connector configuration most likely require a different set of 'flattening' instructions.
  public void clearInstructionCache() {
    this.processingInstructionCache.invalidateAll();
  }

  //Accept a single SinkRecord and stream multiple 'flattened' SinkRecord out.
  public Stream<SinkRecord> transform(SinkRecord record) throws ExecutionException {
    //get the topicname of the sink record
    final String topic = record.topic();
    log.debug("FlattenTransformation.topic: {}", topic);
    //get the partition number of the sink record
    final Integer partition = record.kafkaPartition();
    log.debug("FlattenTransformation.partition: {}", partition);
    //get the offset number of the sink record
    final Long offset = record.kafkaOffset();
    log.debug("FlattenTransformation.offset: {}", offset);
    //get the kafka timestamp of the sink record
    final Date timestamp = record.timestamp() != null ? Timestamp.toLogical(Timestamp.SCHEMA, record.timestamp()) : null;
    log.debug("FlattenTransformation.timestamp: {}", timestamp);
    //get the kafka timestamp type of the sink record
    final String timestampType = record.timestampType().name;
    log.debug("FlattenTransformation.timestampType: {}", timestampType);

    //Get cached 'flattening' instructions or, if the cache is empty, the cache implementaion triggers execution of
    //getMainProcessingInstructions(schemapair) to create a list of instructions for this schema pair.
    final List<MainProcessingInstruction> processInstructions;
    log.debug("FlattenTransformation.cache.size: {}", processingInstructionCache.size());
    log.debug("FlattenTransformation.cache.objects");
    processInstructions = processingInstructionCache.get(new SchemaPair(record.keySchema(), record.valueSchema()));
    log.debug("FlattenTransformation.cache.size: {}", processingInstructionCache.size());
    log.debug("FlattenTransformation.cache.objects");

    //Each set of instruction in the list represents a target table. Extraction of the substructures and insertion in
    //respective target tables can happen in parralel to improve throughput.
    return processInstructions.stream()
            .flatMap(pi -> {
              //Headers contain key values that convey primary key locations (full pathname of the primary key fields)
              // and their target field names for the 'flattened' sinkrecords
              log.debug("FlattenTransformation.transform cached processinginstruction headers {}", pi.getHeaders());
              //Execute the main container instruction, applied with the map of propagated kafkacoordinates if any and the record value.
              //this instruction will extract the relevant subcontainer (called main container as it becomes the new root container for the respective target table),
              // that is to be written to a seperate target table, from the nested record value.
              return pi.getMainContainerFunction().apply(Stream.of(new Pair<>(
                      //If config.flattencoordinates) is configured, the kafka coordinates are put in a map to allow these to propagate to
                      //flattened substructures e.g. get inserted in each target table.
                      config.flattencoordinates ?
                              new HashMap<String, Object>() {
                                {
                                  put(ucase(config.flattencoordinatenames.get(0)), topic);
                                  put(ucase(config.flattencoordinatenames.get(1)), partition);
                                  put(ucase(config.flattencoordinatenames.get(2)), offset);
                                  put(ucase(config.flattencoordinatenames.get(3)), timestamp);
                                  put(ucase(config.flattencoordinatenames.get(4)), timestampType);
                                }
                              }:
                              new HashMap<>()
                      , record.value())))
                      .flatMap(mc -> {
                        log.debug("FlattenTransformation.transform main container {}", mc);
                        //Execute the subprocessing instructions on the relevant subcontainer which results in a flat struct
                        //that carries the fields and field values that are to become columns in the respective target table.
                        return pi.getSubProcessingInstructions().apply(mc);
                      })
                      //If any values from the record key are to be propagated to each target table they get added to the structure.
                      .map(pi.getKeyPkFieldFunction().apply(record.key()))
                      .map(schemaStructPair -> {
                        log.debug("FlattenTransformation.transform record value outstruct after flattening: " + schemaStructPair.getValue1());
                        //Build a SinkRecord to convey the 'flattened' struct. In case the orginal sinkrecord was null,
                        //the value schema and the value of the target sinkrecord will also be set to null to allow for delete semantics
                        return new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                                schemaStructPair.getValue0(),
                                schemaStructPair.getValue1(),
                                record.kafkaOffset(), record.timestamp(), record.timestampType(), pi.getHeaders());
                      });
            });
  }

  //Function that builds the list of instructions that split this schema pair into 'flatteneted' SinkRecord(s).
  private List<MainProcessingInstruction> getMainProcessingInstructions(SchemaPair schemapair) {
    //Start with using function getFlattenSchema that recursively loops through the schema and returns a list of
    //the primitive fields, array items and map values in the schema. Each such entry has a list of containers(Struct, Map, Array)
    //that were 'entered' to get to the entry. This way the schema is reversed from being a container with subelements to elements
    //that contain information about which list of containers it belongs to.
    final List<Entry> flattenedSchemaList =
            Stream.of(schemapair.valueSchema)
                    .flatMap(s -> getFlattenSchema(getContainerPathname(s), s, 0, new ArrayList<Container>()))
                    .collect(Collectors.toList());
    log.debug("FlattenTransformation.getMainProcessingInstructions flattenedSchemaList: {}", flattenedSchemaList);
    //Group the list of entries by the sequence of containers they have in common up until the last Array or Map in their path is encountered.
    //If the entry does not have an Array or Map in its path they are grouped by the first container (main container).
    //These identified container(s) are the correct places in the hierarchical structure of the schema from where the schema is to be split into
    //seperate schemas each representing its own target table.
    Map<List<Container>, List<Entry>> groupedByMainContainer =
            flattenedSchemaList.stream().collect(Collectors.groupingBy(e -> e.getContainer()
                    .subList(0,
                            e.getContainer().indexOf(
                                    e.getContainer()
                                            .stream()
                                            .filter(t -> t.getType() == Schema.Type.ARRAY || t.getType() == Schema.Type.MAP)
                                            .reduce((first, second) -> second)
                                            .orElse(e.getContainer().get(0))) + 1)));

    //Headers are used to convey information to JDBCWriter -> BufferedRecords -> FieldsMetadata on which entries are primary keys.
    ConnectHeaders commonHeaders = new ConnectHeaders();
    //Tempaorary variable specifically which key entries are primary keys.
    List<Entry> keyFieldsPK = new ArrayList<>();

    //If the connector is configured with pkmode flatten and the key schema is available, then check if the key schema has any field(s)
    //that match the configured values in order to add them to the keyFieldsPK variable and headers with help of the extractPkFieldsFromKey function
    if (config.pkMode == JdbcSinkConfig.PrimaryKeyMode.FLATTEN && schemapair.keySchema != null) {
      extractPkFieldsFromKey(schemapair, keyFieldsPK, commonHeaders);
      //Key field primary key field(s) need to be configured in order to support delete operations on flattened target tables
      if (config.deleteEnabled && keyFieldsPK.isEmpty()) {
        throw new ConnectException(
                "Connector is configured with deleteEnabled " + config.deleteEnabled + " but none of the fields in " + config.pkFields
                        + " were found in the key schema of the record " + schemapair.keySchema
        );
      }
    }

    //With the keyfield(s) that are primary keys identified and with the grouped entries with their container information,
    //the remainder of instructions can be built.
    List<Instruction> instructions =
            //For each grouped set of entries (none in case the value schema was missing, and only one in case no arrays or maps
            //are encountered)
            groupedByMainContainer.entrySet().stream().map(c -> {
              ConnectHeaders headers = new ConnectHeaders();
              //Add each common header to the headers for this target schema i.e. propagate keyfields that are primary keys to the
              //flattened structure
              commonHeaders.forEach(headers::add);

              //Build a target struct wuith the same version as the original schema, but a name that represents the path
              //of the containers that are 'leading' to this container
              SchemaBuilder sinkSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                      .name(c.getKey().stream().map(Container::getContainerName).map(String::toLowerCase).collect(Collectors.joining(".")))
                      .version(schemapair.valueSchema.version());

              //If getFlattenSchema encountered any struct containers in the value schema that contain configured primary keys they have been
              //flagged as containsPkField and have been added as references from the respective container. Here they get collected in pkFIelds
              //and a new targetname gets set for these fields that corresponds to the full container path that leads to the respective entry
              //within the structure. This assures uniqueness and prevents collision with fields existing in substructures when such pkFIelds
              //propagate down to substructures from parent structures.
              ArrayList<Entry> pkFields = c.getKey().stream().filter(Container::containsPkField)
                      .flatMap(container -> container.getPkFields().stream())
                      .peek(f -> f.setTargetName(ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), true)))).collect(Collectors.toCollection(ArrayList::new));
              log.debug("FlattenTransformation.getMainProcessingInstructions pkFields from record value: {} in container {}",
                      pkFields,
                      c.getKey().get(c.getKey().size() - 1).getPath().equals("") ? c.getKey().get(c.getKey().size() - 1).getContainerName() :
                              c.getKey().get(c.getKey().size() - 1).getPath() + "." + c.getKey().get(c.getKey().size() - 1).getContainerName());
              //Any fields that function as primary keys identified previously get added to the flattened target schema (propagation from key to
              //each target struct schema
              keyFieldsPK.forEach(keyPK -> sinkSchemaBuilder.field(keyPK.getTargetName(), keyPK.getSchema()));

              //If getFlattenSchema encountered any map containers in the value schema they have been
              //flagged as containsMapKey and get treated in similar fashion as configured pkFIelds. This assures uniqueness for structures
              //that contain exact same map value subschemas within maps. For example a Map -> Struct -> Array -> Struct. The flattened
              //target structure within the array will have an added key that carries the value of the key in the Map.
              //Again these propagated key values get a unique target name that is based on their path within the structure.
              ArrayList<Entry> mapKeys = c.getKey().stream().filter(Container::containsMapKey)
                      .flatMap(container -> container.getMapKeys().stream())
                      .peek(f -> f.setTargetName(ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), true)))).collect(Collectors.toCollection(ArrayList::new));
              log.debug("FlattenTransformation.getMainProcessingInstructions.mapKeys: {} in container {}",
                      mapKeys,
                      c.getKey().get(c.getKey().size() - 1).getPath() + c.getKey().get(c.getKey().size() - 1).getContainerName());

              //Based on the main containers (places in the hierarchical structure of the schema from where the schema is to be split into
              //seperate schemas each representing its own target table, these can each be further grouped into subcontainers.
              //For example with a structure Struct -> Array -> Struct -> Struct, the last two structures are substructures within Array
              //as main container. Subcontainers require their own processing instructions but contained fields will end up in the same target
              //structure as its main container.
              Map<List<Container>, List<Entry>> subContainerEntries = c.getValue().stream()
                      .peek(f -> {
                        String fieldCase;
                        //If there are multiple fields that carry the same name, or if it is configured as a primary key,
                        //their target name will be set to their full container path plus their field name
                        if (c.getValue().stream().map(Entry::getFieldName).map(String::toLowerCase).filter(m -> m.equals(f.getFieldName().toLowerCase())).count() > 1
                                || f.isPK()) {
                          fieldCase = ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), true));
                          //Add any primary key fields to the header
                          if (f.isPK()) {
                            headers.add(f.getPath() + "." + f.getFieldName().toLowerCase(), new SchemaAndValue(Schema.STRING_SCHEMA, fieldCase));
                          }
                        }
                        //Else, add them with their simple name (to prevent a lot of fields in target tables that carry long names
                        else {
                          fieldCase = ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), false));
                        }
                        //Take into account optional fields that contain subcontainers. All fields flattened from subcontainers then
                        //also need to be overwritten as to be optional. To prevent non-null constraint conflicts in target tables
                        if (f.getContainer().get(f.getContainer().size() - 1).isOptional()) {
                          sinkSchemaBuilder.field(fieldCase, new SchemaBuilder(f.getSchema().type()).name(f.getSchema()
                                  .name()).optional().build());
                        } else {
                          sinkSchemaBuilder.field(fieldCase, f.getSchema());
                        }
                        f.setTargetName(fieldCase);
                      })
                      //Group these subcontainer fields by their container list minus the part that already leads to the main container
                      //i.e. in a structure Struct1 -> Array -> Struct2 -> Struct3, the fields in the last struct are already grouped
                      //by Struct1 -> Array as main container (part in the structure where the split needs to happen to individual target
                      //table) but now need to be further grouped within this group on Struct2 subcontainerpath and Struct2 -> Struct3 subconainerpath.
                      .collect(Collectors.groupingBy(e -> e.getContainer().stream()
                              .filter(i -> !c.getKey().subList(0, c.getKey().size() - 1).contains(i))
                              .collect(Collectors.toList())));

              //Map keys that are not already part of the target schema i.e. propagated map keys from parent structures as opposed to map keys that
              //belong the the substructure itself...
              mapKeys.stream().filter(f -> sinkSchemaBuilder.fields().stream().noneMatch(sf -> sf.name().toLowerCase().equals(f.getTargetName().toLowerCase()))
                              //...and only those map key fields that are within the same path at the subcontainer as opposed to disjoint paths
                              && c.getKey().containsAll(f.getContainer()) && !sinkSchemaBuilder.fields().isEmpty()
                      //get added to the target schema
              ).forEach(mapKey -> {
                log.debug("FlattenTransformation.getMainProcessingInstructions Added mapkKey to schema: {}", mapKey.getTargetName());
                sinkSchemaBuilder.field(mapKey.getTargetName(), new SchemaBuilder(mapKey.getSchema().type()).name(mapKey.getSchema()
                        .name()).build());
              });

              //Same as for propagated map keys. But primary keys get also added to the headers so that
              // JDBCWriter -> BufferedRecords -> FieldsMetadata can identify the primary keys when communicating with the target db on constraints.
              pkFields.stream()
                      .filter(f -> c.getKey().containsAll(f.getContainer()))
                      .peek(f -> headers.add(f.getPath() + "." + f.getFieldName().toLowerCase(), new SchemaAndValue(Schema.STRING_SCHEMA, f.getTargetName())))
                      .filter(f -> sinkSchemaBuilder.fields().stream().noneMatch(sf -> sf.name().toLowerCase().equals(f.getTargetName().toLowerCase()))
                              && !sinkSchemaBuilder.fields().isEmpty()
                      ).forEach(pField -> {
                log.debug("FlattenTransformation.getMainProcessingInstructions Added PKField to schema: {}", pField.getTargetName());
                sinkSchemaBuilder.field(pField.getTargetName(), new SchemaBuilder(pField.getSchema().type()).name(pField.getSchema()
                        .name()).build());
              });
              //If flatten.coordinates is set each flattened target structure will get the kafka coordinates added to their schema
              if (config.flattencoordinates && !sinkSchemaBuilder.fields().isEmpty()) {
                addCoordinates(sinkSchemaBuilder);
              }
              //Build the schema
              Schema sinkSchema = sinkSchemaBuilder.build();
              log.debug("FlattenTransformation.getMainProcessingInstructions Sinkschema build with fields: {}", sinkSchema.fields());
              //Now all information that is needed to construct processing functions for extraction of each main container and sub structures
              //is gathered. Return this list of information so that they can be gathered per main container in an instruction list.
              return new Instruction(sinkSchema, c.getKey(), subContainerEntries, headers, keyFieldsPK);
            }).collect(Collectors.toList());

    // If a <keySchema, keyValue, null, null> arrived and this needs to be interpreted as a delete operation, the previous
    // instruction block with help of groupedByMainContainer has been skipped since it has been empty. But the key primary key fields
    // have been captured, an instruction is then returned and to be added to the processing instruction cache that deals with null value
    // for this SchemaPair.
    if (schemapair.valueSchema == null &&
            config.deleteEnabled && !keyFieldsPK.isEmpty()) {
      log.debug("FlattenTransformation.getMainProcessingInstructions: value schema {}, delete enabled {}, key PK fields{}", schemapair.valueSchema, config.deleteEnabled, keyFieldsPK);
      //The processing instruction is then to return from the FlattenTrnsformation.transform method call a SinkRecord with the same value and value schema (null),
      // but with common header information that carries the record key primary key fields.
      return Collections.singletonList(new MainProcessingInstruction(
                      o -> o,
                      o -> Stream.of(new Pair<>(null, null)),
                      commonHeaders,
                      o -> s -> s
              )
      );
    }

    //Else return processing instructions that constructs the chain of functions to actually apply flattening of the record value.
    //- getMainContainerFunction constructs the chain of functions that need to be applied to extract the main container based on the gathered information on the
    //path of the main container and schema type of the other containers that need to be traversed to reach it.
    //- getSubProcessingFunction constructs the chain of functions that need to be applied to extract the fields from any subcontainers based on the gathered
    //information of the type of the main container, the list of subcontainerfields per subcontainer and the final target schema.
    //- gathered pk information is contained in the headers so FieldsMetadata will get this information and also returned in order to cache this
    //- getKeyPkFieldFunction constructs the chain of functions that need to be applied to extract the actual record key primary key values
    return instructions.stream().map(i -> new MainProcessingInstruction(
            getMainContainerFunction(i.getMainContainer()),
            getSubProcessingFunction(i.getMainContainer().get(i.getMainContainer().size() - 1), i.getSubContainerEntries(), i.getSinkSchema()),
            i.getHeaders(),
            getKeyPkFieldFunction(i.getKeyFieldsPK())
    )).collect(Collectors.toList());
  }

  //If the connector is configured with pkmode flatten and the key schema is available,
  // when building the instructions phase (when not yet cached) then this function is called to check if the key schema has any field(s)
  //that match the configured values in order to add them to the keyFieldsPK variable
  private void extractPkFieldsFromKey(SchemaPair schemapair, List<Entry> keyFieldsPK, ConnectHeaders headers) {
    if (!isContainer(schemapair.keySchema.type())) {
      if (config.pkFields.stream().map(String::toLowerCase).collect(Collectors.toList()).contains(
              getContainerPathname(schemapair.keySchema) + "." + "key")
      ) {
        String fieldCase = ucase(fullPathDelimiter(getContainerPathname(schemapair.keySchema), "key", true));
        headers.add(getContainerPathname(schemapair.keySchema) + "." + "key", new SchemaAndValue(Schema.STRING_SCHEMA,
                fieldCase));
        log.debug("FlattenTransformation.getMainProcessingInstructions A key field defined in {} is added to the out headers {}",
                JdbcSinkConfig.PK_FIELDS, headers);
        Entry keyEntry = new Entry("key", 0, schemapair.keySchema, new ArrayList<>());
        keyEntry.setTargetName(fieldCase);
        keyFieldsPK.add(keyEntry);
        log.debug("FlattenTransformation.getMainProcessingInstructions A single primitive key field defined in {} needs" +
                "to be extracted from record key value {}", JdbcSinkConfig.PK_FIELDS, keyEntry);
      }
    } else if (schemapair.keySchema.type() == Schema.Type.STRUCT) {
      ArrayList<Container> keyContainers = new ArrayList<>(Collections.singletonList(new Container(getContainerPathname(schemapair.keySchema), "", 0, Schema.Type.STRUCT, false)));
      schemapair.keySchema.fields().stream().filter(f -> config.pkFields.stream().map(String::toLowerCase).collect(Collectors.toList()).contains(
              getContainerPathname(schemapair.keySchema) + "." + f.name().toLowerCase())).forEach(keyMatch -> {
        String fieldCase = ucase(fullPathDelimiter(getContainerPathname(schemapair.keySchema), keyMatch.name(), true));
        headers.add(
                keyMatch.name()
                , new SchemaAndValue(Schema.STRING_SCHEMA,
                        fieldCase));
        log.debug("FlattenTransformation.getMainProcessingInstructions A key field defined in {} is added to the out headers {}",
                JdbcSinkConfig.PK_FIELDS, headers);
        Entry keyEntry = new Entry(keyMatch.name(), 1, keyMatch.schema(), keyContainers);
        keyEntry.setTargetName(fieldCase);
        keyFieldsPK.add(keyEntry);
        log.debug("FlattenTransformation.getMainProcessingInstructions A key field defined in {} needs " +
                "to be extracted from record key value STRUCT {}", JdbcSinkConfig.PK_FIELDS, keyEntry);
      });
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  // that defines the chain of functions needed to extract the main container from a record value
  private Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getMainContainerFunction(List<Container> containers) {
    //In case the container path contains more than one containers, let getNextContainer define the lambda that gets the next container and then recursively
    //define the rest of the chain of functions with self call on the remainder
    if (containers.size() > 1) {
      return getNextContainer(containers.get(0), containers.get(1))
              .andThen(getMainContainerFunction(containers.subList(1, containers.size())));
    } else {
      //The last function is always to return the remainder itself (the main container is then found)
      return o -> o;
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  // that defines the chain of functions needed to extract the primary key values that are configured to be propagated down from a record key
  private Function<Object, Function<Pair<Schema, Struct>, Pair<Schema, Struct>>> getKeyPkFieldFunction(List<Entry> keyFieldsPK) {
    if (config.pkMode == JdbcSinkConfig.PrimaryKeyMode.FLATTEN && !keyFieldsPK.isEmpty()) {
      log.debug("FlattenTransformation.getKeyPkFieldFunction pkMode = FLATTEN, keyFields defined are {}",
              keyFieldsPK);
      //If the previously gathered keyFieldsPK belong to a key that is according to the schema a Struct, return a lambda that add each key primary key
      //field to the target struct
      if (keyFieldsPK.stream().map(Entry::getContainer).anyMatch(cl -> !cl.isEmpty() && cl.size() == 1 && cl.get(0).getType() == Schema.Type.STRUCT)) {
        log.debug("FlattenTransformation.getKeyPkFieldFunction key schema is a STRUCT, pkFields to be extracted from key value are {}",
                keyFieldsPK);
        return keyValue -> schemaStructPair -> {
          if (schemaStructPair.getValue1() == null) {
            return schemaStructPair;
          }
          keyFieldsPK.forEach(kfPK -> schemaStructPair.getValue1().put(kfPK.getTargetName(), ((Struct) keyValue).get(kfPK.getFieldName())));
          return schemaStructPair;
        };
      }
      //Else, previously gathered keyFieldsPK is a single primitive key value, return a lambda that adds the key primary key value to the target Struct
      //field to the target struct
      else if (keyFieldsPK.size() == 1 && keyFieldsPK.stream().anyMatch(kPK -> kPK.getContainer().isEmpty())) {
        log.debug("FlattenTransformation.getKeyPkFieldFunction key schema is a primitive, pkField to be extracted from key value is {}",
                keyFieldsPK);
        return keyValue -> schemaStructPair -> {
          if (schemaStructPair.getValue1() == null) {
            return schemaStructPair;
          }
          schemaStructPair.getValue1().put(keyFieldsPK.get(0).getTargetName(), keyValue);
          return schemaStructPair;
        };
      }
    }
    log.debug("FlattenTransformation.getKeyPkFieldFunction no pkField to be extracted from key value",
            keyFieldsPK);
    return keyValue -> struct -> struct;
  }

  //Return a lambda (that gets cached with the processing instructions)
  //that defines thenApply function to extract the next container from the previous container
  //In case the container path contains more than one containers, let getNextContainer define the lambda that gets the next container and then recursively
  //define the rest of the chain of functions with self call on the remainder
  private Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getNextContainer(Container container, Container nextContainer) {
    if (container.getType() == Schema.Type.STRUCT) {
      //If the container is a Struct contains pk field(s) according to the analyzed structure, then extract the value from the struct and add it to the HashMap
      //that is part of the stream (stream is pair of this HashMap that contains fields that are to be propagated down and the container object itself)
      if (container.containsPkField()) {
        return os -> os.map(o -> {
          if (o == null || o.getValue1() == null) {
            return null;
          }
          container.getPkFields().forEach(f -> o.getValue0().put(f.getTargetName(), ((Struct) o.getValue1()).get(f.getFieldName())));
          return Pair.with(o.getValue0(), ((Struct) o.getValue1()).get(nextContainer.getContainerName()));
        });
      }
      //Else the function just replaces the Object in the stream with the next extracted container
      return os -> os.map(o -> {
        if (o == null || o.getValue1() == null) {
          return null;
        }
        return Pair.with(o.getValue0(), ((Struct) o.getValue1()).get(nextContainer.getContainerName()));
      });
    }
    //If the container is an Array it will not contain any pk fields that need to be propagated down.
    //Return a function that streams the individual array items
    else if (container.getType() == Schema.Type.ARRAY) {
      return os -> os.flatMap(o -> {
        if (o == null || o.getValue1() == null) {
          return null;
        }
        return ((ArrayList<Object>) o.getValue1()).stream().map(i -> Pair.with(o.getValue0(), i));
      });
    }
    //If the container is a Map, the map key needs to propagate down the structure. Return a function that
    //adds the map key to the HashMap in a full path naming notation plus the string 'key' to prevent collision and improve traceability between fields
    //that are positioned deeply nested several levels deeper than the map and the originating map key.
    //Then stream the individual map values individually.
    else if (container.getType() == Schema.Type.MAP) {
      return os -> os.flatMap(o -> {
        if (o == null || o.getValue1() == null) {
          return null;
        }
        return ((HashMap<Object, Object>) o.getValue1()).entrySet().stream().map(e -> {
                  String fieldCase = ucase(fullPathDelimiter(container.getPath() + "." + container.getContainerName(), "key", true));
                  o.getValue0().put(fieldCase, e.getKey().toString());
                  return new Pair<>(o.getValue0(), e.getValue());
                }
        );
      });
    } else {
      throw new ConnectException("FlattenTransformation.transform was instructed to get next container " + nextContainer + " from within " + container +
              ", however, " + container.getType() + " is not a known container type."
      );
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  //that defines the chain of functions that need to be applied to extract the fields from any subcontainers based on the gathered
  //information of the type of the main container, the list of subcontainerfields per subcontainer and the final target schema.
  private Function<Pair<HashMap<String, Object>, Object>, Stream<Pair<Schema, Struct>>> getSubProcessingFunction(Container container, Map<List<Container>, List<Entry>> entries, Schema schema) {
    //If the main container is a Struct, return a lambda that takes the main container object and propagated value as input and maps this to the final structure
    //with help of getSubContainersAndFieldsFunction that extracts the individual field values.
    if (container.getType() == Schema.Type.STRUCT) {
      return os -> {
        if (os != null && os.getValue1() != null) {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.concat(Stream.of(new Pair<>(schema, null)), Stream.of(os.getValue1()).map(o -> Pair.with(os.getValue0(), o)).map(getSubContainersAndFieldsFunction(entries, schema))
            .map(s -> new Pair<>(schema, s)));
          }
          return Stream.of(os.getValue1()).map(o -> Pair.with(os.getValue0(), o)).map(getSubContainersAndFieldsFunction(entries, schema)).map(s -> new Pair<>(schema, s));
        } else {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.of(new Pair<>(schema, null));
          }
          return Stream.empty();
        }
      };
    }
    //If the main container is an Arrau, return a lambda that takes the main container object and propagated values,
    //streams the individual array items and maps each item to a final structure
    //with help of getSubContainersAndFieldsFunction that extracts the individual items.
    else if (container.getType() == Schema.Type.ARRAY) {
      return os -> {
        if (os != null && os.getValue1() != null) {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.concat(Stream.of(new Pair<>(schema, null)),
                    ((ArrayList<Object>) os.getValue1()).stream().map(o -> Pair.with(os.getValue0(), o)).map(getSubContainersAndFieldsFunction(entries, schema)).map(s -> new Pair<>(schema, s)));
          }
          return ((ArrayList<Object>) os.getValue1()).stream().map(o -> Pair.with(os.getValue0(), o)).map(getSubContainersAndFieldsFunction(entries, schema)).map(s -> new Pair<>(schema, s));
        } else {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.of(new Pair<>(schema, null));
          }
          return Stream.empty();
        }
      };
    }
    //If the main container is a Map, return a lambda that takes the main container object and propagated values,
    //streams the individual map values and maps each item to a final structure
    //with help of getSubContainersAndFieldsFunction that extracts the individual items.
    //The map key gets added to the propagated values.
    else if (container.getType() == Schema.Type.MAP) {
      return os -> {
        if (os != null && os.getValue1() != null) {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.concat(Stream.of(new Pair<>(schema, null)),
                    ((HashMap<String, Object>) os.getValue1()).entrySet().stream()
                            .map(e -> {
                              String fieldCase = ucase(fullPathDelimiter(container.getPath() + "." + container.getContainerName(), "key", true));
                              HashMap<String, Object> mapIncludingKey = new HashMap<>(os.getValue0());
                              mapIncludingKey.put(fieldCase, e.getKey());
                              return new Pair<>(mapIncludingKey, e.getValue());
                            })
                            .map(getSubContainersAndFieldsFunction(entries, schema)).map(s -> new Pair<>(schema, s))
                    );
          }
          return ((HashMap<String, Object>) os.getValue1()).entrySet().stream()
                  .map(e -> {
                    String fieldCase = ucase(fullPathDelimiter(container.getPath() + "." + container.getContainerName(), "key", true));
                    HashMap<String, Object> mapIncludingKey = new HashMap<>(os.getValue0());
                    mapIncludingKey.put(fieldCase, e.getKey());
                    return new Pair<>(mapIncludingKey, e.getValue());
                  })
                  .map(getSubContainersAndFieldsFunction(entries, schema)).map(s -> new Pair<>(schema, s));
        } else {
          if (config.insertMode == JdbcSinkConfig.InsertMode.UPSERT) {
            return Stream.of(new Pair<>(schema, null));
          }
          return Stream.empty();
        }
      };
    } else {
      throw new ConnectException("FlattenTransformation.transform was instructed to get fields from container " + container +
              ", however, " + container.getType() + " is not a known container type."
      );
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  //that defines the chain of functions that need to be applied to extract the fields from a subcontainer and map this to a target Struct
  private Function<Pair<HashMap<String, Object>, Object>, Struct> getSubContainersAndFieldsFunction(Map<List<Container>, List<Entry>> containers, Schema schema) {
    return p -> {
      //Build the targetstruct based on the schema
      Struct targetStruct = new Struct(schema);
      //For each subcontainer that gets streamed
      containers.forEach((key, value) -> {
        //Define the function that streams the subcontainer with help of getSubContainerFunction
        getSubContainerFunction(key)
                .apply(p.getValue1())
                //For each subcontainer entry
                .forEach(sc ->
                        //Define the function that extracts fields with help of getFieldValuesFunction
                        //by giving it the type of subcontainer and contained entries as information
                        getFieldValuesFunction(key.get(key.size() - 1).getType(), value).apply(sc)
                                .forEach(fp ->
                                        //For each extracted field that gets returned/streamed,
                                        //add it to the target structure
                                        targetStruct.put(fp.getValue0(), fp.getValue1())
                                )
                );
      });
      //Also add any propagated fields
      p.getValue0().forEach(targetStruct::put);
      return targetStruct;
    };
  }

  //Return a lambda (that gets cached with the processing instructions)
  // that defines the chain of functions needed to extract and stream the subcontainers from the main container
  private Function<Object, Stream<Object>> getSubContainerFunction(List<Container> containers) {
    if (containers.size() > 1) {
      return getNextSubContainer(containers.get(0).getType(), containers.get(1))
              .andThen(getSubContainerFunction(containers.subList(1, containers.size())));
    } else {
      return Stream::of;
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  //Subcontainers within subcontainers can only be Struct (flattening happens on Array and Map)
  private Function<Object, Object> getNextSubContainer(Schema.Type type, Container container) {
    if (type == Schema.Type.STRUCT) {
      return o -> {
        if (o != null && ((Struct) o).schema().fields().stream().anyMatch(f -> f.name().equals(container.getContainerName()))) {
          return ((Struct) o).get(container.getContainerName());
        } else {
          return o;
        }
      };
    } else {
      return o -> o;
    }
  }

  //Return a lambda (that gets cached with the processing instructions)
  //Based on the schema type and list of entries, the fields, array items or map values get extracted and streamed
  private Function<Object, Stream<Pair<String, Object>>> getFieldValuesFunction(Schema.Type type, List<Entry> fields) {
    return o -> {
      if (type == Schema.Type.STRUCT && o != null) {
        Struct struct = (Struct) o;
        return fields.stream().map(f -> {
                  if (struct.schema().fields().stream().anyMatch(schemafield -> schemafield.name().equals(f.getFieldName()))) {
                    return new Pair<>(f.getTargetName(), struct.get(f.getFieldName()));
                  } else {
                    return new Pair<>(f.getTargetName(), null);
                  }
                }
        );
      } else if (type == Schema.Type.MAP && o != null && fields.size() > 1) {
        return Stream.of(new Pair<>(fields.get(1).getTargetName(), o));
      } else if (type == Schema.Type.ARRAY && o != null) {
        return Stream.of(new Pair<>(fields.get(0).getTargetName(), o));
      } else {
        return Stream.empty();
      }
    };
  }

  //Recursively loops through the schema and returns a list of
  //the primitive fields, array items and map values in the schema. Each such entry has a list of containers(Struct, Map, Array)
  //that were 'entered' to get to the entry. This way the schema is reversed from being a container with subelements to elements
  //that contain information about which list of containers it belongs to.
  private Stream<Entry> getFlattenSchema(String fieldname, Schema schema, int depth, ArrayList<Container> containers) {
    if (schema == null) {
      return Stream.empty();
    }
    containers.removeIf(c -> c.getDepth() >= depth);
    if (isContainer(schema.type())) {
      String path = containers.stream().map(Container::getContainerName).map(String::toLowerCase).collect(Collectors.joining("."));

      if (schema.type() == Schema.Type.STRUCT) {
        Container newContainer = new Container(fieldname, path, depth, Schema.Type.STRUCT, schema.isOptional());
        containers.add(newContainer);

        schema.fields().stream().filter(f -> !isContainer(f.schema().type())).forEach(f -> {
          if (config.flattenPkValueFields.stream().anyMatch(s -> {
            log.debug("FlattenTransformation.getFlattenSchema checking if configured propagate pk field matches current field {}  {}",
                    s,
                    path.isEmpty() ? ucase(fieldname.toLowerCase() + "." + f.name().toLowerCase()) : (path + "." + fieldname.toLowerCase() + "." + f.name().toLowerCase()));
            return s.equals(path.isEmpty() ? fieldname.toLowerCase() + "." + f.name().toLowerCase() : (path + "." + fieldname.toLowerCase() + "." + f.name().toLowerCase()));
          })) {
            Entry entry = new Entry(f.name(), depth + 1, f.schema(), containers);
            newContainer.addPkField(entry);
            log.debug("FlattenTransformation.getFlattenSchema PK field found and added to container as PKField {}",
                    entry.getPath() + "." + entry.getFieldName());
          }
        });
        return schema.fields().stream().flatMap(f -> getFlattenSchema(f.name(), f.schema(), depth + 1, containers));
      }

      if (schema.type() == Schema.Type.ARRAY) {
        containers.add(new Container(fieldname, path, depth, Schema.Type.ARRAY, schema.isOptional()));
        return getFlattenSchema(fieldname, schema.valueSchema(), depth + 1, containers);
      }

      if (schema.type() == Schema.Type.MAP) {
        boolean isPkField = config.flattenPkValueFields.stream().anyMatch(s ->
                (path.isEmpty() ? fieldname.toLowerCase() + "key" : path + "." + fieldname.toLowerCase() + "key").equals(ucase(s)));
        Container newContainer = new Container(fieldname, path, depth, Schema.Type.MAP, schema.isOptional());
        containers.add(newContainer);
        Entry mapKey = new Entry("key", depth + 1, Schema.STRING_SCHEMA, containers);
        newContainer.addMapKey(mapKey);
        if (isPkField) {
          newContainer.addPkField(mapKey);
          log.debug("FlattenTransformation.getFlattenSchema propagate PK field found and added to container as PKField {}",
                  mapKey.getPath() + "." + mapKey.getFieldName());
        }
        return getFlattenSchema(fieldname, schema.valueSchema(), depth + 1, containers);
      }
    }
    Entry entry = new Entry(fieldname, depth, schema, containers);
    if (!config.flattenWhitelistContainers.isEmpty()) {
      if (config.flattenWhitelistContainers.stream()
              .noneMatch(s -> s.equals(
                      entry.getContainer().stream().map(Container::getContainerName).map(String::toLowerCase).collect(Collectors.joining("."))))) {
        log.debug("FlattenTransformation.getFlattenSchema: Not found following entry in whitelist {}. Entry will be filtered. ", entry.getPath() + "." + entry.getFieldName());
        return Stream.empty();
      }
    }
    if (containers.get(containers.size() - 1).getPkFields().stream().anyMatch(cpk -> cpk.getFieldName().toLowerCase().equals(fieldname.toLowerCase()))
            || config.pkFields.stream().anyMatch(pk -> {
      log.debug("FlattenTransformation.getFlattenSchema checking if configured pk field matches current field {}  {}",
              pk, entry.getPath() + "." + entry.getFieldName());
      return pk.equals(entry.getPath() + "." + entry.getFieldName().toLowerCase());
    })) {
      entry.setPK();
    }
    return Stream.of(entry);
  }

  private void addCoordinates(SchemaBuilder outschema) {
    outschema.field(ucase(config.flattencoordinatenames.get(0)), Schema.STRING_SCHEMA)
            .field(ucase(config.flattencoordinatenames.get(1)), Schema.INT32_SCHEMA)
            .field(ucase(config.flattencoordinatenames.get(2)), Schema.INT64_SCHEMA)
            .field(ucase(config.flattencoordinatenames.get(3)), Timestamp.builder().optional().build())
            .field(ucase(config.flattencoordinatenames.get(4)), Schema.STRING_SCHEMA);
  }

  private String ucase(String string) {
      return config.flattenUppercase ? string.toUpperCase() : string.toLowerCase();
  }

  private String fullPathDelimiter(String path, String fieldName, boolean fullPath) {
    if (config.flattenfieldrenames.get(path + "." + fieldName.toLowerCase()) != null) {
      return config.flattenfieldrenames.get(path + "." + fieldName.toLowerCase());
    }
    if (fullPath) {
      return ucase(path.replace(".", config.flattenDelimiter) + config.flattenDelimiter + fieldName);
    } else {
      return ucase(fieldName);
    }
  }

  private Boolean isContainer(Schema.Type schemaType) {
    return schemaType == Schema.Type.STRUCT ||
            schemaType == Schema.Type.ARRAY ||
            schemaType == Schema.Type.MAP;
  }

  private String getContainerPathname(Schema s) {
    if (s == null) {
      return null;
    }
    if (s.type() == Schema.Type.STRUCT && s.name() != null) {
      if (s.name().contains(".")) {
        return s.name().substring(s.name().lastIndexOf('.') + 1).toLowerCase();
      }
      else{
          return s.name().toLowerCase();
        }
      }
      return "root";
    }
  }