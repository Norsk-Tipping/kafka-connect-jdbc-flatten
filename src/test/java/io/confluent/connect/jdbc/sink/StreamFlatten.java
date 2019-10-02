package io.confluent.connect.jdbc.sink;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import javafx.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamFlatten {
  private static final String configDelimiter = "_";
  private static final boolean configUpperCase = true;
  private static final boolean configCoordinates = true;
  private static final List<String> configCoordinatenames = Arrays.asList("kafkatopic", "kafkapartition", "kafkaoffset", "kafkatimestamp", "kafkatimestamptype");
  private static final List<String> configPKFields = Arrays.asList(
          "person.firstname",
          "person.record2.mapfield.key",
          "person.record2.record2again.addresses.key"
          //"person.record1.addresses.key",
          //"person.record2.record2again.address.street"
  );
  private static final Map<String, String> configRenameFields = Stream.of(new String[][] {
          { "person.record2.record2again.firstname", "P_R2_R2A_F" },
          { "person.record2.record2again.addresses.key", "P_R2_R2A_A_K" },
  }).collect(Collectors.toMap(data -> ucase(data[0]), data -> data[1]));

  private static final List<String> configWhitelist  = Arrays.asList(
          //"person",
          //"person.firstname",
          "person.record2",
          "person.record2.mapfield"
          //"person.record2.mapfield.key"
          //"person.record2.record2again.addresses.key",
          //"person.record2.firstname"
          //"person.record1.addresses.key",
          //"person.record2.record2again.address.street",
          //"person.record2.record2again.firstname"
          );

  private static final int cacheCapacity = 100;
  private static final LoadingCache<Schema, List<MainProcessingInstruction>> processingInstructionCache = CacheBuilder.newBuilder()
          .maximumSize(cacheCapacity)
          .build(
                  new CacheLoader<Schema, List<MainProcessingInstruction>>() {
                    public List<MainProcessingInstruction> load(Schema schema) throws Exception {
                      return getMainProcessingInstructions(schema);
                    }
                  });

  private static final Schema ARRAYSTRUCT = SchemaBuilder.struct()
          .name("com.example.Address")
          .field("street", Schema.STRING_SCHEMA)
          .field("number", Schema.INT8_SCHEMA)
          .build();

  private static final Schema ARRAY = SchemaBuilder.array(ARRAYSTRUCT).optional().defaultValue(null)
          .build();

  private static final Schema MAP = SchemaBuilder.map(Schema.STRING_SCHEMA, ARRAYSTRUCT).optional().defaultValue(null)
          .build();

  private static final Schema SCHEMA4 = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA).name("com.example.Map").optional().defaultValue(null)
          .build();

  private static final Schema SCHEMAX = SchemaBuilder.struct().name("com.example.Subrecord1")
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA).optional()
          .field("short", Schema.OPTIONAL_INT16_SCHEMA).optional()
          .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA).optional()
          .field("modified", Timestamp.SCHEMA)
          .field("address", ARRAY)
          .field("addresses", MAP)
          .build();

  private static final Schema SCHEMAY = SchemaBuilder.struct().name("com.example.Subrecord2")
          .field("firstname", Schema.STRING_SCHEMA)
          .field("lastname", Schema.STRING_SCHEMA)
          .field("modified", Timestamp.SCHEMA)
          .field("record2again", SCHEMAX).optional().defaultValue(null)
          .field("mapfield", SCHEMA4)
          .build();

  private static final Schema SCHEMA3 = SchemaBuilder.struct().version(1).name("com.example.Person")
          .field("firstname", Schema.STRING_SCHEMA)
          .field("record1", SCHEMAX)
          .field("record2", SCHEMAY)
          .build();



  private static Boolean isContainer(Schema.Type schemaType) {
    return schemaType == Schema.Type.STRUCT ||
            schemaType == Schema.Type.ARRAY ||
            schemaType == Schema.Type.MAP;
  }

  private static String getContainerPathname(Schema s) {
    if (s.type()== Schema.Type.STRUCT && s.name().contains(".")) {
      return s.name().substring(s.name().lastIndexOf('.') + 1);
    }
    return "root";
  }

  @Test
  public void stream_flatten_test() throws Exception {
    final List<Struct> addresses = new ArrayList<Struct>();
    final Struct address1 = new Struct(ARRAYSTRUCT)
            .put("street", "Moltevegen")
            .put("number", (byte) 14);
    final Struct address2 = new Struct(ARRAYSTRUCT)
            .put("street", "M?sabekkvegen")
            .put("number", (byte) 9);
    addresses.add(address1);
    addresses.add(address2);

    final Map<String, Struct> mapStruct = new HashMap<>();
    final Struct addressHome = new Struct(ARRAYSTRUCT)
            .put("street", "Homestreet")
            .put("number", (byte) 2);
    final Struct addressOffice = new Struct(ARRAYSTRUCT)
            .put("street", "OfficeStreet")
            .put("number", (byte) 1);
    mapStruct.put("homeKey", addressHome);
    mapStruct.put("officeKey", addressOffice);
    final Map<String, Integer> mapValue = new HashMap<>();
    mapValue.put("mapentry1key", 23);
    mapValue.put("mapentry2key", 34);
    mapValue.put("mapentry3key", 8);

    final Struct struct1 = new Struct(SCHEMAX)
            .put("firstname", "Alex")
            .put("lastname", "Alexis")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("double", -2436546.56457)
            .put("modified", new Date(1474661402123L))
            .put("address", addresses);

    final Struct struct2again = new Struct(SCHEMAX)
            .put("firstname", "JohnsBrother")
            .put("lastname", "FernandesFamilyMemberAdditional")
            .put("addresses", mapStruct)
            .put("modified", new Date(1474661402123L));

    final Struct struct2 = new Struct(SCHEMAY)
            .put("firstname", "John")
            .put("lastname", "Fernandes")
            .put("modified", new Date(1474661402123L))
            .put("record2again", struct2again)
            .put("mapfield", mapValue);

    final Struct struct3 = new Struct(SCHEMA3)
            .put("firstname", "John")
            .put("record1", struct1)
            .put("record2", struct2);

    SinkRecord record = new SinkRecord("atopic", 1, null, null, SCHEMA3, struct3, 3000);
    //get the topicname of the sink record
    final String topic = record.topic();
    //get the partition number of the sink record
    final Integer partition = record.kafkaPartition();
    //get the offset number of the sink record
    final Long offset = record.kafkaOffset();
    //get the kafka timestamp of the sink record
    final Date timestamp = record.timestamp() != null ? Timestamp.toLogical(Timestamp.SCHEMA, record.timestamp()) : null;
    //get the kafka timestamp type of the sink record
    final String timestampType = record.timestampType().name;

    HashMap<String, Object> propagateFields = new HashMap<>();
    if (configCoordinates) {
      propagateFields.put(ucase(configCoordinatenames.get(0)), record.topic());
      propagateFields.put(ucase(configCoordinatenames.get(1)), record.kafkaPartition());
      propagateFields.put(ucase(configCoordinatenames.get(2)), record.kafkaOffset());
      propagateFields.put(ucase(configCoordinatenames.get(3)), record.timestamp() != null ? Timestamp.toLogical(Timestamp.SCHEMA, record.timestamp()) : null);
      propagateFields.put(ucase(configCoordinatenames.get(4)), record.timestampType().name);
    }
    final List<MainProcessingInstruction> processInstructions;
    try {
      processInstructions = processingInstructionCache.get(record.valueSchema());
      final List<Object> executeFunctions = processInstructions.parallelStream()
              .flatMap(pi -> pi.getMainContainerFunction().apply(Stream.of(new Pair<>(propagateFields, record.value())))
                      .flatMap(mc -> pi.getSubProcessingInstructions().apply(mc))).collect(Collectors.toList());
      System.out.println("Cache size: " + processingInstructionCache.size());
      System.out.println(executeFunctions);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private static List<MainProcessingInstruction> getMainProcessingInstructions(Schema schema) {
    final List<Entry> flattenedSchemaList =
            Stream.of(schema)
                    .flatMap(s -> getFlattenSchema(getContainerPathname(s), s, 0, new ArrayList<Container>()))
                    .collect(Collectors.toList());
    System.out.println(flattenedSchemaList);

    Map<List<Container>, List<Entry>> groupedByMainContainer =
            flattenedSchemaList.stream().collect(Collectors.groupingBy(e -> e.getContainer()
                    .subList(0,
                            e.getContainer().indexOf(
                                    e.getContainer()
                                            .stream()
                                            .filter(t -> t.getType() == Schema.Type.ARRAY || t.getType() == Schema.Type.MAP)
                                            .reduce((first, second) -> second)
                                            .orElse(e.getContainer().get(0))) +1 )));

      ArrayList<Entry> pkFields = flattenedSchemaList.stream().filter(Entry::getPk)
              .peek(f -> f.setTargetName(ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), true)))).collect(Collectors.toCollection(ArrayList::new));

    List<Instruction> instructions =
            groupedByMainContainer.entrySet().stream().map(c -> {
              SchemaBuilder sinkSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT)
                      .name(c.getKey().stream().map(Container::getContainerName).collect(Collectors.joining(".")))
                      .version(schema.version());

              Map<List<Container>, List<Entry>> subContainerEntries = c.getValue().stream()
                      .peek(f ->{
                        String fieldCase;
                        if (f.getFieldName().equals("key") || c.getValue().stream().map(Entry::getFieldName).filter(m -> m.equals(f.getFieldName())).count() > 1) {
                         fieldCase = ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), true));
                        }
                        else {
                          fieldCase = ucase(fullPathDelimiter(f.getPath(), f.getFieldName(), false));
                        }
                        sinkSchemaBuilder.field(fieldCase, f.getSchema());
                        f.setTargetName(fieldCase);
                      })
                      .collect(Collectors.groupingBy(e -> e.getContainer().stream()
                      .filter(i -> !c.getKey().subList(0, c.getKey().size() -1).contains(i))
                      .collect(Collectors.toList())));
              pkFields.stream().filter(f -> sinkSchemaBuilder.fields().stream().noneMatch(sf -> sf.name().equals(f.getTargetName()))
              && c.getKey().containsAll(f.getContainer()) && !sinkSchemaBuilder.fields().isEmpty()
              ).forEach(pField -> sinkSchemaBuilder.field(pField.getTargetName(), pField.getSchema()));
              if (configCoordinates) {addCoordinates(sinkSchemaBuilder);}
              Schema sinkSchema = sinkSchemaBuilder.build();
              //System.out.println(sinkSchema.fields());
              return new Instruction(sinkSchema, c.getKey(), subContainerEntries);
            }).collect(Collectors.toList());

    System.out.println(instructions);


    return instructions.stream().map(i -> new MainProcessingInstruction(
            getMainContainerFunction(i.getMainContainer()),
            getSubProcessingFunction(i.getMainContainer().get(i.getMainContainer().size() -1), i.getSubContainerEntries(), i.getSinkSchema())
    )).collect(Collectors.toList());
  }

  private static Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getMainContainerFunction(List<Container> containers) {
    if (containers.size() > 1) {
     return getNextContainer(containers.get(0), containers.get(1))
             .andThen(getMainContainerFunction(containers.subList(1, containers.size())));
   }
   else {
     return o -> o;
   }
  }

  private static Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getNextContainer(Container container, Container nextContainer) {
      if (container.getType() == Schema.Type.STRUCT) {
        if (container.containsPkField()) {
          return  os -> os.map(o -> {
            container.getPkFields().forEach(f -> o.getKey().put(f.getTargetName(), ((Struct) o.getValue()).get(f.getFieldName())));
            return new Pair<>(o.getKey(), ((Struct) o.getValue()).get(nextContainer.getContainerName()));
          });
        }
        return os -> os.map(o -> new Pair<>(o.getKey(), ((Struct) o.getValue()).get(nextContainer.getContainerName())));
      }
      else if (container.getType() == Schema.Type.ARRAY) {
        return os -> os.flatMap(o -> ((ArrayList<Object>) o.getValue()).stream().map(i -> new Pair<>(o.getKey(), i)));
      }
      else if (container.getType() == Schema.Type.MAP) {
        return os -> os.flatMap(o -> ((HashMap<Object, Object>) o.getValue()).entrySet().stream().map(e -> {
                  String fieldCase = ucase(fullPathDelimiter(container.getPath() + "."  + container.getContainerName(), "key", true));
                  o.getKey().put(fieldCase, e.getKey().toString());
                  return new Pair<>(o.getKey(), e.getValue());
                }
        ));
      }
      else {
        return os -> Stream.empty();
      }
  }

  private static Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> getSubProcessingFunction(Container container, Map<List<Container>, List<Entry>> containers, Schema schema){
      if (container.getType() == Schema.Type.STRUCT) {
        return os -> {
          if (os.getValue() != null) {
            return Stream.of(os.getValue()).map(o -> new Pair<>(os.getKey(), o)).map(getSubContainersAndFieldsFunction(containers, schema));
          }
          else {return Stream.empty();}
        };
      }
      else if (container.getType() == Schema.Type.ARRAY) {
        return os -> {
          if (os.getValue() != null) {
            return ((ArrayList<Object>) os.getValue()).stream().map(o -> new Pair<>(os.getKey(), o)).map(getSubContainersAndFieldsFunction(containers, schema));
          }
          else {return Stream.empty();}
        };
      }
      else if (container.getType() == Schema.Type.MAP) {
        return os -> {
          if (os.getValue() != null) {
            return ((HashMap<String, Object>) os.getValue()).entrySet().stream()
                    .map(e -> {
                      String fieldCase = ucase(fullPathDelimiter(container.getPath() + "."  + container.getContainerName(), "key", true));
                      HashMap<String, Object> mapIncludingKey = new HashMap<>(os.getKey());
                      mapIncludingKey.put(fieldCase, e.getKey().toString());
                      return new Pair<>(mapIncludingKey, e.getValue());
                    })
                    .map(getSubContainersAndFieldsFunction(containers, schema));
          }
          else {return Stream.empty();}
        };
      }
      else {
        return os -> Stream.empty();
      }
  }

  private static Function<Pair<HashMap<String, Object>, Object>, Struct> getSubContainersAndFieldsFunction(Map<List<Container>, List<Entry>> containers, Schema schema) {
    return p -> {
      Struct targetStruct = new Struct(schema);
      containers.forEach((key, value) -> {
        if (containers.size() > 1 && key.get(0).getType() == Schema.Type.MAP && key.size() == 1) {
          return;
        }
        getSubContainerFunction(key)
                .apply(p.getValue())
                .forEach(sc ->
                        getFieldValuesFunction(key.get(key.size() - 1).getType(), value).apply(sc)
                                .forEach(fp ->
                                        targetStruct.put(fp.getKey(), fp.getValue())
                                )
                );
      });
      p.getKey().forEach(targetStruct::put);
      return targetStruct;
    };
  }

  private static Function<Object, Stream<Object>> getSubContainerFunction(List<Container> containers) {
    if (containers.size() > 1) {
      return getNextSubContainer(containers.get(0).getType(), containers.get(1))
              .andThen(getSubContainerFunction(containers.subList(1, containers.size())));
    }
    else {
      return Stream::of;
    }
  }

  private static Function<Object, Object> getNextSubContainer(Schema.Type type, Container container) {
      if (type == Schema.Type.STRUCT) {
        return o -> ((Struct) o).get(container.getContainerName());
      }
      else {
        return o -> o;
      }
  }

  private static Function<Object, Stream<Pair<String, Object>>> getFieldValuesFunction(Schema.Type type, List<Entry> fields) {
    return o -> {
      if (type == Schema.Type.STRUCT && o!=null) {
        Struct struct = (Struct) o;
        return fields.stream().map(f -> new Pair<>(f.getTargetName(), struct.get(f.getFieldName())));
      }
      else if (type == Schema.Type.MAP && o!=null && fields.size() > 1) {
        //return ((HashMap<Object, Object>) o).values().stream().map(v -> new Pair<>(fields.get(1).getPath() + "." + fields.get(1).getFieldName(), v));
        return Stream.of(new Pair<>(fields.get(1).getTargetName(), o));
      }
      else if (type == Schema.Type.ARRAY && o!=null){
        //return e.stream().map(f -> new Pair<>(f.getContainer().stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + f.getFieldName(), o));
        return Stream.of(new Pair<>(fields.get(0).getTargetName(), o));
      }
      else {
        return Stream.empty();
      }
    };
  }

   private static Stream<Entry> getFlattenSchema(String fieldname, Schema schema, int depth, ArrayList<Container> containers) {

    if (!configWhitelist.isEmpty()) {
      if (isContainer(schema.type())) {
        if (containers.size() > 0 && configWhitelist.stream()
                .noneMatch(s -> ucase(s).startsWith(
                        ucase(containers.stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + fieldname)))) {
          System.out.println("field: " + ucase(containers.stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + fieldname) +
                  " Not in whitelist: " + configWhitelist.stream().map(String::toUpperCase).collect(Collectors.toList()));
          return Stream.empty();
        }
        if (containers.size() == 0 && configWhitelist.stream()
                .noneMatch(s -> ucase(s).startsWith(ucase(fieldname)))) {
          System.out.println("field: " + ucase(ucase(fieldname)) +
                  " Not in whitelist: " + configWhitelist.stream().map(String::toUpperCase).collect(Collectors.toList()));
          return Stream.empty();
        }
      }
/*      else if (!isContainer(schema.type())) {
        if (configWhitelist.stream()
                .noneMatch(s -> ucase(s)
                        .equals(ucase(containers.stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + fieldname)))) {
          System.out.println("field: " + ucase(containers.stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + fieldname) +
                  " Not in whitelist: " + configWhitelist.stream().map(String::toUpperCase).collect(Collectors.toList()));
          return Stream.empty();
        }
      }*/
    }

    if(isContainer(schema.type())) {
      containers.removeIf(c -> c.getDepth() >= depth);
      String path = containers.stream().map(Container::getContainerName).collect(Collectors.joining("."));

      if (schema.type() == Schema.Type.STRUCT) {
        containers.add(new Container(fieldname, path, depth, Schema.Type.STRUCT));
        return schema.fields().stream().flatMap(f -> getFlattenSchema(f.name(), f.schema(), depth + 1, containers));
      }

      if (schema.type() == Schema.Type.ARRAY) {
        containers.add(new Container(fieldname, path, depth, Schema.Type.ARRAY));
        return getFlattenSchema(fieldname, schema.valueSchema(), depth + 1, containers);
      }

      if (schema.type() == Schema.Type.MAP) {
        boolean isPkField = configPKFields.stream().anyMatch(s -> ucase(path + "." + fieldname + "key").equals(ucase(s)));
        Container newContainer = new Container(fieldname, path, depth, Schema.Type.MAP);
        containers.add(newContainer);
        Entry mapKey = new Entry( "key", depth +1, isPkField, Schema.STRING_SCHEMA, containers);
        if (isPkField) {newContainer.addPkField(mapKey);}
        return Stream.concat(
                Stream.of(mapKey)
                , getFlattenSchema(fieldname, schema.valueSchema(), depth + 1, containers));
      }
    }
    boolean isPkField = configPKFields.stream().anyMatch(s ->
            ucase(containers.stream().map(Container::getContainerName).collect(Collectors.joining(".")) + "." + fieldname).equals(ucase(s)));
    Entry entry = new Entry(fieldname, depth, isPkField, schema, containers);
    if (isPkField) {entry.getContainer().get(entry.getContainer().size()-1).addPkField(entry);}
    return Stream.of(entry);
  }

  private static void addCoordinates(SchemaBuilder outschema) {
    outschema.field(ucase(configCoordinatenames.get(0)), Schema.OPTIONAL_STRING_SCHEMA)
            .field(ucase(configCoordinatenames.get(1)), Schema.OPTIONAL_INT32_SCHEMA)
            .field(ucase(configCoordinatenames.get(2)), Schema.OPTIONAL_INT64_SCHEMA)
            .field(ucase(configCoordinatenames.get(3)), Timestamp.builder().optional().build())
            .field(ucase(configCoordinatenames.get(4)), Schema.OPTIONAL_STRING_SCHEMA);
  }
  private static String ucase(String string) {
    return configUpperCase ? string.toUpperCase() : string.toLowerCase();
  }
  private static String fullPathDelimiter(String path, String fieldName, boolean fullPath) {
    if(configRenameFields.get(ucase(path + "." + fieldName)) != null) {
      return configRenameFields.get(ucase(path + "." + fieldName));
    }
    if (fullPath) {
      return (path.replace(".", configDelimiter) + configDelimiter + fieldName);
    }
    else {
      return fieldName;
    }
  }
}



class MainProcessingInstruction {
  private final Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> mainContainerFunction;
  private Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> subProcessingInstructions;

  public MainProcessingInstruction(Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> mainContainerFunction, Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> subProcessingInstructions) {
    this.mainContainerFunction = mainContainerFunction;
    this.subProcessingInstructions = subProcessingInstructions;
  }

  public Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getMainContainerFunction() {
    return mainContainerFunction;
  }


  public Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> getSubProcessingInstructions() {
    return subProcessingInstructions;
  }
}


class Instruction {
  private final Schema sinkSchema;
  private final List<Container> mainContainer;
  private final Map<List<Container>, List<Entry>> subContainerEntries;

  public Instruction(Schema sinkSchema, List<Container> mainContainer, Map<List<Container>, List<Entry>> subContainerEntries) {
    this.sinkSchema = sinkSchema;
    this.mainContainer = mainContainer;
    this.subContainerEntries = subContainerEntries;
  }

  public Schema getSinkSchema() {
    return sinkSchema;
  }

  public List<Container> getMainContainer() {
    return Collections.unmodifiableList(mainContainer);
  }

  public Map<List<Container>, List<Entry>> getSubContainerEntries() {
    return Collections.unmodifiableMap(subContainerEntries);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Instruction that = (Instruction) o;
    return getSinkSchema().equals(that.getSinkSchema()) &&
            getMainContainer().equals(that.getMainContainer()) &&
            Objects.equals(getSubContainerEntries(), that.getSubContainerEntries());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSinkSchema(), getMainContainer(), getSubContainerEntries());
  }

  @Override
  public String toString() {
    return "Instruction{" +
            "sinkSchema=" + sinkSchema +
            ", mainContainer=" + mainContainer +
            ", subContainerEntries=" + subContainerEntries +
            '}';
  }
}

class Container {
  private String containerName;
  private int depth;
  private Schema.Type type;
  private String path;
  private List<Entry> pkFields = new ArrayList<>();
  private boolean containsPkField = false;

  public Container(String containerName, String path, int depth, Schema.Type type) {
    this.containerName = containerName;
    this.depth = depth;
    this.type = type;
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public String getContainerName() {
    return containerName;
  }

  public int getDepth() {
    return depth;
  }

  public Schema.Type getType() {
    return type;
  }

  public void addPkField(Entry pkField) {
    this.pkFields.add(pkField);
    this.containsPkField = true;
  }

  public List<Entry> getPkFields() {
    return pkFields;
  }
  public boolean containsPkField() {
    return containsPkField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Container container = (Container) o;
    return getDepth() == container.getDepth() &&
            getContainerName().equals(container.getContainerName()) &&
            getPath().equals(container.getPath()) &&
            getType() == container.getType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerName(), getDepth(), getType(), getPath());
  }

  @Override
  public String toString() {
    return "Container{" +
            "containerName='" + containerName + '\'' +
            ", path=" + path +
            ", depth=" + depth +
            ", type=" + type +
            '}';
  }
}

class Entry {
  private String fieldName;
  private String targetName;
  private int depth;
  private boolean pk;
  private Schema schema;
  private ArrayList<Container> containers = new ArrayList<>();
  private String path = "";

  public Entry(String fieldName, int depth, boolean pk, Schema schema, ArrayList<Container> containers) {
    this.fieldName = fieldName;
    this.targetName = targetName;
    this.depth = depth;
    this.schema = schema;
    this.pk = pk;
    containers.forEach(this::addContainer);
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getDepth() {
    return depth;
  }

  public boolean getPk() {
    return pk;
  }

  public String getPath() {
    return path;
  }

  public Schema.Type getType() {
    return schema.type();
  }

  public Schema getSchema() {
    return schema;
  }

  public List<Container> getContainer() {
    return Collections.unmodifiableList(containers);
  }
  public String getTargetName() {
    return targetName;
  }
  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  public void addContainer(Container container) {
    containers.add(container);
    addToPath(container.getContainerName());
  }

  private void addToPath(String container) {
    if (path == "") {
      path = container;
    } else {
      path = path + "." + container;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Entry entry = (Entry) o;
    return getDepth() == entry.getDepth() &
            getPk() == entry.getPk() &
            getFieldName().equals(entry.getFieldName()) &&
            getTargetName().equals(entry.getTargetName()) &&
            getSchema().equals(entry.getSchema()) &&
            getPath().equals(entry.getPath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFieldName(), getTargetName(), getDepth(), getPk(), getSchema(), getPath());
  }

  @Override
  public String toString() {
    return "Entry{" +
            "fieldName='" + fieldName + '\'' +
            "targetName='" + targetName + '\'' +
            ", depth=" + depth +
            ", pk=" + pk +
            ", schema=" + schema +
            ", containers=" + containers +
            ", path='" + path + '\'' +
            '}';
  }
}