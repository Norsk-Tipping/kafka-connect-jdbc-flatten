package io.confluent.connect.jdbc.sink.StreamFlatten;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


class Container {
  private String containerName;
  private int depth;
  private Schema.Type type;
  private String path;
  private List<Entry> pkFields = new ArrayList<>();
  private List<Entry> mapKeys = new ArrayList<>();
  private boolean containsPkField = false;
  private boolean containsMapKey = false;
  private boolean isOptional;

  public Container(String containerName, String path, int depth, Schema.Type type, boolean optional) {
    this.containerName = containerName;
    this.depth = depth;
    this.type = type;
    this.path = path.toLowerCase();
    this.isOptional = optional;
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

  public void addMapKey(Entry mapKey) {
    this.mapKeys.add(mapKey);
    this.containsMapKey = true;
  }

  public List<Entry> getPkFields() {
    return pkFields;
  }
  public List<Entry> getMapKeys() {
    return mapKeys;
  }
  public boolean containsPkField() {
    return containsPkField;
  }
  public boolean containsMapKey() {
    return containsMapKey;
  }
  public boolean isOptional() { return isOptional; }

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
            ", optional=" + isOptional +
            ", containsPkField=" + containsPkField +
            '}';
  }
}

