package no.norsktipping.kafka.connect.jdbc.sink.StreamFlatten.StreamFlatten;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


class Entry {
  private String fieldName;
  private String targetName;
  private int depth;
  private Schema schema;
  private ArrayList<Container> containers = new ArrayList<>();
  private String path = "";
  private boolean isPK = false;

  public Entry(String fieldName, int depth, Schema schema, ArrayList<Container> containers) {
    this.fieldName = fieldName;
    this.targetName = targetName;
    this.depth = depth;
    this.schema = schema;
    containers.forEach(this::addContainer);
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getDepth() {
    return depth;
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

  public boolean isPK() {
    return isPK;
  }

  public void setPK() {this.isPK = true;}

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
      path = container.toLowerCase();
    } else {
      path = path + "." + container.toLowerCase();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Entry entry = (Entry) o;
    return getDepth() == entry.getDepth() &
            getFieldName().equals(entry.getFieldName()) &&
            getTargetName().equals(entry.getTargetName()) &&
            getSchema().equals(entry.getSchema()) &&
            getPath().equals(entry.getPath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFieldName(), getTargetName(), getDepth(), getSchema(), getPath());
  }

  @Override
  public String toString() {
    return "Entry{" +
            "fieldName='" + fieldName + '\'' +
            " targetName='" + targetName + '\'' +
            ", depth=" + depth +
            ", schema=" + schema +
            ", containers=" + containers +
            ", path='" + path + '\'' +
            '}';
  }
}