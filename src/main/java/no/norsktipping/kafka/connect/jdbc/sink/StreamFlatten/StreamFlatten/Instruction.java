package no.norsktipping.kafka.connect.jdbc.sink.StreamFlatten.StreamFlatten;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;


class Instruction {
  private final Schema sinkSchema;
  private final List<Container> mainContainer;
  private final Map<List<Container>, List<Entry>> subContainerEntries;
  private final Headers headers;
  private final List<Entry> keyFieldsPK;

  public Instruction(Schema sinkSchema, List<Container> mainContainer, Map<List<Container>, List<Entry>> subContainerEntries, Headers headers, List<Entry> keyFieldsPK) {
    this.sinkSchema = sinkSchema;
    this.mainContainer = mainContainer;
    this.subContainerEntries = subContainerEntries;
    this.headers = headers;
    this.keyFieldsPK = keyFieldsPK;
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

  public Headers getHeaders() {
    return headers;
  }

  public List<Entry> getKeyFieldsPK() {
    return keyFieldsPK;
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

