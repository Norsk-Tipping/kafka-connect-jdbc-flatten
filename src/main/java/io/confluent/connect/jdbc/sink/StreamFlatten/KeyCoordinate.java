package io.confluent.connect.jdbc.sink.StreamFlatten;

import java.util.Objects;

public class KeyCoordinate {
    private String valueSchemaName;
    private int partition;
    private long offset;

    public KeyCoordinate(String valueSchemaName, int partition, long offset) {
        this.valueSchemaName = valueSchemaName;
        this.partition = partition;
        this.offset = offset;
    }

    public String getvalueSchemaName() {
        return valueSchemaName;
    }

    public void setvalueSchemaName(String valueSchemaName) {
        this.valueSchemaName = valueSchemaName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyCoordinate that = (KeyCoordinate) o;
        return partition == that.partition &&
                offset == that.offset &&
                valueSchemaName.equals(that.valueSchemaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueSchemaName, partition, offset);
    }
}
