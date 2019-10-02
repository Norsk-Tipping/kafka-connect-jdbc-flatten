package io.confluent.connect.jdbc.sink.StreamFlatten;

import org.javatuples.Pair;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;

import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Stream;


class MainProcessingInstruction {
  private final Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> mainContainerFunction;
  private final Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> subProcessingInstructions;
  private final Function<Object, Function<Struct, Struct>> keyPkFieldFunction;
  private final Headers headers;

  public MainProcessingInstruction(Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> mainContainerFunction, Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> subProcessingInstructions,
                                   Headers headers, Function<Object, Function<Struct, Struct>> keyPkFieldFunction) {
    this.mainContainerFunction = mainContainerFunction;
    this.subProcessingInstructions = subProcessingInstructions;
    this.headers = headers;
    this.keyPkFieldFunction = keyPkFieldFunction;
  }

  public Function<Stream<Pair<HashMap<String, Object>, Object>>, Stream<Pair<HashMap<String, Object>, Object>>> getMainContainerFunction() {
    return mainContainerFunction;
  }

  public Function<Pair<HashMap<String, Object>, Object>, Stream<Struct>> getSubProcessingInstructions() {
    return subProcessingInstructions;
  }

  public Function<Object, Function<Struct, Struct>> getKeyPkFieldFunction() {
    return keyPkFieldFunction;
  }

  public Headers getHeaders() {
    return headers;
  }
}


