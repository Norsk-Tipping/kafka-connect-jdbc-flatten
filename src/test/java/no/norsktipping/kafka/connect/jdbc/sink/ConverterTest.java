package no.norsktipping.kafka.connect.jdbc.sink;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.Test;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConverterTest {

    @Test
    public void testAvroUuidLogicalTypeToConnect() throws IOException {
        // 1. Define Avro schema with a UUID logical type
        String avroSchemaJson = "{"
                + "\"type\":\"record\","
                + "\"name\":\"Person\","
                + "\"namespace\":\"com.example\","
                + "\"fields\":["
                + "  {\"name\":\"id\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},"
                + "  {\"name\":\"firstname\", \"type\": \"string\"}"
                + "]}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

        // 2. Create Avro record
        String uuidString = UUID.randomUUID().toString();
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", uuidString);
        record.put("firstname", "Alex");

        /*
         * // 3. Serialize to Avro binary
         * ByteArrayOutputStream out = new ByteArrayOutputStream();
         * GenericDatumWriter<GenericRecord> writer = new
         * GenericDatumWriter<>(avroSchema);
         * BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
         * writer.write(record, encoder);
         * encoder.flush();
         * byte[] avroBytes = out.toByteArray();
         */

        /*
         * // test
         * GenericData avroDataModel = new GenericData();
         * avroDataModel.addLogicalTypeConversion(new Conversions.UUIDConversion()); //
         * Avro's built-in UUID conversion
         * //
         * 
         * // 4. Deserialize from Avro binary to GenericRecord
         * GenericDatumReader<GenericRecord> reader = new
         * GenericDatumReader<>(avroSchema, avroSchema, avroDataModel);
         * BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new
         * ByteArrayInputStream(avroBytes), null);
         * GenericRecord deserialized = reader.read(null, decoder);
         * // 5. Convert to Connect data
         * AvroData avroData = new AvroData(100);
         * SchemaAndValue connect = avroData.toConnectData(avroSchema, deserialized);
         */

        // 3. Set up MockSchemaRegistryClient and KafkaAvroSerializer
        String topic = "test-topic";

        // 5. Configure AvroConverter
        AvroConverter converter = new AvroConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schema.registry.url", "mock://test");
        converterConfig.put("auto.register.schemas", true);
        converterConfig.put("avro.use.logical.type.converters", true);
        converter.configure(converterConfig, false); // false = value converter

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(converterConfig, false);

        // 4. Serialize record to Kafka Avro bytes (with Confluent wire format)
        byte[] avroBytes = serializer.serialize(topic, record);

        // 6. Use AvroConverter to deserialize to Connect data
        Headers headers = new RecordHeaders(); // empty headers for this example
        SchemaAndValue connect = converter.toConnectData(topic, headers, avroBytes);

        // 6. Assertions
        assertNotNull(connect);
        assertTrue(connect.value() instanceof Struct);
        Struct struct = (Struct) connect.value();
        assertEquals(uuidString, struct.getString("id"));
        assertEquals("Alex", struct.getString("firstname"));

        System.out.println("UUID: " + uuidString);
        System.out.println("Avro Schema: " + avroSchema.toString(true));
        System.out.println("Kafka Connect Schema: " + connect.schema());
        System.out.println("Kafka Connect Struct: " + struct);
    }
}