package kafka.connect;

import com.google.common.collect.ImmutableMap;
import kafka.connect.transform.Namespacefy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;


public class NamespacefyTest {

    private Namespacefy namespacefy = new Namespacefy();

    @Test
    public void struct() {
        Map config = ImmutableMap.of(
            "record.namespace", "com.corelogic"
        );
        namespacefy.configure(config);

        final Schema inputSchema = SchemaBuilder.struct()
            .field("testField", Schema.STRING_SCHEMA)
            .build();
        final Schema inputKeySchema = SchemaBuilder.struct()
            .build();

        final Struct inputStruct = new Struct(inputSchema)
            .put("testField", "test");
        final Struct inputKeyStruct = new Struct(inputKeySchema);


        final SourceRecord inputRecord = new SourceRecord(
            ImmutableMap.of(
                "partition", 1
            ),
            ImmutableMap.of(
                "offset", 1
            ),
            "testTopic",
            inputKeySchema,
            inputStruct,
            inputSchema,
            inputStruct
        );

        final SourceRecord transformedRecord = (SourceRecord) this.namespacefy.apply(inputRecord);

        Assert.assertTrue(transformedRecord.valueSchema().name().contains("com.corelogic"));
        Struct actualStruct = (Struct)transformedRecord.value();
        Assert.assertEquals(actualStruct.get("testField"), "test");

    }

}
