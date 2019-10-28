package kafka.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class Namespacefy<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.NAMESPACE,
            ConfigDef.Type.STRING,
            "asia.corelogic.sales.messaging.avro",
            ConfigDef.Importance.HIGH,
            "Fully qualified namespace for the record.");

    private interface ConfigName {
        String NAMESPACE = "record.namespace";
    }

    private static final String PURPOSE = "add namespace to record";

    private String recordNamespace;

    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        recordNamespace = config.getString(ConfigName.NAMESPACE);
    }

    public R apply(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        Schema updatedSchema = makeUpdatedSchema(record.valueSchema(), recordNamespace);
        Schema updatedKeySchema = makeUpdatedSchema(record.keySchema(), recordNamespace);

        final Struct updatedValue = new Struct(updatedSchema);
        final Struct updatedKey = new Struct(updatedKeySchema);

        for (Field field: updatedValue.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        for (Field field: updatedKey.schema().fields()) {
            updatedKey.put(field.name(), value.get(field));
        }

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            updatedKeySchema,
            updatedKey,
            updatedSchema,
            updatedValue,
            record.timestamp()
        );
    }

    private Schema makeUpdatedSchema(Schema schema, String namespace) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        builder.name(namespace+"."+schema.name());
        builder.version(schema.version());
        builder.doc(schema.doc());
        Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        return builder.build();
    }

    public void close() {}

    public ConfigDef config() {
        return CONFIG_DEF;
    }

}