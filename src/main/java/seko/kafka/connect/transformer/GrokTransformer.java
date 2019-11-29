package seko.kafka.connect.transformer;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class GrokTransformer<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();
    static {
        grokCompiler.registerDefaultPatterns();
    }
    public static final String GROK_PATTERN = "GROK_PATTERN";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(GROK_PATTERN, STRING, MEDIUM, "Script for key transformation");

    private Grok grokPattern;

    @Override
    public R apply(R record) {
        String value = String.valueOf(record.value());
        return newRecord(record, grokPattern.match(value).capture());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private R newRecord(R record, Object newValue) {
        Object key = record.key();
        Object value = newValue == null ? record.value() : newValue;

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                key, record.valueSchema(), value, record.timestamp());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        String pattern = config.getString(GROK_PATTERN);
        this.grokPattern = grokCompiler.compile(pattern);
    }
}
