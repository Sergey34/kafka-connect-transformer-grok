package seko.kafka.connect.transformer;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GrokTransformerTest {
    private GrokTransformer<SourceRecord> transformer = new GrokTransformer<>();
    private Map<String, Object> config;
    private SourceRecord record;

    @Before
    public void setUp() {
        config = new HashMap<>();
        String event = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";
        record = new SourceRecord(null, null, "topic", 0, null, "key___", null, event);
    }

    @Test
    public void applyWithoutSchemaJs() {
        config.put(GrokTransformer.GROK_PATTERN, "%{COMBINEDAPACHELOG}");
        transformer.configure(config);

        SourceRecord transformed = transformer.apply(record);
        Map<String, Object> result = Requirements.requireMapOrNull(transformed.value(), "");
        Assert.assertEquals(22, result.size());
    }

}