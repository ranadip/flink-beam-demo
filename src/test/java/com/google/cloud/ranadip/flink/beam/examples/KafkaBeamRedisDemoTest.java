package com.google.cloud.ranadip.flink.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class KafkaBeamRedisDemoTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    static class ParGetFsym extends DoFn<JsonNode, String> {
        @DoFn.ProcessElement public void getFsym (@DoFn.Element JsonNode in, OutputReceiver<String> out) {
            out.output(in.getFsym());
        }
    }

    @Test
    public void FilterTest() throws Exception {

        final String[] JsonArray = new String[] {
                "{\"TYPE\":\"0\",\"M\":\"Coinbase\",\"FSYM\":\"BTC\",\"TSYM\":\"USD\",\"F\":\"1\",\"ID\":\"155073841\",\"TS\":1618335982,\"Q\":0.0001,\"P\":63240,\"TOTAL\":6.324,\"RTS\":1618335983,\"TSNS\":414000000,\"RTSNS\":615000000}",
                "{\"TYPE\":\"0\",\"M\":\"Coinbase\",\"FSYM\":\"BTC\",\"TSYM\":\"USD\",\"F\":\"1\",\"ID\":\"155073843\",\"TS\":1618335983,\"Q\":0.00480162,\"P\":63239.88,\"TOTAL\":303.6538726056,\"RTS\":1618335983,\"TSNS\":471000000,\"RTSNS\":615000000}",
                "{\"TYPE\":\"16\",\"MESSAGE\":\"SUBSCRIBECOMPLETE\",\"SUB\":\"0~Coinbase~BTC~USD\"}",
                "{\"TYPE\":\"3\",\"MESSAGE\":\"LOADCOMPLETE\",\"INFO\":\"All your valid subs have been loaded.\"}"
        };

        final List<JsonNode> nodeList = Arrays.stream(JsonArray).map(s -> JsonNode.createJsonNode(s)).collect(Collectors.toList());

        // Create an input PCollection.
        PCollection<JsonNode> input = testPipeline.apply(Create.of(nodeList));

        PCollection<JsonNode> filterInput = input.apply(Filter.by(new KafkaBeamRedisDemo.FilterUSDBTCTrades()));
        PCollection<String> output = filterInput.apply(ParDo.of(new ParGetFsym()));
        output.setCoder(NullableCoder.of(StringUtf8Coder.of()));
        PAssert.that(output)
                .containsInAnyOrder("BTC", "BTC");

        // Run the pipeline.
        testPipeline.run();

    }

    @Test
    public void GetFsymTest() throws Exception {
        final String jsonStr = "{\"TYPE\":\"0\",\"M\":\"Coinbase\",\"FSYM\":\"BTC\",\"TSYM\":\"USD\",\"F\":\"1\",\"ID\":\"155073841\",\"TS\":1618335982,\"Q\":0.0001,\"P\":63240,\"TOTAL\":6.324,\"RTS\":1618335983,\"TSNS\":414000000,\"RTSNS\":615000000}";
        final JsonNode jsonNode = JsonNode.createJsonNode(jsonStr);

        Assert.assertEquals("BTC", jsonNode.getFsym());
    }

    @Test
    public void WordCountTest() {

        // Our static input data, which will make up the initial PCollection.
        final String[] WORDS_ARRAY = new String[] {
                "hi", "there", "hi", "hi", "sue", "bob",
                "hi", "sue", "", "", "ZOW", "bob", ""};

        final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
        // Create an input PCollection.
        PCollection<String> input = testPipeline.apply(Create.of(WORDS));

        // Apply the Count transform under test.
        PCollection<KV<String, Long>> output =
                input.apply(Count.<String>perElement());

        // Assert on the results.
        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("hi", 4L),
                        KV.of("there", 1L),
                        KV.of("sue", 2L),
                        KV.of("bob", 2L),
                        KV.of("", 3L),
                        KV.of("ZOW", 1L));

        // Run the pipeline.
        testPipeline.run();
    }
}