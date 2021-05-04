/*
Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Based on https://min-api.cryptocompare.com/documentation/websockets?key=Channels&cat=Trade
// SCHEMA of incoming JSON mesages:
//
//  TYPE string Always
//    Type of the message, this is 0 for trade type messages.
//  M string Always
//    The market / exchange you have requested (name of the market / exchange e.g. Coinbase, Kraken, etc.)
//  FSYM string Always
//    The mapped from asset (base symbol / coin) you have requested (e.g. BTC, ETH, etc.)
//  TSYM string Always
//    The mapped to asset (quote/counter symbol/coin) you have requested (e.g. BTC, USD, etc.)
//  F string Always
//    The flag for the trade as a bitmask: &1 for SELL, &2 for BUY, &4 for UNKNOWN and &8 for REVERSED (inverted). A flag of 1 would be a SELL, a flag of 9 would be a SELL + the trade was REVERSED (inverted). We reverse trades when we think the dominant pair should be on the right hand side of the trade. Uniswap for example has ETH trading into a lot of symbols, we record it as the other symbols trading into ETH and we invert the trade. We only use UNKNOWN when the underlying market / exchange API does not provide a side
//  ID string
//    The trade id as reported by the market / exchange or the timestamp in seconds + 0 - 999 if they do not provide a trade id (for uniqueness under the assumption that there would not be more than 999 trades in the same second for exchanges that do not provide a trade id)
//  TS timestamp
//    The timestamp in seconds as reported by the market / exchange or the received timestamp if the market / exchange does not provide one.
//  Q number
//    The from asset (base symbol / coin) volume of the trade (for a BTC-USD trade, how much BTC was traded at the trade price)
//  P number
//    The price in the to asset (quote / counter symbol / coin) of the trade (for a BTC-USD trade, how much was paid for one BTC in USD)
//  TOTAL number
//    The total volume in the to asset (quote / counter symbol / coin) of the trade (it is always Q * P so for a BTC-USD trade, how much USD was paid in total for the volume of BTC traded)
//  RTS timestamp
//    The timestamp in seconds when we received the trade. This varies from a few millisconds from the trade taking place on the market / exchange to a few seconds depending on the market / exchange API options / rate limits
//  CCSEQ number
//    Our internal sequence number for this trade, this is unique per market / exchange and trading pair. Should always be increasing by 1 for each new trade we discover, not in chronological order, only available for a subset of markets / exchanges.
//  TSNS number
//    The nanosecond part of the reported timestamp, only available for a subset of markets / exchanges
//  RTSNS number
//    The nanosecond part of the received timestamp, only available for a subset of markets / exchanges

package com.google.cloud.ranadip.flink.beam.examples;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.Serializable;

public class KafkaBeamRedisDemo {

    /**
     * Filter only "trade" messages (Type = 0)
     * and BUY messages (i.e. 1. FSYM = BTC and TSYM = USD and F == 2
     * or 2. FSYM = USD and TSYM = BTC and F == 10 (BUY, with REVERSED symbols)
     */
    static class FilterUSDBTCTrades implements ProcessFunction<JsonNode, Boolean> {
        @Override
        public Boolean apply(JsonNode node) {
            return node.getType() == 0 &&
                    ((node.getFsym().equals("BTC") && node.getTsym().equals("USD") && node.getF() == 2)
                            ||
                            (node.getFsym().equals("USD") && node.getTsym().equals("BTC") && node.getF() == 10));
        }
    }

    public interface KafkaBeamRedisDemoOptions extends PipelineOptions {

        @Description("Window size in minutes.")
        @Default.Long(1)
        Long getWindowMinutes();
        void setWindowMinutes(Long minutes);

        @Description("Window slide interval in seconds.")
        @Default.Long(20)
        Long getSlideSeconds();
        void setSlideSeconds(Long seconds);

        @Description("Kafka broker string")
        @Default.String("localhost:9092")
        String getKafkaBrokerString();
        void setKafkaBrokerString(String kafkaBrokerString);

        @Description("Kafka topic name")
        @Default.String("crypto-trade")
        String getKafkaTopic();
        void setKafkaTopic(String kafkaTopic);

    }

    public static void main(String[] args) {

        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner you wish to use. This example
        // will run with the DirectRunner by default, based on the class path configured
        // in its dependencies.
        KafkaBeamRedisDemoOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(KafkaBeamRedisDemoOptions.class);

        // Create the Pipeline object with the options we defined above
        Pipeline p = Pipeline.create(options);

        // Concept #1: Use KafkaIO to subscribe to Kafka topic and consume messages

        // This example reads a public data set consisting of the complete works of Shakespeare.
        PCollection<KV<Long, String>> pcoll = p.apply("Read Input", KafkaIO.<Long, String>read()
                        .withBootstrapServers(options.getKafkaBrokerString())
                        .withTopic(options.getKafkaTopic())  // use withTopics(List<String>) to read from multiple topics.
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)

                        // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

                        // Rest of the settings are optional :

                        // you can further customize KafkaConsumer used to read the records by adding more
                        // settings for ConsumerConfig. e.g :
//                .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))

                        // set event times and watermark based on LogAppendTime. To provide a custom
                        // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
//                .withLogAppendTime()

                        // restrict reader to committed messages on Kafka (see method documentation).
//                .withReadCommitted()

                        // offset consumed by the pipeline can be committed back.
//                .commitOffsetsInFinalize()

                        // finally, if you don't need Kafka metadata, you can drop it.g
                        .withoutMetadata() // PCollection<KV<Long, String>>
        );

        PCollection<JsonNode> valuePcoll = pcoll.apply("Sliding Windows",
                Window.into(SlidingWindows.of(Duration.standardMinutes(options.getWindowMinutes()))
                        .every(Duration.standardSeconds(options.getSlideSeconds()))))
                .apply("Extract values", Values.create())
                .apply("Covert to Json Objects", ParDo.of(new DoFn<String, JsonNode>() {
                    @ProcessElement
                    public void convertJsonStrToObj (@Element String input, OutputReceiver<JsonNode> out) {
                        out.output(JsonNode.createJsonNode(input));
                    }
                }));


        PCollection<JsonNode> filteredValuePcoll =
                valuePcoll.apply(Filter.by(new FilterUSDBTCTrades()));

        PCollection<Double> quantityPColl = filteredValuePcoll.apply("Get Volumes",
                ParDo.of(new DoFn<JsonNode, Double>() {
                    @ProcessElement public void getVol (@Element JsonNode input, OutputReceiver<Double> out) {
                        out.output(input.getQ());
                    }
        }));

        PCollection<Double> pricePColl = filteredValuePcoll.apply("Get Price",
                ParDo.of(new DoFn<JsonNode, Double>() {
                    @ProcessElement public void getVol (@Element JsonNode input, OutputReceiver<Double> out) {
                        out.output(input.getP());
                    }
                }));

        PCollection<Double> avgVolPerWindow = quantityPColl.apply("Average Vol", Mean.<Double>globally().withoutDefaults());

        PCollection<Double> maxPricePerWindow = pricePColl.apply("Max Price", Max.<Double>globally().withoutDefaults());

//        maxPricePerWindow.apply("Write Output", TextIO.write().to("tmpOutput"));

        avgVolPerWindow.apply("Convert AvgVol to String", MapElements.into(TypeDescriptor.of(String.class))
                .via(x -> x.toString()))
                .apply("Write Avg vol to Kafka", KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getKafkaBrokerString())
                .withTopic("AvgVol")
                .withValueSerializer(StringSerializer.class)
                .values()
        );

        maxPricePerWindow.apply("Convert MaxPrice to String", MapElements.into(TypeDescriptor.of(String.class))
                .via(x -> x.toString()))
                .apply("Write Max price to Kafka", KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getKafkaBrokerString())
                .withTopic("MaxPrice")
                .withValueSerializer(StringSerializer.class)
                .values()
        );

        p.run().waitUntilFinish();
    }
}

/**
 * JsonNode is a Java bean representation of the input json
 */
class JsonNode implements Serializable {
    @SerializedName("TYPE") int type;
    @SerializedName("F") int f;
    @SerializedName("M") String m;
    @SerializedName("FSYM") String fsym;
    @SerializedName("TSYM") String tsym;
    @SerializedName("ID") long id;
    @SerializedName("TS") long ts;
    @SerializedName("RTS") long rts;
    @SerializedName("TSNS") long tsns;
    @SerializedName("RTSNS") long rtsns;
    @SerializedName("Q") double q;
    @SerializedName("P") double p;
    @SerializedName("TOTAL") double total;

    JsonNode() {}

    public int getType() {
        return type;
    }

    public int getF() {
        return f;
    }

    public String getFsym() {
        return fsym;
    }

    public String getTsym() {
        return tsym;
    }

    public double getQ() {
        return q;
    }

    public double getP() {
        return p;
    }

    public  static JsonNode createJsonNode(String jsonStr) {
        Gson gson = new Gson();
        return gson.fromJson(jsonStr, JsonNode.class);
    }
}