package edu.cnam.nfe101;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import java.util.Properties;

public class AssociationsStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AssociationsStreamProcessor.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "associations-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("associations.raw");

        ObjectMapper objectMapper = new ObjectMapper();

        KTable<String, Long> groupedByCity = sourceStream
                .mapValues(value -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(value);
                        String cityName = jsonNode.get("city").asText();
                        return cityName;
                    } catch (Exception e) {
                        logger.error("Error parsing city name from value {}", value, e);
                        return "ERROR";
                    }
                })
                .mapValues(city -> Optional.ofNullable(city).orElse("Unknown"))
                .groupBy((key, city) -> city)
                .count(Materialized.as("city-counts"));

        groupedByCity.toStream()
                .peek((key, value) -> logger.info("City: {} Count: {}", key, value))
                .map((city, count) -> {
                    return new KeyValue<>(city, city + ":" + count);
                })
                .to("associations.grouped.by.city", Produced.with(Serdes.String(), Serdes.String()));

        sourceStream.mapValues(json -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(json);
                String city = jsonNode.get("city").asText();
                String postalCode = jsonNode.get("postalCode").asText();
                String actDomain = jsonNode.get("actDomain").asText();
                return new Association(city, postalCode, actDomain);
            } catch (Exception e) {
                logger.error("Error parsing city and name from value {}", json, e);
                return new Association();
            }
        })
        .filter((key, association) -> association.getCity().toUpperCase().contains("PARIS") || association.getPostalCode().toUpperCase().contains("75"))
        .mapValues(association -> association.getActDomain())
        .groupBy((key, actDomain) -> actDomain)
        .count(Materialized.as("paris-asso-domain-counts"))
        .toStream()
        .mapValues((actDomain, count) -> actDomain + ": " + count)
        .to("paris.associations.grouped.by.act.domain", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}