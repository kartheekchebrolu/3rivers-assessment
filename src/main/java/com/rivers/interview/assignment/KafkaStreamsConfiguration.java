
package com.rivers.interview.assignment;

import java.util.HashMap;
import java.util.Map;

import com.rivers.interview.assignment.model.Balance;
import com.rivers.interview.assignment.model.Customer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

    @Autowired private KafkaProperties kafkaProperties;

    private static final String BALANCE_TOPIC = "Balance";
    private static final String CUSTOMER_TOPIC = "Customer";
    private static final String CUSTOMERBALANCE_TOPIC = "CustomerBalance";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new JsonSerde<>().getClass());
        props.put("schema.registry.url", "localhost:8081");
        return new StreamsConfig(props);
    }

    @Bean
    public KStream<String, Customer> kStreamJson(StreamsBuilder builder) {
        KStream<String, Balance> balanceStream = builder.stream(BALANCE_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Balance.class)));
        KStream<String, Customer> customerStream = builder.stream(CUSTOMER_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(Customer.class)));
        customerStream.to(CUSTOMERBALANCE_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Customer.class)));
        return customerStream;
    }

}