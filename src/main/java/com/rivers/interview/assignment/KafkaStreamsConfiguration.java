package com.rivers.interview.assignment;

import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaStreamsConfiguration {

    private static final String APP_NAME = "StreamJoinApp";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REG_URL = "http://localhost:8081";
    private static final String AVRO_READER_CONFIG = "true";
    public static final String CUSTOMER_TOPIC = "Customer";
    public static final String BALANCE_TOPIC = "Balance";
    public static final String CUSTOMER_BALANCE_TOPIC = "CustomerBalance";
    public static final String CUSTOMER_REKEY_TOPIC = "KeyCustomer";



    public Properties loadProperties() {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REG_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, AVRO_READER_CONFIG);
        return props;
    }

    private  Topology getTopology(Properties properties) {
        final Map<String, String> serdeConfig = Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG,
                SCHEMA_REG_URL);

        Serde<CustomerBalance> customerBalanceSerde = new SpecificAvroSerde<>();
        customerBalanceSerde.configure(serdeConfig, false);

        Serdes.StringSerde stringSerde = new Serdes.StringSerde();

        final Serde<Customer> customerSpecificAvroSerde = new SpecificAvroSerde<>();
        customerSpecificAvroSerde.configure(serdeConfig, false);

        final Serde<Transaction> transactionSpecificAvroSerde = new SpecificAvroSerde<>();
        transactionSpecificAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Customer> customerStream =
                builder.<String, Customer>stream(CUSTOMER_TOPIC).map((key, customer) -> new KeyValue<>(String.valueOf(customer.getAccountId()), customer));
        customerStream.to(CUSTOMER_REKEY_TOPIC);
        KTable<String, Customer> customerKTable = builder.table(CUSTOMER_REKEY_TOPIC);

        KStream<String, Transaction> transactionStream =
                builder.<String, Transaction>stream(BALANCE_TOPIC).map((key, transaction) -> new KeyValue<>(String.valueOf(transaction.getAccountId()), transaction));
        KStream<String, CustomerBalance> customerBalanceStream =
                transactionStream.join(customerKTable, new CustomerBalanceJoiner());

        customerBalanceStream.to(CUSTOMER_BALANCE_TOPIC, Produced.with(stringSerde, customerBalanceSerde));
        return builder.build();
    }
    public void join() throws InterruptedException {

        final Properties properties = loadProperties();
        KafkaStreams streams = new KafkaStreams(getTopology(properties), properties);
        streams.start();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaStreamsConfiguration kafkaStreamsConfiguration = new KafkaStreamsConfiguration();
        kafkaStreamsConfiguration.join();
    }
}