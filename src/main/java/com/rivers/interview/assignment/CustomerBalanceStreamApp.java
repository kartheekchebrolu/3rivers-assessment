package com.rivers.interview.assignment;

import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.time.Duration;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class CustomerBalanceStreamApp {

    public Topology buildTopology(Properties allProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String customerTopic = allProps.getProperty("customer.topic.name");
        final String rekeyedCustomerTopic = allProps.getProperty("rekeyed.customer.topic.name");
        final String balanceTopic = allProps.getProperty("balance.topic.name");
        final String customerBalanceTopic = allProps.getProperty("customer.balance.topic.name");
        final CustomerBalanceJoiner joiner = new CustomerBalanceJoiner();

        KStream<String, Customer> customerStream = builder.<String, Customer>stream(customerTopic)
                .map((key, customer) -> new KeyValue<>(String.valueOf(customer.getAccountId()), customer));

        customerStream.to(rekeyedCustomerTopic);

        KTable<String, Customer> customers = builder.table(rekeyedCustomerTopic);

        KStream<String, Transaction> transactions = builder.<String, Transaction>stream(balanceTopic)
                .map((key, transaction) -> new KeyValue<>(String.valueOf(transaction.getAccountId()), transaction));

        KStream<String, CustomerBalance> customerBalanceKStream = transactions.join(customers, joiner);

        customerBalanceKStream.to(customerBalanceTopic, Produced.with(Serdes.String(), customerBalanceSpecificAvroSerde(allProps)));

        return builder.build();
    }

    private SpecificAvroSerde<CustomerBalance> customerBalanceSpecificAvroSerde(Properties allProps) {
        SpecificAvroSerde<CustomerBalance> movieAvroSerde = new SpecificAvroSerde<>();
        movieAvroSerde.configure((Map)allProps, false);
        return movieAvroSerde;
    }

    public Properties loadEnvProperties(URL fileName) throws IOException {
        Properties allProps = new Properties();
        FileInputStream input = new FileInputStream(fileName.getFile());
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws Exception {
        CustomerBalanceStreamApp ts = new CustomerBalanceStreamApp();
        ClassLoader classLoader = CustomerBalanceStreamApp.class.getClassLoader();

        Properties allProps = ts.loadEnvProperties(classLoader.getResource("application.properties"));
        allProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        allProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        Topology topology = ts.buildTopology(allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}