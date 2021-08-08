package com.mohey.springcloud.configuration;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    /*@Bean
    public ProducerFactory<Integer, String> producerFactory(){
        return new DefaultKafkaProducerFactory<>(
                Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        RETRIES_CONFIG,2,
                        BUFFER_MEMORY_CONFIG, 33554432,
                        KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                        VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
        );
    }

    @Bean("KafkaTemplate")
    public KafkaTemplate<Integer, String> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }*/

    public ConsumerFactory<Integer, String> integerConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return new DefaultKafkaConsumerFactory<>(props,
                new IntegerDeserializer(),
                new StringDeserializer());
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String>
    integerKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(integerConsumerFactory());
        return factory;
    }

    public ConsumerFactory<String, Long> longConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new LongDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Long>
    longKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Long> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(longConsumerFactory());
        return factory;
    }
}
