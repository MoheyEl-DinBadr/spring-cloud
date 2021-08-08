package com.mohey.springcloud;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableKafkaStreams
public class SpringCloudApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudApplication.class, args);
    }

    @Bean
    public NewTopic hobbit(){
        //TODO check Why it doesn't work
        return TopicBuilder
                .name("hobbit")
                .partitions(3)
                .replicas(3)
                .build();
    }
    @Bean
    public NewTopic counts(){
        //TODO check Why it doesn't work
        return TopicBuilder
                .name("streams-wordcount-output")
                .partitions(3)
                .replicas(3)
                .build();
    }

    @RequiredArgsConstructor
    @Component
    class Producer{
        ExecutorService exec = Executors.newFixedThreadPool(2);
        private final KafkaTemplate<Integer, String> template;
        Faker faker;

        @EventListener(ApplicationStartedEvent.class)
        public void generate(){
            faker = Faker.instance();

            final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
            
            final Flux<String> hobbitQuotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

            final Flux<String> howIMetYourMotherQuotes = Flux.fromStream(Stream.generate(() -> faker.howIMetYourMother().quote()));

            exec.execute(() ->{
                Flux.zip(interval, hobbitQuotes).
                        map(it -> template.send("hobbit",
                                faker.random().nextInt(42),
                                it.getT2())).blockLast();
            });

            exec.execute(()->{
                Flux.zip(interval, howIMetYourMotherQuotes).
                        map(it -> template.send("howIMetYourMother",
                                faker.random().nextInt(42),
                                it.getT2())).blockLast();
            });

        }
    }


    @Component
    class Consumer{

        @KafkaListeners(
                @KafkaListener(topics = {"howIMetYourMother", "hobbit"},
                        groupId = "spring-boot-kafka",
                containerFactory = "integerKafkaListenerContainerFactory")

        )
        public void consume(ConsumerRecord<Integer, String> consumedRecord){
            System.out.println("Received: " +
                    "\n\tKey: " + consumedRecord.key()+
                    "\n\tValue: " + consumedRecord.value()+
                    "\n\tTopic: " + consumedRecord.topic()+
                    "\n\tDate: " + new Date(consumedRecord.timestamp()));
        }

        @KafkaListener(topics = {"streams-wordcount-output"},
                groupId = "spring-boot-kafka",
        containerFactory = "longKafkaListenerContainerFactory")
        public void consumeStreams(ConsumerRecord<String, Long> record){

            System.out.println("Key Class: " + record.key().getClass());
            System.out.println("Value Class: " + record.value().getClass());
            System.out.println("Received: " +
                    "\n\tKey: " + record.key()+
                    "\n\tValue: " + record.value()+
                    "\n\tTopic: " + record.topic()+
                    "\n\tDate: " + new Date(record.timestamp()));
        }
    }


    @Component
    class Processor{

        @Autowired
        public void process(StreamsBuilder builder){

            final Serde<Integer> integerSerde = Serdes.Integer();
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Long> longSerde = Serdes.Long();


            KStream<Integer, String> textLines =
                    builder
                            .stream("howIMetYourMother", Consumed.with(integerSerde, stringSerde));

            KTable<String, Long> wordCounts = textLines
                    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                    .groupBy((key, value) -> value, Grouped.with(stringSerde,stringSerde))
                    .count(Materialized.as("counts"));

            wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));


        }
    }


    @RestController
    @RequiredArgsConstructor
    class RestService{
        private final StreamsBuilderFactoryBean factoryBean;

        @GetMapping("/count/{word}")
        public Long getCount(@PathVariable String word){
            final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

            final ReadOnlyKeyValueStore<String, Long> counts =
                    kafkaStreams.store(
                            StoreQueryParameters
                                    .fromNameAndType("counts"
                                            ,QueryableStoreTypes.keyValueStore()));

            ;
            return counts.get(word);
        }
    }
}
