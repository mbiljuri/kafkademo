package com.example.kafkademo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${kafka.input.topic}")
    private String inputTopic;
    @Value("${kafka.input.topic}")
    private String outputTopic;

    @Bean(name = "inputTopic")
    public NewTopic inputTopic() {
        return TopicBuilder.name(inputTopic).partitions(3).replicas(2).build();
    }

    @Bean(name = "outputTopic")
    public NewTopic outputTopic() {
        return TopicBuilder.name(outputTopic).partitions(1).replicas(2).build();
    }
    
}
