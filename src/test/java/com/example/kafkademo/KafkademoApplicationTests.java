package com.example.kafkademo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.event.annotation.BeforeTestClass;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.example.kafkademo.api.service.KafkaService;
import com.example.kafkademo.model.VehiclePosition;

@SpringBootTest
@RunWith(SpringRunner.class)
public class KafkademoApplicationTests {

	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@Autowired
	private KafkaService kafkaService;

	@DynamicPropertySource
	static void dataSourceProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
	}

	@BeforeTestClass
	public void beforeTest() {
		kafkaListenerEndpointRegistry.getListenerContainers().forEach(
				messageListenerContainer -> {
					ContainerTestUtils
							.waitForAssignment(messageListenerContainer, 1);

				});
	}

	@Test
	public void checkDistanceCalculation() throws Exception {
		VehiclePosition vehiclePos = new VehiclePosition(6.0, 8.0, 10L);
		kafkaService.sendVehiclePosition(vehiclePos);
		TimeUnit.SECONDS.sleep(5);
		assertEquals(kafkaService.getDistances().get(vehiclePos.getId()).getDistance(), 10.0);
	}

}
