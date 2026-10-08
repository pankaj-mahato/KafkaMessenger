package com.kafka_messenger.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Consumes string messages from the messenger Kafka topic.
 */
@Service
public class KafkaConsumerService {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

	/**
	 * Listens to {@code messenger-topic} and prints each received message.
	 *
	 * @param message payload received from Kafka
	 */
	@KafkaListener(topics = KafkaProducerService.TOPIC_NAME, groupId = "messenger-group")
	public void listen(String message) {
		// Standard console output for local practice / verification
		System.out.println("Received message from Kafka: " + message);
		log.info("Consumed message from topic '{}': {}", KafkaProducerService.TOPIC_NAME, message);
	}
}
