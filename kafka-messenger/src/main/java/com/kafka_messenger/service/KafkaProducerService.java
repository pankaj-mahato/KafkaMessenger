package com.kafka_messenger.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes string messages to the configured Kafka topic.
 */
@Service
public class KafkaProducerService {

	private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

	/** Topic used for local messenger practice. */
	public static final String TOPIC_NAME = "messenger-topic";

	private final KafkaTemplate<String, String> kafkaTemplate;

	public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	/**
	 * Sends the given message to {@link #TOPIC_NAME}.
	 *
	 * @param message payload to publish
	 */
	public void sendMessage(String message) {
		log.info("Publishing message to topic '{}': {}", TOPIC_NAME, message);
		kafkaTemplate.send(TOPIC_NAME, message);
	}
}
