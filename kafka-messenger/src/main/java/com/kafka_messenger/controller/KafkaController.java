package com.kafka_messenger.controller;

import com.kafka_messenger.service.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints for publishing messages to Kafka.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

	private final KafkaProducerService kafkaProducerService;

	public KafkaController(KafkaProducerService kafkaProducerService) {
		this.kafkaProducerService = kafkaProducerService;
	}

	/**
	 * Publishes a message to Kafka.
	 * <p>
	 * Accepts either a query parameter {@code message} or a plain-text request body.
	 * Query parameter takes precedence when both are provided.
	 *
	 * @param messageParam optional query parameter payload
	 * @param messageBody  optional request body payload
	 * @return confirmation response
	 */
	@PostMapping("/publish")
	public ResponseEntity<String> publish(
			@RequestParam(value = "message", required = false) String messageParam,
			@RequestBody(required = false) String messageBody) {

		String message = messageParam != null ? messageParam : messageBody;

		if (message == null || message.isBlank()) {
			return ResponseEntity.badRequest()
					.body("Message is required. Provide ?message=... or a request body.");
		}

		kafkaProducerService.sendMessage(message);
		return ResponseEntity.ok("Message published to Kafka topic '"
				+ KafkaProducerService.TOPIC_NAME + "': " + message);
	}
}
