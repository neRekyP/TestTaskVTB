package com.example.stub.consumer;

import com.example.stub.dto.ClientMessageDto;
import com.example.stub.entity.ClientMessageEntity;
import com.example.stub.service.ClientMessageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ClientKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final ClientMessageService messageService;
    // private int retryCount = 0;
    // private static final int MAX_RETRIES = 3;

    @KafkaListener(
            topics = "${app.kafka.topic.input}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            // Десериализация JSON в DTO
            ClientMessageDto message = objectMapper.readValue(
                    record.value(),
                    ClientMessageDto.class
            );

            // Валидация
            if (!validateMessage(message)) {
                throw new IllegalArgumentException("Валидация не пройдена");
            }

            // Сохранение в БД
            ClientMessageEntity savedEntity = messageService.saveMessage(message);

            // Подтверждение обработки
            acknowledgment.acknowledge();
            // retryCount = 0;

        } catch (Exception e) {
            // retryCount++;
            // 
            // if (retryCount >= MAX_RETRIES) {
            //     acknowledgment.acknowledge();
            //     retryCount = 0;
            // }
        }
    }

    private boolean validateMessage(ClientMessageDto message) {
        if (message.getMsgId() == null || message.getMsgId().isEmpty()) {
            return false;
        }

        if (message.getFullName() == null || message.getFullName().trim().isEmpty()) {
            return false;
        }

        if (!message.isValidInn()) {
            return false;
        }

        return true;
    }
}