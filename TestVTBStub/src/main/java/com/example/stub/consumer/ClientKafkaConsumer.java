package com.example.stub.consumer;

import com.example.stub.dto.ClientMessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClientKafkaConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topic.input}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.info("=== Получено сообщение из Kafka ===");
            log.info("Topic: {}, Partition: {}, Offset: {}",
                    record.topic(), record.partition(), record.offset());
            log.info("Key: {}", record.key());
            log.info("Raw Value: {}", record.value());

            // Десериализация JSON в DTO
            ClientMessageDto message = objectMapper.readValue(
                    record.value(),
                    ClientMessageDto.class
            );

            // Валидация данных
            if (!validateMessage(message)) {
                log.warn("Сообщение не прошло валидацию: {}", message.getMsgId());
                acknowledgment.acknowledge(); // Подтверждаем, чтобы не зациклить
                return;
            }

            // Обработка сообщения
            processClientMessage(message);

            // Подтверждение обработки
            acknowledgment.acknowledge();

            log.info("Сообщение успешно обработано: msg_id={}", message.getMsgId());

        } catch (Exception e) {
            log.error("Критическая ошибка обработки сообщения: {}", record.value(), e);
            // Не подтверждаем — сообщение вернётся в очередь
        }
    }

    /**
     * Валидация входящего сообщения
     */
    private boolean validateMessage(ClientMessageDto message) {
        if (message.getMsgId() == null || message.getMsgId().isEmpty()) {
            log.error("Отсутствует msg_id");
            return false;
        }

        if (message.getFullName() == null || message.getFullName().trim().isEmpty()) {
            log.error("Отсутствует или пустой full_name для msg_id={}", message.getMsgId());
            return false;
        }

        if (!message.isValidInn()) {
            log.error("Некорректный ИНН '{}' для msg_id={}",
                    message.getInn(), message.getMsgId());
            return false;
        }

        return true;
    }

    /**
     * Обработка сообщения клиента
     */
    private void processClientMessage(ClientMessageDto message) {
        log.info("=== ОБРАБОТКА ДАННЫХ КЛИЕНТА ===");
        log.info("ID сообщения: {}", message.getMsgId());
        log.info("ФИО: {}", message.getCleanFullName());
        log.info("ИНН: {}", message.getInn());

        // Здесь ваша бизнес-логика:
        // - Сохранение в БД
        // - Отправка в другой сервис
        // - Вызов REST API
        // - Запись в файл

        log.info("================================");
    }
}