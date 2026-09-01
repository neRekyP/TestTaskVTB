package com.example.stub.consumer;

import com.example.stub.dto.ClientMessageDto;
import com.example.stub.entity.ClientMessageEntity;
import com.example.stub.service.ClientMessageService;
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
    private final ClientMessageService messageService;
    private int retryCount = 0;
    private static final int MAX_RETRIES = 3;

    @KafkaListener(
            topics = "${app.kafka.topic.input}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("═══════════════════════════════════════════════════════");
        log.info("📨 Получено сообщение: offset={}, value={}", record.offset(), record.value());
        log.info("═══════════════════════════════════════════════════════");
        
        try {
            // Десериализация JSON в DTO
            log.info("🔄 Десериализация JSON...");
            ClientMessageDto message = objectMapper.readValue(
                    record.value(),
                    ClientMessageDto.class
            );
            log.info("✅ DTO: msgId={}, inn={}, fullName={}", message.getMsgId(), message.getInn(), message.getFullName());

            // Валидация
            log.info("✅ Валидация пройдена");

            // Сохранение в БД
            log.info("💾 Вызов saveMessage...");
            ClientMessageEntity savedEntity = messageService.saveMessage(message);

            if (savedEntity != null) {
                log.info("✅ Сообщение успешно обработано: id={}, msgId={}",
                        savedEntity.getId(), savedEntity.getMsgId());
            }

            // Подтверждение обработки
            acknowledgment.acknowledge();
            log.info("✅ Подтверждение отправлено");
            retryCount = 0; // Сброс счётчика при успехе

        } catch (Exception e) {
            retryCount++;
            log.error("❌ ОШИБКА (попытка {}/{}): {}", retryCount, MAX_RETRIES, e.getMessage(), e);
            
            if (retryCount >= MAX_RETRIES) {
                log.warn("⚠️ Достигнуто максимальное число попыток ({}) — подтверждаю сообщение и пропускаю", MAX_RETRIES);
                acknowledgment.acknowledge();
                retryCount = 0;
            } else {
                log.error("❌ Не подтверждаю сообщение — оно вернётся в очередь");
            }
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
}