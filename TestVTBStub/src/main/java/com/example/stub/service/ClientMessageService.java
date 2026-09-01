package com.example.stub.service;

import com.example.stub.dto.ClientMessageDto;
import com.example.stub.entity.ClientMessageEntity;
import com.example.stub.repository.ClientMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClientMessageService {

    private final ClientMessageRepository repository;

    /**
     * Сохранение сообщения из Kafka в БД
     */
    @Transactional
    public ClientMessageEntity saveMessage(ClientMessageDto dto) {
        log.info("📥 saveMessage: msgId={}, inn={}, fullName={}", dto.getMsgId(), dto.getInn(), dto.getFullName());
        
        // Проверка на дубликат через findByMsgId
        log.info("🔍 Проверка на дубликат: findByMsgId({})", dto.getMsgId());
        Optional<ClientMessageEntity> existing = repository.findByMsgId(dto.getMsgId());
        log.info("📋 Результат findByMsgId: {}", existing.isPresent() ? "найдено" : "не найдено");
        
        if (existing.isPresent()) {
            log.warn("Сообщение с msg_id={} уже существует в БД", dto.getMsgId());
            return existing.get();
        }

        // Создаём entity из DTO
        log.info("🏗 Создание entity: msgId={}, fullName={}, inn={}", dto.getMsgId(), dto.getCleanFullName(), dto.getInn());
        ClientMessageEntity entity = new ClientMessageEntity(
                dto.getMsgId(),
                dto.getCleanFullName(), // Убираем пробелы
                dto.getInn()
        );
        log.info("📝 Entity время: {}", entity.getTime());

        // Сохраняем в БД
        log.info("💾 Сохранение в БД...");
        ClientMessageEntity saved = repository.save(entity);
        log.info("✅ Сохранено: id={}, msgId={}, full_name={}, inn={}, time={}",
                saved.getId(),
                saved.getMsgId(),
                saved.getFullName(),
                saved.getInn(),
                saved.getTime()
        );

        return saved;
    }
}