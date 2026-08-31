package com.example.stub.service;

import com.example.stub.dto.ClientMessageDto;
import com.example.stub.entity.ClientMessageEntity;
import com.example.stub.repository.ClientMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
        // Проверка на дубликат
        if (repository.existsByMsgId(dto.getMsgId())) {
            log.warn("Сообщение с msg_id={} уже существует в БД", dto.getMsgId());
            return repository.findByMsgId(dto.getMsgId()).orElse(null);
        }

        // Создаём entity из DTO
        ClientMessageEntity entity = new ClientMessageEntity(
                dto.getMsgId(),
                dto.getCleanFullName(), // Убираем пробелы
                dto.getInn()
        );

        // Сохраняем в БД
        ClientMessageEntity saved = repository.save(entity);

        log.info("Сохранено в БД: id={}, msg_id={}, full_name={}, inn={}, time={}",
                saved.getId(),
                saved.getMsgId(),
                saved.getFullName(),
                saved.getInn(),
                saved.getTime()
        );

        return saved;
    }
}