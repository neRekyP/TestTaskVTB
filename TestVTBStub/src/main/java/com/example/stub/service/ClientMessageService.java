package com.example.stub.service;

import com.example.stub.dto.ClientMessageDto;
import com.example.stub.entity.ClientMessageEntity;
import com.example.stub.repository.ClientMessageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ClientMessageService {

    private final ClientMessageRepository repository;

    /**
     * Сохранение сообщения из Kafka в БД
     */
    @Transactional
    public ClientMessageEntity saveMessage(ClientMessageDto dto) {
        Optional<ClientMessageEntity> existing = repository.findByMsgId(dto.getMsgId());
        
        if (existing.isPresent()) {
            return existing.get();
        }

        ClientMessageEntity entity = new ClientMessageEntity(
                dto.getMsgId(),
                dto.getCleanFullName(),
                dto.getInn()
        );

        return repository.save(entity);
    }
}