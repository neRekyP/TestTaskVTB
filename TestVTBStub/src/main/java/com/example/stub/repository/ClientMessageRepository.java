package com.example.stub.repository;

import com.example.stub.entity.ClientMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ClientMessageRepository extends JpaRepository<ClientMessageEntity, Long> {

    /**
     * Поиск по msg_id
     */
    Optional<ClientMessageEntity> findByMsgId(String msgId);

    /**
     * Проверка существования по msg_id
     */
    boolean existsByMsgId(String msgId);
}