package com.example.stub.repository;

import com.example.stub.entity.ClientMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ClientMessageRepository extends JpaRepository<ClientMessageEntity, Long> {

    @Query(value = "SELECT * FROM messages WHERE msg_id = :msgId", nativeQuery = true)
    Optional<ClientMessageEntity> findByMsgId(@Param("msgId") String msgId);
}