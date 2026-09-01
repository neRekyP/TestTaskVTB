package com.example.stub.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "messages")
public class ClientMessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "msg_id", nullable = false, unique = true, length = 36)
    private String msgId;

    @Column(name = "full_name", nullable = false, length = 255)
    private String fullName;

    @Column(name = "inn", nullable = false, length = 12)
    private String inn;

    @Column(name = "time", nullable = false)
    private LocalDateTime time;

    /**
     * Конструктор для создания из DTO
     */
    public ClientMessageEntity(String msgId, String fullName, String inn) {
        this.msgId = msgId;
        this.fullName = fullName;
        this.inn = inn;
        this.time = LocalDateTime.now(); // Текущее время
    }
}