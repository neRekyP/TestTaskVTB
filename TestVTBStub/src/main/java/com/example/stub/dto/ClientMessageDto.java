package com.example.stub.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientMessageDto {

    @JsonProperty("msg_id")
    private String msgId;

    @JsonProperty("full_name")
    private String fullName;

    @JsonProperty("inn")
    private String inn;

    /**
     * Метод для получения очищенного ФИО (без лишних пробелов)
     */
    public String getCleanFullName() {
        return fullName != null ? fullName.trim() : null;
    }

    /**
     * Проверка валидности ИНН (11 или 12 цифр)
     */
    public boolean isValidInn() {
        return inn != null && inn.matches("^\\d{11}$|^\\d{12}$");
    }
}