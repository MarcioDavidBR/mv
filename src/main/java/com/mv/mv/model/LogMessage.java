package com.mv.mv.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class LogMessage {

    @JsonProperty("timestamp")
    private LocalDateTime timestamp;

    @JsonProperty("level")
    private String level;

    @JsonProperty("message")
    private String message;

    @JsonProperty("transactionId")
    private String transactionId;

}