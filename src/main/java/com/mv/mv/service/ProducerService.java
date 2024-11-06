package com.mv.mv.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mv.mv.model.LogMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class ProducerService {

    @Value("${aws.sqs.queueUrl}")
    private String queueUrl;
    private String messageGroupId = "defaultGroup";

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;

    public ProducerService(SqsClient sqsClient, ObjectMapper objectMapper) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
    }

    public void sendMessage() throws JsonProcessingException {
        // Cria um exemplo do LogMessage
        LocalDateTime currentDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM");

        LogMessage logMessage = new LogMessage();
        logMessage.setTimestamp(currentDateTime);
        logMessage.setLevel("log");
        logMessage.setMessage("teste");
        logMessage.setTransactionId("transaction id " + currentDateTime.format(formatter));

        // Converte LogMessage para JSON
        String messageBody = objectMapper.writeValueAsString(logMessage);

        // Cria a requisição para enviar a mensagem
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageGroupId(messageGroupId + LocalDateTime.now())
                .build();

        // Envia a mensagem para a fila SQS
        sqsClient.sendMessage(sendMessageRequest);
        System.out.println("Mensagem enviada: " + messageBody);
    }
}
