package com.mv.mv.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class ConsumerService {

    @Value("${aws.sqs.queueUrl}")
    private static final String QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/575108923526/Fila.fifo";
    @Value("${aws.s3.bucketName}")
    private static final String BUCKET_NAME = "bucketmv";

    @Autowired
    private SqsClient sqsClient;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private ObjectMapper objectMapper;

    public void consumeMessages() {
        // Configura a solicitação de recebimento de mensagens da fila
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .maxNumberOfMessages(5)
                .waitTimeSeconds(10)
                .build();

        // Recebe mensagens da fila
        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        for (Message message : messages) {
            try {
                // Salva a mensagem no S3
                saveMessageToS3(message);

                // Deleta a mensagem da fila após o processamento
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteRequest);

                System.out.println("Mensagem processada e deletada: " + message.body());

            } catch (IOException e) {
                System.err.println("Erro ao salvar a mensagem no S3: " + e.getMessage());
            }
        }
    }

    private void saveMessageToS3(Message message) throws IOException {
        // Converte a mensagem para JSON formatado (caso necessário)
        String messageContent = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(message.body());

        // Nome do arquivo baseado no timestamp
        String fileName = "log_" + System.currentTimeMillis() + ".json";

        // Faz o upload da mensagem para o bucket S3
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(fileName)
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(messageContent, StandardCharsets.UTF_8));
    }
}
