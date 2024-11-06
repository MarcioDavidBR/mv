package com.mv.mv.sqs;

import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.util.List;

public class SQSProcessor {

    @Value("${aws.sqs.queueUrl}")
    private static final String QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/575108923526/Fila.fifo";
    @Value("${aws.s3.bucketName}")
    private static final String BUCKET_NAME = "bucketmv";

    public static void main(String[] args) {
        // Inicializar o cliente SQS
        SqsClient sqsClient = SqsClient.builder().build();

        // Inicializar o cliente S3
        S3Client s3Client = S3Client.builder().build();

        try {
            // Receber mensagens da fila
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(QUEUE_URL)
                    .maxNumberOfMessages(5)
                    .waitTimeSeconds(10)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            for (Message message : messages) {
                // Lógica de processamento
                String processedData = processMessage(message.body());

                // Upload do resultado para o S3
                String fileName = "resultado_" + System.currentTimeMillis() + ".txt";
                s3Client.putObject(PutObjectRequest.builder()
                                .bucket(BUCKET_NAME)
                                .key(fileName)
                                .build(),
                        RequestBody.fromString(processedData));

                System.out.println("Arquivo enviado para o S3: " + fileName);

                // Deletar a mensagem da fila após o processamento
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteRequest);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Fechar clientes
            sqsClient.close();
            s3Client.close();
        }
    }

    // Método de processamento de mensagens (exemplo simples de conversão)
    private static String processMessage(String message) {
        // Transformar a mensagem para maiúsculas como exemplo de processamento
        return "Dados Processados: " + message.toUpperCase();
    }
}