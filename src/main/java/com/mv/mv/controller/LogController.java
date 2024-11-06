package com.mv.mv.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mv.mv.service.ConsumerService;
import com.mv.mv.service.ProducerService;
import com.mv.mv.service.SchedulerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/logs")
public class LogController {

    private static final String BUCKET_NAME = "bucketmv";

    @Autowired
    private S3Client s3Client;

    @Autowired
    private ProducerService producerService;

    @Autowired
    private ConsumerService consumerService;

    @Autowired
    private  SchedulerService schedulerService;

    // Chama producer e popula a fila SQS
    @PostMapping("/create")
    public ResponseEntity<String> createLog() {
        try {
            producerService.sendMessage();
            return ResponseEntity.ok("Log enviado com sucesso!");
        } catch (JsonProcessingException e) {
            return ResponseEntity.status(500).body("Erro ao processar o log: " + e.getMessage());
        }
    }

    // Chama consumer e salva no Bucket do S3
    @PostMapping("/consume")
    public ResponseEntity<String> consumeMessages() {
        try {
            consumerService.consumeMessages();
            return ResponseEntity.ok("Mensagens consumidas e salvas no S3 com sucesso.");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Erro ao consumir mensagens: " + e.getMessage());
        }
    }

    // Endpoint para iniciar o agendamento
    @PostMapping("/start")
    public ResponseEntity<String> startSchedule() {
        try {
            schedulerService.startSchedule();
            return ResponseEntity.ok("Agendamento iniciado com sucesso!");
        } catch (Exception e) {
            String errorMessage = "Erro ao iniciar o agendamento: " + e.getMessage();
            System.err.println(errorMessage);
            return ResponseEntity.status(500).body(errorMessage);
        }
    }

    // Endpoint para parar o agendamento
    @PostMapping("/stop")
    public ResponseEntity<String> stopSchedule() {
        try {
            schedulerService.stopSchedule();
            return ResponseEntity.ok("Agendamento parado com sucesso!");
        } catch (Exception e) {
            String errorMessage = "Erro ao parar o agendamento: " + e.getMessage();
            System.err.println(errorMessage);
            return ResponseEntity.status(500).body(errorMessage);
        }
    }

    // Lista os objetos no S3
    @GetMapping("/logs")
    public List<String> listLogs() {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();

        ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

        // Retorna a lista de nomes dos arquivos (logs) no bucket
        return listResponse.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    // Retorna objeto específico no S3
    @GetMapping("/logs/{fileName}")
    public String getLogContent(@PathVariable String fileName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(fileName)
                .build();

        // Lê o conteúdo do arquivo e converte para uma string
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                s3Client.getObject(getObjectRequest)))) {

            return reader.lines().collect(Collectors.joining("\n"));

        } catch (Exception e) {
            return "Erro ao buscar o log: " + e.getMessage();
        }
    }

}