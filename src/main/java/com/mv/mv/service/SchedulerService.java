package com.mv.mv.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
public class SchedulerService {

    private final ConsumerService sqsConsumerService;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledTask;

    @Autowired
    public SchedulerService(ConsumerService sqsConsumerService) {
        this.sqsConsumerService = sqsConsumerService;
    }

    public void startSchedule() {
        if (scheduledTask == null || scheduledTask.isCancelled()) {
            scheduledTask = scheduler.scheduleAtFixedRate(
                    sqsConsumerService::consumeMessages,
                    0,
                    30,
                    TimeUnit.SECONDS
            );
            System.out.println("Agendamento ativado.");
        } else {
            System.out.println("O agendamento já está ativo.");
        }
    }

    public void stopSchedule() {
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            scheduledTask.cancel(true);
            System.out.println("Agendamento desativado.");
        } else {
            System.out.println("O agendamento já está desativado.");
        }
    }
}
