package br.com.pedrobelmino.sbatch.virtualthreads.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
public class JobTimingListener implements JobExecutionListener {

    private LocalDateTime startTime;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = LocalDateTime.now();
        System.out.println(">>> Job iniciado em: " + startTime);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);
        
        System.out.println(">>> Job finalizado em: " + endTime);
        System.out.println(">>> Status: " + jobExecution.getStatus());
        System.out.println(">>> Tempo total de execução: " + duration.toMinutes() + " minutos e " + duration.toSecondsPart() + " segundos (" + duration.toMillis() + " ms)");
    }
}
