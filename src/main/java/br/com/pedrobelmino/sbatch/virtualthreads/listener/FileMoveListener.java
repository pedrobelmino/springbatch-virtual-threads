package br.com.pedrobelmino.sbatch.virtualthreads.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Component
public class FileMoveListener implements StepExecutionListener {

    private final String inputDir;
    private final String outputDir;

    public FileMoveListener(@Value("${app.batch.input-dir}") String inputDir,
                            @Value("${app.batch.output-dir}") String outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        Path outputPath = Paths.get(outputDir);
        if (!Files.exists(outputPath)) {
            try {
                Files.createDirectories(outputPath);
            } catch (IOException e) {
                throw new RuntimeException("Não foi possível criar o diretório de saída.", e);
            }
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (stepExecution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode())) {
            String resourceUrl = stepExecution.getExecutionContext().getString("fileName");
            if (resourceUrl != null) {
                try {
                    Path sourcePath = Paths.get(resourceUrl.replace("file:", ""));
                    Path destinationPath = Paths.get(outputDir, sourcePath.getFileName().toString());
                    Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
                    System.out.println(">>> Arquivo movido: " + sourcePath + " para " + destinationPath);
                } catch (IOException e) {
                    e.printStackTrace();
                    // Retorna um status de erro se a movimentação falhar
                    return ExitStatus.FAILED;
                }
            }
        }
        return stepExecution.getExitStatus();
    }
}
