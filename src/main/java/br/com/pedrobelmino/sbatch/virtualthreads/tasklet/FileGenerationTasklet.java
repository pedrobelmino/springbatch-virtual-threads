package br.com.pedrobelmino.sbatch.virtualthreads.tasklet;

import br.com.pedrobelmino.sbatch.virtualthreads.domain.CustomerXml;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class FileGenerationTasklet implements Tasklet {

    @Value("${app.batch.input-dir}")
    private String inputDir;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Path inputPath = Paths.get(inputDir);

        // Limpa o diretório de entrada
        if (Files.exists(inputPath)) {
            System.out.println(">>> Limpando o diretório de entrada: " + inputDir);
            FileSystemUtils.deleteRecursively(inputPath);
        }

        // Recria o diretório
        Files.createDirectories(inputPath);
        
        JAXBContext context = JAXBContext.newInstance(CustomerXml.class);
        Marshaller marshaller = context.createMarshaller();
        // Configura para escrever fragmentos (sem cabeçalho XML para cada item)
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false); // Desliga formatação para economizar espaço em disco

        // Formato do timestamp para o nome do arquivo
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        
        // Gerar 2 arquivos para o exemplo (1 milhão cada demora um pouco)
        int numberOfFiles = 30;
        int recordsPerFile = 1_000_000;

        System.out.println(">>> Iniciando geração de " + numberOfFiles + " arquivos com " + recordsPerFile + " registros cada.");

        for (int i = 1; i <= numberOfFiles; i++) {
            String fileName = String.format("customers_%s_%d.xml", timestamp, i);
            Path filePath = inputPath.resolve(fileName);
            
            try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                // Escreve o cabeçalho e a tag raiz
                writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
                writer.write("<customers>\n");

                for (int j = 1; j <= recordsPerFile; j++) {
                    CustomerXml customer = new CustomerXml((long) j, "User " + j, "user" + j + "@mail.com", new BigDecimal(j * 100));
                    
                    // Marshal do objeto para o writer como fragmento
                    marshaller.marshal(customer, writer);
                    writer.write("\n");
                    
                    if (j % 100_000 == 0) {
                        System.out.println("Arquivo " + i + ": Gerados " + j + " registros...");
                    }
                }

                // Fecha a tag raiz
                writer.write("</customers>");
            }
            System.out.println(">>> Arquivo gerado: " + filePath);
        }
        
        return RepeatStatus.FINISHED;
    }
}
