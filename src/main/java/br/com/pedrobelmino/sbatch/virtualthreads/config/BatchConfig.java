package br.com.pedrobelmino.sbatch.virtualthreads.config;

import br.com.pedrobelmino.sbatch.virtualthreads.domain.CustomerJson;
import br.com.pedrobelmino.sbatch.virtualthreads.domain.CustomerXml;
import br.com.pedrobelmino.sbatch.virtualthreads.listener.FileMoveListener;
import br.com.pedrobelmino.sbatch.virtualthreads.listener.JobTimingListener;
import br.com.pedrobelmino.sbatch.virtualthreads.tasklet.FileGenerationTasklet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

@Configuration
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);

    @Value("${app.batch.input-dir}")
    private String inputDir;

    @Value("${app.batch.output-dir}")
    private String outputDir;

    @Qualifier("taskExecVirtualThreads")
    @Bean
    public TaskExecutor taskExecutor() {
        var exec = new SimpleAsyncTaskExecutor("virtualthread-");
        exec.setVirtualThreads(true);
        exec.setConcurrencyLimit(20);
        return exec;
    }

    @Qualifier("taskExecPoolThreads")
    @Bean
    public TaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(20);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setQueueCapacity(100);
        taskExecutor.setThreadNamePrefix("batch-pool-thread-");
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    @StepScope
    public StaxEventItemReader<CustomerXml> xmlReader(@Value("#{stepExecutionContext['fileName']}") String resourceUrl) {
        logger.info("Lendo arquivo: {}", resourceUrl);
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(CustomerXml.class);

        return new StaxEventItemReaderBuilder<CustomerXml>()
                .name("xmlReader")
                .resource(new DefaultResourceLoader().getResource(resourceUrl))
                .addFragmentRootElements("customer") // O leitor deve procurar por 'customer'
                .unmarshaller(marshaller)
                .build();
    }

    @Bean
    public ItemProcessor<CustomerXml, CustomerJson> processor() {
        return item -> new CustomerJson(
                item.id(),
                item.name() != null ? item.name().toUpperCase() : "UNKNOWN",
                item.email(),
                item.amount(),
                LocalDateTime.now().toString()
        );
    }

    @Bean
    @StepScope
    public KafkaItemWriter<Long, CustomerJson> kafkaWriter(KafkaTemplate<Long, CustomerJson> kafkaTemplate) {
        return new KafkaItemWriterBuilder<Long, CustomerJson>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(CustomerJson::customerId)
                .build();
    }

    @Bean
    @StepScope
    public Partitioner partitioner() throws IOException {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner() {
            @Override
            public Map<String, ExecutionContext> partition(int gridSize) {
                logger.info("Iniciando particionamento com grid size: {}", gridSize);
                return super.partition(gridSize);
            }
        };
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("file:" + inputDir + "/*.xml");
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public Step workerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                           KafkaTemplate<Long, CustomerJson> kafkaTemplate, FileMoveListener fileMoveListener) {
        return new StepBuilder("workerStep", jobRepository)
                .<CustomerXml, CustomerJson>chunk(100000, transactionManager) // Chunk maior para performance
                .reader(xmlReader(null))
                .processor(processor())
                .writer(kafkaWriter(kafkaTemplate))
                .listener(fileMoveListener)
                .build();
    }

    @Bean
    public TaskExecutorPartitionHandler partitionHandler(Step workerStep,
                                                         @Qualifier("taskExecPoolThreads") TaskExecutor taskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setGridSize(20);
        handler.setTaskExecutor(taskExecutor);
        handler.setStep(workerStep);
        return handler;
    }

    @Bean
    public Step managerStep(JobRepository jobRepository,
                            Partitioner partitioner,
                            TaskExecutorPartitionHandler partitionHandler) {
        return new StepBuilder("managerStep", jobRepository)
                .partitioner("workerStep", partitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, Step managerStep, FileGenerationTasklet fileGeneratorStep,
                   PlatformTransactionManager transactionManager, JobTimingListener jobTimingListener) {
        
        Step generateFiles = new StepBuilder("generateFiles", jobRepository)
                .tasklet(fileGeneratorStep, transactionManager)
                .build();

        return new JobBuilder("processXmlToKafkaJob", jobRepository)
                .listener(jobTimingListener) // Adiciona o listener ao job
                .start(generateFiles)
                .next(managerStep)
                .build();
    }
}
