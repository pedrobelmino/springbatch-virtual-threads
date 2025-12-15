# Spring Batch com Virtual Threads (Java 21) vs Platform Threads

Este projeto é uma Prova de Conceito (PoC) desenvolvida para analisar o impacto de performance e consumo de recursos ao utilizar as novas **Virtual Threads** (Project Loom) do Java 21 em um ambiente de processamento em lote com **Spring Batch**, comparando-as com o modelo tradicional de **Thread Pool** (Platform Threads).

O fluxo de trabalho consiste na leitura de grandes volumes de arquivos XML particionados e envio dos dados processados para um tópico Kafka.

## 🚀 Tecnologias Utilizadas

*   **Java 21**: Obrigatório para suporte nativo a Virtual Threads.
*   **Spring Boot 3.3.0**: Suporte aprimorado para Java 21 e Virtual Threads.
*   **Spring Batch**: Orquestração do processamento (Leitura XML -> Processamento -> Escrita Kafka).
*   **Spring Kafka**: Integração com o broker de mensagens.
*   **JAXB**: Parsing de XML (via `StaxEventItemReader`).
*   **Docker / Docker Compose**: Infraestrutura para o Kafka e Zookeeper.

## ⚙️ Arquitetura e Configuração

### Estrutura do Job
O Job é composto por dois passos principais:
1.  **FileGenerationTasklet**: Gera massa de dados (arquivos XML) dinamicamente para teste.
2.  **ManagerStep (Particionamento)**: Utiliza um `MultiResourcePartitioner` para distribuir os arquivos XML entre threads.
    *   **WorkerStep**: Lê o arquivo XML (Stax), processa e envia para o Kafka.

### Arquivos Importantes

*   **`pom.xml`**:
    *   Define a versão do Java como `21`.
    *   Utiliza o `spring-boot-starter-parent` versão `3.3.0`.
    *   Dependências de `spring-batch`, `spring-kafka` e `jaxb`.

*   **`application.properties`**:
    *   `spring.threads.virtual.enabled=true`: A flag mágica do Spring Boot 3.2+ que habilita VTs nos executores padrões (Tomcat, TaskExecutors gerenciados).
    *   `spring.datasource.hikari.maximum-pool-size`: Ajustado para suportar a alta concorrência gerada pelas Virtual Threads (evitando *connection starvation*).

*   **`docker-compose.yml`** (Infraestrutura):
    *   Responsável por subir o **Zookeeper** e o **Kafka** localmente para receber as mensagens produzidas pelo Batch.

*   **`BatchConfig.java`**:
    *   Define dois `TaskExecutor` beans para comparação:
        *   `SimpleAsyncTaskExecutor` com `setVirtualThreads(true)`.
        *   `ThreadPoolTaskExecutor` (Tradicional) com pool fixo.

## 📊 Cenários de Teste e Resultados

O objetivo dos testes foi estressar o mecanismo de I/O (leitura de disco e escrita de rede no Kafka) para verificar onde as Virtual Threads oferecem ganho.

### Cenário 1: Carga Média
*   **Volume**: 20 Milhões de itens.
*   **Arquivos**: 20 arquivos XML (1 milhão cada).
*   **Chunk Size**: 1.000.
*   **Concorrência**: Máximo de 20 threads (1 thread por arquivo).

| Tipo de Thread | Tempo Total | Status | Memória | Observação |
| :--- | :--- | :--- | :--- | :--- |
| **Virtual Threads** | **4m 39s** (279s) | COMPLETED | 63% | ~4 segundos mais rápido |
| **Platform Threads** | 4m 43s (283s) | COMPLETED | 63% | Pool limitado a 10 threads |

---

### Cenário 2: Carga Alta (Chunk Otimizado)
*   **Volume**: 30 Milhões de itens.
*   **Arquivos**: 30 arquivos XML (1 milhão cada).
*   **Chunk Size**: 100.000 (Otimizado para throughput).
*   **Concorrência**: Máximo de 30 threads (Virtual) vs 20 threads (Pool).

| Tipo de Thread | Tempo Total | Status | Memória | Observação |
| :--- | :--- | :--- | :--- | :--- |
| **Virtual Threads** | **6m 50s** (410s) | COMPLETED | 85% | Ligeira vantagem no tempo |
| **Platform Threads** | 6m 54s (414s) | COMPLETED | 85% | Pool saturado |

## 📝 Conclusão e Parecer Final

Após a execução dos benchmarks comparativos, observou-se o seguinte comportamento:

1.  **Paridade em Baixa Escala**: Com poucos arquivos e poucas threads, a diferença entre Virtual Threads e Platform Threads é quase imperceptível. O overhead de gerenciamento de threads do SO não é o gargalo principal nesses casos.

2.  **Vantagem na Escalabilidade**: A diferença começa a aparecer (ainda que sutil neste tipo de workload misto CPU/IO) quando aumentamos a quantidade de arquivos e a necessidade de concorrência.
    *   As **Virtual Threads** permitiram escalar o número de "workers" (threads de processamento) para igualar o número de arquivos (30 threads para 30 arquivos) sem custo significativo de memória ou CPU para o contexto da thread.
    *   As **Platform Threads** ficam limitadas ao tamanho do Pool (ex: 20). Se houver 30 arquivos, 10 ficarão em fila esperando uma thread liberar.

3.  **Gargalos Externos**: É importante notar que, ao usar Virtual Threads, o gargalo rapidamente deixa de ser a CPU/Thread e passa a ser:
    *   **Banco de Dados**: O pool de conexões (HikariCP) precisa ser ajustado, pois milhares de VTs podem tentar pegar conexões simultaneamente.
    *   **Kafka**: A capacidade de ingestão do broker ou a latência de rede.

**Veredito**: Para jobs de Batch intensivos em I/O (Network/Disk) com alto grau de paralelismo (muitas partições), as Virtual Threads oferecem um modelo de programação mais simples (estilo "uma thread por tarefa") e consomem menos recursos do SO, embora o ganho bruto de tempo dependa estritamente da eliminação de bloqueios de I/O.

## 🛠️ Como Executar

1.  Suba a infraestrutura (Kafka):
    ```bash
    docker-compose up -d
    ```

2.  Compile o projeto (Certifique-se de usar JDK 21):
    ```bash
    mvn clean package -DskipTests
    ```

3.  Execute a aplicação:
    ```bash
    java -jar target/batch-vt-kafka-0.0.1-SNAPSHOT.jar
    ```

---
*Desenvolvido para fins de estudo sobre Java 21 e Spring Batch.*# springbatch-virtual-threads
# springbatch-virtual-threads
