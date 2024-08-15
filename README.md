# kafka-learning

### Nesse repositório se contra os seguintes artefatos

* **kafka-beginner:** Artefatos criados no curso de Kafka
  - condkutor-plataform: Arquivo docker-compose com os serviços para rodar em container: Apache Kafka, Zookeeper, Schema Registry, PostgreSQL, Conduktor Console e Conduktor Monitoring;
  - kafka-basics: Implementações basícas de consumers e producers com as principais configurações;
  - kafka-producer-wikimedia: Implementação de producer do Apache Kafka usando a API Kafka Client. Esse producer consome a stream de mudança recentes da [Wikimedia](https://stream.wikimedia.org/v2/stream/recentchange) e faz a escrita num tópico; 
  - kafka-consumer-opensearch: Implementação do consumer Apache Kafka usando APi Kafka Client. Esse consumer lê as informações de um tópico Kafka e salva no Opensearch/Elasticsearch;
  - kafka-connect: Implementação do Apache Kafka connect, primeiro um connector source para consumir dados da stream de mudanças recentes da [Wikimedia](https://stream.wikimedia.org/v2/stream/recentchange) para postar num tópico, e o segundo Kafka connect Sink para ler dados de um tópico e salvar em um cluster Opensearch; e
  - kafka-stream-wikimedia: Implementação deu um Apache Kafka stream, que lê dados de um tópico Kafka e faz o cálculo de algumas métricas. As metrícas calculadas pela stream Kafka são: Quantidade de alterações feita ou não por bots, Quantidade de alterações por site e de timestamp.
  - kafka-producer-wikimedia-spring: Implementação de producer do Apache Kafka usando Spring Kafka. Esse producer consome a stream de mudança recentes da [Wikimedia](https://stream.wikimedia.org/v2/stream/recentchange) e faz a escrita num tópico;
  - kafka-consumer-opensearch-spring: Implementação do consumer Apache Kafka usando Spring Kafka. Esse consumer lê as informações de um tópico Kafka e salva no Opensearch/Elasticsearch;
