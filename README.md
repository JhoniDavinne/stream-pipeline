# Stream Pipeline

Pipeline de Engenharia de Dados desenvolvido com Apache Spark Structured Streaming,
utilizando arquitetura Medallion (Raw, Trusted e Refined).

O projeto demonstra ingestão, validação, transformação e agregação de eventos
de financiamento de veículos, com processamento incremental e execução
reprodutível através de Docker.

O projeto foi desenvolvido com foco em práticas de Engenharia de Dados, incluindo ingestão de eventos, validação, transformação, agregação, processamento incremental, testes automatizados e containerização do ambiente.

---

## 📌 Visão geral

O pipeline recebe eventos de financiamento de veículos e os processa em diferentes camadas de dados.

```text
                 ┌──────────────────┐
                 │     Producer     │
                 │   Eventos JSON   │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │       RAW        │
                 │ Dados originais  │
                 └────────┬─────────┘
                          │
                    Validação
                          │
                          ▼
                 ┌──────────────────┐
                 │     TRUSTED      │
                 │ Dados validados  │
                 │ e padronizados   │
                 └────────┬─────────┘
                          │
                    Transformação
                          │
                          ▼
                 ┌──────────────────┐
                 │     REFINED      │
                 │ Dados agregados  │
                 │ para análise     │
                 └──────────────────┘
```

A arquitetura segue o conceito de **Medallion Architecture**, no qual os dados evoluem progressivamente de seu estado bruto para dados confiáveis e preparados para consumo analítico.

---

## 🏗️ Arquitetura

### Raw

A camada **Raw** representa os dados recebidos pelo pipeline praticamente em seu formato original.

Objetivos:

* preservar os eventos recebidos;
* permitir reprocessamento;
* manter rastreabilidade;
* separar ingestão da lógica de transformação.

### Trusted

A camada **Trusted** contém os dados após aplicação das regras de qualidade e padronização.

Nesta etapa são aplicados controles como:

* validação de campos;
* validação de tipos;
* validação de valores;
* normalização dos dados;
* separação de registros válidos e inválidos.

### Refined

A camada **Refined** contém dados preparados para análise.

São realizadas operações como:

* agregações;
* cálculo de métricas;
* agrupamentos;
* processamento por janelas de tempo;
* preparação dos dados para consumo analítico.

---

## ⚡ Streaming

O processamento utiliza **Apache Spark Structured Streaming**.

O pipeline trabalha de forma incremental, processando novos eventos conforme eles são disponibilizados.

Entre os recursos utilizados estão:

* Structured Streaming;
* processamento em micro-batches;
* triggers;
* watermark;
* janelas de tempo;
* checkpoints;
* processamento distribuído com Spark.

O uso de watermark permite controlar a chegada de eventos atrasados durante o processamento de dados temporais.

---

## 📂 Estrutura do projeto

```text
stream-pipeline/
│
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── README.md
│
├── producer/
│   └── ...
│
├── src/
│   ├── config.py
│   ├── financing_schema.py
│   ├── medallion_pipeline.py
│   ├── pipeline.py
│   ├── raw.py
│   ├── refined.py
│   ├── schemas.py
│   ├── transformations.py
│   ├── trusted.py
│   └── validate_layers.py
│
├── tests/
│   └── test_transformations.py
│
└── data/
    ├── raw/
    ├── trusted/
    └── refined/
```

---

## 🐳 Execução com Docker

O projeto utiliza Docker para garantir um ambiente de execução reproduzível, evitando a necessidade de configurar manualmente versões específicas de Python, Java e Spark.

### Pré-requisitos

É necessário ter instalado:

* Docker
* Docker Compose

Não é necessário instalar Spark ou Java diretamente no ambiente local para executar o projeto via Docker.

---

## 🚀 Executando o projeto

Clone o repositório:

```bash
git clone https://github.com/carloszaramella/stream-pipeline.git
```

Entre no diretório:

```bash
cd stream-pipeline
```

Construa a imagem:

```bash
docker compose build
```

Execute o pipeline:

```bash
docker compose up stream-pipeline
```

Para reconstruir a imagem e executar:

```bash
docker compose up --build stream-pipeline
```

---

## 🧪 Testes

Os testes automatizados são executados dentro do container Docker.

Execute:

```bash
docker compose run --rm tests
```

Ou, caso o projeto esteja utilizando o target correspondente no `Makefile`:

```bash
make test
```

Os testes validam principalmente as regras de transformação e agregação dos dados.

Exemplos de validações:

* separação de eventos inválidos;
* validação de valores;
* validação de atributos dos veículos;
* agregações por categoria;
* agregações utilizando janelas temporais.

A execução dos testes dentro do Docker garante que eles utilizem o mesmo ambiente de execução do pipeline.

---

## ⚙️ Configuração

O comportamento do pipeline pode ser configurado por variáveis de ambiente.

Exemplo:

```bash
PIPELINE_RUNTIME_SECONDS=30
PIPELINE_ROWS_PER_SECOND=5
```

No Docker Compose, essas configurações são definidas para facilitar a execução da demonstração.

### Principais parâmetros

| Variável                   | Descrição                                               |
| -------------------------- | ------------------------------------------------------- |
| `PIPELINE_RUNTIME_SECONDS` | Tempo de execução do pipeline                           |
| `PIPELINE_ROWS_PER_SECOND` | Quantidade aproximada de eventos produzidos por segundo |
| `PIPELINE_BASE_DIR`        | Diretório base utilizado pelo pipeline                  |

---

## 🗄️ Armazenamento

O projeto utiliza o diretório `data/` para persistência dos dados processados.

```text
data/
├── raw/
├── trusted/
└── refined/
```

O diretório é montado como volume Docker:

```yaml
volumes:
  - ./data:/opt/stream-pipeline/data
```

Dessa forma, os dados produzidos pelo container permanecem disponíveis no ambiente do projeto.

---

## 🪣 MinIO

O projeto também disponibiliza o **MinIO** através do Docker Compose como opção de object storage compatível com S3.

Para iniciar:

```bash
docker compose up minio
```

A configuração disponibiliza:

```text
API:     http://localhost:9000
Console: http://localhost:9001
```

O MinIO está preparado como uma possibilidade de evolução do armazenamento local para object storage.

---

## 🐘 PostgreSQL

O projeto também possui PostgreSQL configurado como serviço opcional para utilização como warehouse.

Para iniciar o PostgreSQL:

```bash
docker compose --profile warehouse up postgres
```

Configurações utilizadas no ambiente de demonstração:

```text
Database: vehicle_financing
User:     dw_user
Password: dw_password
Port:     5432
```

O PostgreSQL é executado através do profile `warehouse`, portanto não é iniciado automaticamente na execução básica do pipeline.

---

## 🛠️ Makefile

O `Makefile` fornece atalhos para as operações mais utilizadas no projeto.

Exemplos:

```bash
make test
```

Executa os testes automatizados.

```bash
make run
```

Executa o pipeline.

Caso esteja utilizando os comandos Docker definidos no projeto:

```bash
make docker-build
make docker-test
make docker-run
```

Esses comandos facilitam a execução do projeto sem a necessidade de memorizar todos os comandos Docker.

---

## 🔄 Fluxo de processamento

O fluxo conceitual do projeto é:

```text
                 Eventos
                    │
                    ▼
              ┌───────────┐
              │  Producer │
              └─────┬─────┘
                    │
                    ▼
              ┌───────────┐
              │    RAW    │
              └─────┬─────┘
                    │
                    ▼
          ┌───────────────────┐
          │ Validação /       │
          │ Qualidade         │
          └─────────┬─────────┘
                    │
                    ▼
              ┌───────────┐
              │  TRUSTED  │
              └─────┬─────┘
                    │
                    ▼
          ┌───────────────────┐
          │ Transformação /   │
          │ Agregação         │
          └─────────┬─────────┘
                    │
                    ▼
              ┌───────────┐
              │  REFINED  │
              └─────┬─────┘
                    │
                    ▼
             Dados analíticos
```

---

## 🧩 Tecnologias

| Tecnologia                 | Utilização                       |
| -------------------------- | -------------------------------- |
| Python                     | Desenvolvimento do pipeline      |
| Apache Spark               | Processamento distribuído        |
| Spark Structured Streaming | Processamento de eventos         |
| PySpark                    | API Python do Spark              |
| Docker                     | Containerização                  |
| Docker Compose             | Orquestração do ambiente local   |
| Pytest                     | Testes automatizados             |
| MinIO                      | Object storage compatível com S3 |
| PostgreSQL                 | Warehouse opcional               |

---

## 🎯 Objetivos técnicos

O projeto demonstra conceitos importantes de Engenharia de Dados:

* ingestão de dados em streaming;
* processamento incremental;
* arquitetura Medallion;
* qualidade e validação de dados;
* transformação de dados com PySpark;
* agregações por janela temporal;
* watermark;
* checkpoint;
* testes automatizados;
* containerização;
* separação entre processamento e armazenamento;
* possibilidade de evolução para object storage e data warehouse.

---

## 🔍 Qualidade e testes

A qualidade dos dados é tratada durante o processamento, evitando que registros inválidos avancem para as camadas posteriores.

Os testes automatizados permitem validar as principais regras de transformação antes da execução do pipeline.

A execução recomendada é:

```bash
docker compose build
docker compose run --rm tests
docker compose up stream-pipeline
```

---

## 📌 Próximas evoluções

O projeto pode evoluir futuramente para:

* utilização efetiva do MinIO como camada de armazenamento;
* integração com um data warehouse PostgreSQL;
* implementação de mais testes de integração;
* CI/CD;
* monitoramento do pipeline;
* observabilidade;
* métricas de processamento;
* tratamento mais avançado de eventos atrasados;
* execução em ambiente cloud.

---

## 👨‍💻 Autor

**Carlos Zaramella**

Projeto desenvolvido para estudo e demonstração de conceitos de Engenharia de Dados, Apache Spark e processamento de dados em streaming.
