# Stream Pipeline

Projeto acadêmico de Stream Pipeline com Apache Spark Structured Streaming. A organização segue a separação por componentes e entregáveis usada como referência no projeto [FIAP-2026-ABD-DataProductManagement](https://github.com/carloszaramella/FIAP-2026-ABD-DataProductManagement).

## Entrega

- Ingestão contínua de arquivos JSON/JSONL na camada `raw`.
- Normalização, tipagem e validação na camada `trusted`, com saída Parquet.
- Agregação na camada `refined`, com janela temporal e saída Parquet.
- Tabela agregada `vehicle_financing_summary` em SQLite para consultas SQL sem dependência nativa adicional.
- MinIO disponível no Docker Compose como opção de object storage.
- Watermark para eventos atrasados e checkpoints separados por camada.
- Testes unitários das transformações.
- Dockerfile e Docker Compose para deploy local.
- Notebook complementar em `../stream_pipeline_spark.ipynb`.

## Estrutura

```text
stream-pipeline/
├── README.md                 # visão geral e execução
├── Dockerfile                # imagem do pipeline
├── docker-compose.yml        # execução containerizada
├── requirements.txt          # dependências Python
├── Makefile                  # comandos repetíveis
├── src/                      # código do pipeline
├── producer/                 # gerador de eventos JSONL
├── tests/                    # testes das transformações
├── data/                     # entrada, saída e checkpoints
└── docs/                     # arquitetura e evento de exemplo
```

## Execução local

```bash
cd stream-pipeline
make install
make test
PIPELINE_RUNTIME_SECONDS=30 make run
```

As consultas escrevem em:

- `data/raw/`: eventos brutos em JSON.
- `data/trusted/`: eventos normalizados em Parquet.
- `data/refined/`: indicadores agregados em Parquet e arquivo SQLite.

O comando `make run` executa `src/medallion_pipeline.py`, que encadeia as três camadas. Cada estágio também pode ser executado isoladamente com `make run-raw`, `make run-trusted` ou `make run-refined`.

Depois de executar o pipeline, consulte o DW local:

```bash
make query-dw
PYTHONPATH=src python src/query_dw.py --query janelas
```

Para gerar uma amostra JSONL independente:

```bash
make producer
```

## Deploy com Docker

```bash
docker compose up --build
```

O tempo de execução pode ser alterado com `PIPELINE_RUNTIME_SECONDS`. O MinIO fica disponível em `http://localhost:9000` e na console `http://localhost:9001`. Para um warehouse compartilhado, PostgreSQL é uma alternativa recomendada e pode ser iniciado com `docker compose --profile warehouse up postgres`.
