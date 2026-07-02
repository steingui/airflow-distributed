# Calango Investimentos - Pipeline de Ingestão de Marketplace (Airflow Distribuído)

Este repositório contém a implementação do pipeline de dados distribuído para a **Calango Investimentos**, uma fintech focada na compra e venda de créditos de pequenas bandeiras de cartões. Com a recente aquisição de 70% de um marketplace, esta solução visa escalar e migrar o MVP original (que capturava dados de produto via endpoint e estruturava em padrão medalhão) para uma estrutura distribuída e escalável baseada no **Apache Airflow com Celery Executor**.

## Arquitetura do Sistema

A arquitetura do pipeline distribuído é composta pelos seguintes serviços orquestrados via Docker Compose:
- **PostgreSQL**: Banco de dados relacional que atua tanto como o metadata store do Airflow quanto como o repositório de destino (`target_db`) para armazenamento dos dados estruturados de Users, Products, Carts e itens de Carrinho.
- **Redis**: Broker de mensageria de alto desempenho para o Celery, gerenciando a fila de tarefas enviadas para execução distribuída.
- **Airflow Webserver**: Interface web para gerenciamento e monitoramento manual de DAGs.
- **Airflow Scheduler**: Agendador que decide quando as tarefas de cada DAG devem rodar.
- **Airflow Workers**: Agentes executores que escutam as filas do Redis e processam as tasks paralelamente.
- **Flower**: Interface de monitoramento web em tempo real para os workers do Celery (acessível na porta 5555).

---

## Estrutura de Pastas

```bash
airflow-distributed/
├── dags/
│   └── fakestore_pipeline.py  # DAG principal utilizando TaskFlow API
├── logs/                      # Pasta mapeada para logs de execução
├── plugins/                   # Pasta para plugins customizados
├── .gitignore                 # Arquivos ignorados pelo Git
├── docker-compose.yml         # Definição dos containers do cluster
└── README.md                  # Este documento explicativo
```

---

## Passos para Execução e Escalonamento Dinâmico

### Passo 1: Inicializar o Cluster com Exatamente 3 Workers
Para subir todos os serviços e fixar a quantidade de workers em **3**, execute o comando:
```bash
docker compose up -d --scale airflow-worker=3
```

### Passo 2: Interromper e Destruir os Containers (Limpeza Completa)
Para simular a interrupção completa e destruição da infraestrutura dos workers e demais serviços (liberando volumes, se necessário), execute:
```bash
docker compose down -v
```

### Passo 3: Iniciar o Cluster com 5 Workers Dinamicamente
Para subir os serviços escalando o cluster dinamicamente para **5 workers**, execute:
```bash
docker compose up -d --scale airflow-worker=5
```

### Passo 4: Escalar Down para 2 Workers Sem Derrubar os Serviços
Com o cluster ativo (com 5 workers), podemos alterar a quantidade de workers ativos dinamicamente (sem derrubar o banco ou os outros serviços) executando o mesmo comando de scale ajustado para **2**:
```bash
docker compose up -d --scale airflow-worker=2
```
O Docker Compose fará o downgrade da quantidade de workers de 5 para 2 de forma limpa, interrompendo graciosamente os workers excedentes.

---

## Pipeline de Dados (DAG)

O pipeline foi construído utilizando a **TaskFlow API** (`@dag` e `@task`) atendendo aos seguintes requisitos:
- **Paralelismo**: A criação de tabelas é a task inicial. Uma vez executada com sucesso, ela dispara as tasks de ingestão de **Users**, **Products** e **Carts** de forma 100% paralela.
- **TaskFlow Dependencies**: O fluxo de dados e controle foi estruturado diretamente através das chamadas de funções, sem operadores clássicos explícitos.
- **Filtros e Timezone**: O agendamento está ancorado no fuso horário `America/Sao_Paulo` utilizando a biblioteca `pendulum`, com `catchup=False`.

---

## Acessos UIs
- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Usuário: `admin` | Senha: `admin`)
- **Flower UI**: [http://localhost:5555](http://localhost:5555)
