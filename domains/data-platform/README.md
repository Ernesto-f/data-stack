# Plataforma de coleta de dados — demo/dev

Este domínio agrupa Airflow, PostgreSQL para o warehouse, MinIO para saídas S3/data lake e monitoramento. O MinIO é somente para os dados da plataforma de coleta; **não substitui nem armazena documentos do Paperless**, que permanece a fonte do RAG.

## Componentes

- `orchestration/`: DAGs, conectores e jobs do Airflow.
- `transform/`: área de transformação.
- `monitoring/`: Prometheus, Node Exporter, cAdvisor e Grafana, branded como **Izatacore Pulse**.
- `docker-compose.yml`: Compose isolado da plataforma de dados.
- `.env.example`: modelo sem segredos; a VPS usa um `.env` local ignorado pelo Git.

O Compose legado da plataforma em `infra/` foi removido. O Dockerfile compartilhado do Airflow continua em `infra/airflow/`. Não use o antigo `infra/.env`; use o arquivo local da plataforma, criado a partir de `.env.example`, com credenciais próprias de demo/dev.

## Inicialização na VPS

O `.env` e o arquivo de senha legado do SimpleAuth do Airflow são privados na VPS; não os versione. O Airflow atual usa FAB Auth Manager com login OIDC pelo realm Pulse; o antigo arquivo SimpleAuth fica apenas para rollback.

Com os segredos prontos, inicie a coleta e o monitoramento:

```bash
docker compose -p data-stack --env-file domains/data-platform/.env \
  -f domains/data-platform/docker-compose.yml --profile collection up -d

docker compose -p data-platform-monitoring --env-file domains/data-platform/.env \
  -f domains/data-platform/monitoring/docker-compose.monitoring.yml up -d
```

Airflow cria DAGs novas pausadas; assim, nada tenta acessar fontes externas antes de configurar as Connections. Os serviços web, bancos, MinIO e métricas publicados no host ficam vinculados a `127.0.0.1`. O Nginx publica a UI HTTPS e o OAuth2 Proxy exige `/pulse/home/airflow`; o login OIDC do Airflow usa o mesmo Keycloak Pulse.

## Integração com fontes

Os DAGs atuais referenciam Airflow Connections como `sql_leaf_chronos`, `sql_udi_chronos_reader`, `sql_udi_guardian`, `sql_udi_gmes`, `sql_udi_pmdnet` e `pg_dw_apex`. As 20 DAGs carregam pausadas; nenhuma está executando. Não foram copiados segredos do ambiente antigo, e essas Connections ainda precisam ser criadas com credenciais próprias para demo/dev. Não conecte os DAGs a sistemas de clientes sem autorização e credenciais específicas.

## Acesso local na VPS

| Serviço | Porta local |
| --- | ---: |
| Airflow | 18082 |
| Warehouse PostgreSQL | 15432 |
| MinIO API / Console | 19000 / 19001 |
| Grafana | 13001 |
| Prometheus | 19090 |

Airflow e Grafana são publicados pelo Nginx em `https://automation.izatacore.com` (**Pulse Automation**) e `https://analytics.izatacore.com` (**Pulse Analytics**). Os endereços antigos redirecionam para os novos; os ports de container permanecem presos ao loopback. MinIO e PostgreSQL não têm subdomínio público.

## Dashboard Pulse Knowledge

O dashboard provisionado **Pulse Knowledge — Visão geral** consulta views agregadas do PostgreSQL do WeKnora. O usuário `grafana_knowledge_reader` recebe SELECT somente no schema `grafana_readonly`; as tabelas de conteúdo, chunks e embeddings não são concedidas ao Grafana.

O acesso entre o Postgres do WeKnora e o Grafana usa a rede Docker dedicada `pulse-knowledge-metrics`. A senha do leitor fica somente em `GRAFANA_WEKNORA_DB_PASSWORD` no `.env` privado da plataforma e também é aplicada à role do banco. O datasource é carregado por `monitoring/grafana/provisioning/datasources/weknora.yml`; as views estão em `monitoring/sql/weknora-grafana-views.sql`.

O dashboard mostra contagem de bases/documentos, status de processamento, ingestão diária, tipo de arquivo e agregados das etapas de processamento. Não apresenta nomes de documentos, texto, chunks, embeddings, mensagens, usuários ou conteúdo de tenants.

## Monitoramento da infraestrutura, PostgreSQL e vetores

O dashboard **Pulse Infra, Databases & Vetores** (`/d/pulse-databases-vectors/...`) mostra uso de CPU/memória/disco do host, recursos dos containers de banco e as sete instâncias PostgreSQL/Timescale ativas: warehouse, metadados do Airflow, WeKnora/ParadeDB, Archive, SALUS ERP demo, SALUS LIMS demo e Traccar/TimescaleDB. Exporters Prometheus sem portas publicadas usam roles `postgres_exporter` com `pg_read_all_stats`; os coletores de texto de consulta e configurações ficam desligados e os segredos ficam no `.env` privado.

O WeKnora usa ParadeDB com `pgvector` e `pg_search`. Views no schema `grafana_readonly` apresentam estimativa de vetores, extensão/versão e tamanhos de tabela/índices HNSW sem conceder ao Grafana acesso aos vetores ou textos. Qdrant, Milvus e Weaviate não estão ativos nesta instalação. A role do Grafana pode consultar somente essas views agregadas.

O coletor também identifica consultas SQL ativas que ultrapassam `PG_SLOW_QUERY_THRESHOLD_SECONDS` (padrão: 180 segundos). O sinal contém somente serviço, base, contagem, duração máxima e limiar; não coleta texto SQL. A regra `PostgreSQLSlowQueryOverThreshold` fica visível no estado de alertas do Prometheus e o dashboard mostra as consultas encontradas. Notificações externas ainda dependem de configurar um contact point.

### Alerta de consulta lenta

O exporter tem um coletor agregado que observa apenas consultas em execução (não exporta o SQL), separa métricas por instância/banco e só gera séries quando passa do limiar configurado. O padrão é `PG_SLOW_QUERY_THRESHOLD_SECONDS=180` (3 min). Para alterar, edite essa variável em `/srv/data-stack/domains/data-platform/.env`, rode `python3 monitoring/scripts/setup-postgres-exporters.py` e recrie os exporters PostgreSQL. O dashboard mostra consultas que ultrapassaram o limiar e o Prometheus avalia a regra `PostgreSQLSlowQueryOverThreshold`.

A regra fica visível no estado de alertas Prometheus/Grafana; ainda não há um contact point (e-mail, Telegram etc.) configurado para notificação externa.

O Node Exporter escuta apenas no gateway Docker da VPS para coleta do host. Os DAGs copiados não incluem `.env` local nem banco SQLite de desenvolvimento; os dados persistentes da plataforma ficam em volumes Docker.

Airflow e Grafana são publicados com HTTPS pelo Nginx. Airflow usa o FAB Auth Manager com OIDC do realm Pulse; o client `pulse-automation` e a role vêm dos grupos Keycloak `/pulse/home/airflow` e `/pulse/home/airflow-admin`. O arquivo SimpleAuth antigo fica disponível apenas como referência de rollback e não autentica o serviço após a migração.
