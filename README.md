# Data Stack — Demo e Desenvolvimento

Plataforma self-hosted para desenvolver e demonstrar domínios funcionais independentes. Produção de clientes será implantada em servidores separados, contratados para cada cliente.

## Domínios

- `platform/portal/`: Pulse Portal em `portal.izatacore.com`, catálogo central das aplicações.
- `domains/data-platform/`: Pulse Analytics em `analytics.izatacore.com`, Pulse Automation em `automation.izatacore.com`, PostgreSQL, MinIO, transformações e monitoramento.
- `domains/archive/`: Pulse Archive (Paperless-ngx) em `archive.izatacore.com` para armazenar e organizar documentos.
- `domains/preprocessing/`: Pulse SmartETL em `smartetl.izatacore.com`, para preparar documentos com DeepSeek Flash antes da ingestão no RAG.
- `domains/rag/`: Samia (WeKnora) em `knowledge.izatacore.com`, com DeepSeek Flash para conversa e BGE-M3 via Ollama para embeddings.
- `domains/crm/`: domínio reservado para CRM.
- `domains/automation/`: Pulse DataLogger (Node-RED) em `datalogger.izatacore.com`.
- `apps/`: aplicações existentes ainda aguardando classificação por domínio.
- `infra/`: autenticação compartilhada, ferramentas mantidas e imagens de suporte.

Veja [`domains/README.md`](domains/README.md) para o mapa de componentes e o estágio da reorganização.

## Plataforma de dados

O Compose demo/dev da plataforma está em `domains/data-platform/docker-compose.yml`; monitoramento está em `domains/data-platform/monitoring/docker-compose.monitoring.yml`. DAGs e transformações ficam dentro de `domains/data-platform/`. A implantação usa um `.env` exclusivo, não o arquivo do servidor anterior em `infra/.env`.

```bash
docker compose -p data-stack --env-file domains/data-platform/.env \
  -f domains/data-platform/docker-compose.yml --profile collection up -d

docker compose -p data-platform-monitoring --env-file domains/data-platform/.env \
  -f domains/data-platform/monitoring/docker-compose.monitoring.yml up -d
```

## Serviços compartilhados

Proxy/autenticação e ferramentas mantidas permanecem em `infra/compose.auth.yml` e `infra/compose.apextools.yml`. CloudBeaver e Guacamole fazem parte do conjunto de ferramentas. Os Composes legados de aplicação foram removidos; a plataforma de dados usa apenas `domains/data-platform/docker-compose.yml`.

## Ambientes

- Este repositório e a VPS Hostinger são para **demo/dev**, não produção.
- Produções de clientes terão infraestrutura separada.
- Não reutilize dados nem credenciais de demo/dev em produção.
- Volumes locais são dados persistentes; não mova nem apague sem backup e conferência.
- Pulse Automation e Pulse Analytics usam `automation.izatacore.com` e `analytics.izatacore.com`; `airflow.izatacore.com` e `grafana.izatacore.com` redirecionam para eles. PostgreSQL, MinIO, Prometheus e portas de métricas permanecem privadas.
