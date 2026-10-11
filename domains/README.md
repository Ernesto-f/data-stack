# Domínios da plataforma

Este repositório e a VPS Hostinger atendem **demo e desenvolvimento**. Produção de cliente será implantada separadamente em servidor contratado pelo próprio cliente.

## Organização

Para preparar uma demonstração e provisionar acessos mínimos, consulte [`GUIA-OPERACIONAL-DE-DEMO-E-ACESSOS.md`](GUIA-OPERACIONAL-DE-DEMO-E-ACESSOS.md). O CRM demo/dev é Odoo Community com login Keycloak; veja [`crm/README.md`](crm/README.md).

```text
domains/
├── data-platform/  # coleta, pipelines, transformação e monitoramento
├── preprocessing/  # DocETL, preparação de documentos antes do RAG
├── archive/        # Paperless-ngx, gestão de documentos
├── rag/            # WeKnora, usando provedor externo de LLM pago
├── crm/            # Odoo Community + CRM, OIDC via Keycloak
└── automation/     # domínio reservado para automações

apps/               # aplicações ainda não classificadas
infra/              # Compose/imagens compartilhadas durante a migração
platform/           # destino planejado para proxy e autenticação compartilhados
```

## Estado dos componentes

| Domínio/camada | Localização | Situação |
| --- | --- | --- |
| Coleta e armazenamento | `domains/data-platform/docker-compose.yml`, DAGs em `domains/data-platform/orchestration/`; Dockerfile do Airflow em `infra/airflow/` | Airflow, PostgreSQL e MinIO estão na VPS demo/dev; DAGs ficam pausadas até configurar Connections próprias para fontes de teste. |
| DAGs e transformações | `domains/data-platform/orchestration/`, `domains/data-platform/transform/` | Movidos para o domínio de dados. |
| Monitoramento | `domains/data-platform/monitoring/` | Prometheus, Node Exporter, cAdvisor e Grafana com marca Pulse Analytics. Airflow usa Pulse Automation; publicados via HTTPS em `analytics.izatacore.com` e `automation.izatacore.com`. |
| Pré-processamento | `domains/preprocessing/README.md`; fonte DocETL em `/media/sam/Arquivos/arquivos/Projetos/chat-industial-v1/docetl` | Pulse SmartETL publicado em `https://smartetl.izatacore.com`, com Keycloak/OAuth2 Proxy e HTTPS; usa o DeepSeek Flash ativo no Samia e produz JSON para revisão/importação manual. |
| Portal | `platform/portal/` | Pulse Portal publicado em `https://portal.izatacore.com`, protegido por Keycloak/OAuth2 Proxy e com atalhos para as aplicações demo/dev. |
| Documentos | `domains/archive/README.md`; Compose em `domains/archive/docker-compose.yml` | Pulse Archive publicado em `https://archive.izatacore.com`; instância Paperless nova e vazia na VPS, separada dos dados Paperless locais. |
| RAG | Código em `domains/rag/README.md`; cópia operacional em `/srv/data-stack/domains/rag/weknora` na VPS | Samia (WeKnora) publicado em `https://knowledge.izatacore.com`; DeepSeek Flash para chat e Ollama/BGE-M3 local para embeddings. |
| CRM | `domains/crm/` | Odoo Community 19.0 com Compose/PostgreSQL separados por tenant. `democrm.izatacore.com` é demo; outros tenants usam `<tenant>.crm.izatacore.com` e grupos `/pulse/tenants/<tenant>/crm`. |
| Automação | `domains/automation/` | Node-RED publicado em `https://datalogger.izatacore.com` como Pulse DataLogger, protegido por HTTPS e Keycloak/OAuth2 Proxy; `nodered.izatacore.com` redireciona para ele; inicializa em safe mode. |
| Apps existentes | `apps/services/`, `apps/frontend/` | Ainda não classificados entre os domínios de negócio. |
| Serviços compartilhados | `infra/compose.auth.yml`, `infra/nginx/`, `infra/keycloak/`, `infra/oauth2-proxy/` | Keycloak 26.7.0 + OAuth2 Proxy v7.15.3 estão ativos como identidade central em `portal.izatacore.com/keylogin`; a migração dos serviços legados está em andamento. Ver `infra/AUTH-ARCHITECTURE.md`. |
| Ferramenta de banco | CloudBeaver em `infra/compose.apextools.yml` | Incluído no plano inicial. |

## Regras

- Interfaces de produto e o portal usam os assets oficiais do logo Pulse; cada serviço mantém seu nome comercial (Samia, Pulse Analytics, Pulse Automation, Pulse DataLogger, Pulse SmartETL, Pulse Archive e Pulse CRM).
- Demo/dev e produção devem ter dados, credenciais e volumes independentes.
- Componentes persistentes ignorados pelo Git são dados locais: não apagar nem mover sem backup.
- Segredos não devem ser versionados; configure credenciais exclusivas para a VPS.
- Serviços compartilhados atendem múltiplos domínios e devem ficar fora dos domínios de negócio.
- Ao mover Compose ou imagens, ajustar todos os caminhos relativos, bind mounts, scripts e workflows e validar com `docker compose config`.

## Migração atual

DAGs, transformações e monitoramento estão em `domains/data-platform/`. Airflow, warehouse PostgreSQL, MinIO e monitoramento estão rodando na VPS demo/dev. As 20 DAGs carregam pausadas; precisam de Connections com credenciais de fontes de teste antes de serem executadas. Pulse SmartETL prepara documentos antes do RAG; a importação do JSON para o Samia segue manual após revisão.

O código-fonte do WeKnora customizado foi transferido para a VPS sem `.env`, `.git`, dados locais ou bundle offline. Consulte `domains/rag/README.md` para portas, perfis e operação.
