# Pulse Invoice

Projeto de produto para captura e processamento assistido de notas fiscais, com conferência humana e consulta segura dos dados aprovados.

## Estrutura

- `docs/PRODUCT-PLAN.md`: escopo do cliente, pacotes, entrada/saída e etapas do produto.
- `database/warehouse-schema.sql`: schema `pulse_invoice` aplicado ao warehouse, com uma tentativa interna pendente de revisão.
- `database/tenant-isolation.sql`: associação Keycloak `sub` → tenant, role restrita e RLS; aplicado ao warehouse ativo vazio.
- `api/`: API FastAPI com validação de JWT OIDC, resolução de membership/RLS, upload ao Archive, extração local, revisão e exportação; implantada atrás do Portal.
- `docker-compose.api.yml` e `.env.example`: serviço preso ao loopback da VPS, conectado às redes privadas do warehouse, Archive e DocETL, com DSN/token em `.env` privado.
- `security/README.md`: contrato de tenant para Portal, API, Archive e PostgreSQL.
- `docetl/pipelines/`: pipeline NFS-e sobre operadores `split`, `code_map` e `code_reduce` do DocETL existente; sem chamadas a LLM.
- `airflow/dags/`: orquestração, ingestão, extração e publicação de dados aprovados.
- `grafana/dashboards/`: dashboards internos e, quando houver isolamento por tenant, painéis por cliente.
- `tests/`: documentos sintéticos e testes de aceite.

## Serviços Pulse compartilhados

O projeto usa Pulse Archive para preservar os originais e revisão; Pulse SmartETL/DocETL para extração; Pulse Automation/Airflow para orquestrar; o warehouse PostgreSQL para registros estruturados; Samia para consulta; e Pulse Analytics/Grafana para métricas operacionais. Os artefatos específicos do produto ficam aqui, enquanto as configurações compartilhadas continuam nos domínios de cada serviço.

## Estado atual

- Schema e views de reporting criados no warehouse; há uma fatura municipal aprovada e uma NFS-e nacional pendente, com eventos de extração, correção, backfill e aprovação. `iss_rate_percent` tem evidência na página 1; `invoice_lines` segue vazia.
- O DDL ativo inclui `payment_date`; a migração RLS e a role de runtime foram aplicadas. Só existe o tenant interno provisório `pulse-invoice-demo`, associado à conta `elonesampaio`; nenhum cliente externo foi provisionado.
- A tela está disponível em `https://portal.izatacore.com/pulse-invoice/`, atrás do login Portal. A API é protegida pelo OAuth2 Proxy e continua ligada somente ao loopback da VPS.
- Um PDF estritamente sintético passou pela conversão Docling e pipeline DocETL ativos, sem chamadas a LLM. A tentativa de upload reportada pelo usuário foi recuperada do original já salvo no Archive e, após revisão humana, aprovada; nenhuma informação extraída foi exibida durante a recuperação.
- O fluxo MVP aceita somente PDF digital com camada de texto (limite 25 MiB), detecta reenvio do mesmo arquivo, preserva o original no Archive, abre rascunho para revisão, registra alterações/aprovação/rejeição e exporta aprovadas em CSV. PDF escaneado falha fechado; OCR, XML, lote, Airflow e integrações seguem fora do MVP.
- O campo **Alíquota ISS (%)** acompanha a extração, revisão e exportação separadamente do valor do ISS; o limite de edição é de 0 a 100%.
- Cada tentativa registra uma família de layout: `municipal_classic_v1` é o perfil estabelecido; `nfse_nacional_candidate_v1` tem regras candidatas e continua em revisão; formatos não reconhecidos ficam sem perfil e com sinal heurístico reduzido.
- Os testes descartáveis da API/RLS, do cliente Archive e da classificação de layout passaram (14 testes); a extração foi exercitada contra Docling e DocETL da VPS com conteúdo sintético. Antes de clientes reais, ainda é necessário provar ACL e isolamento documental entre tenants no Archive e provisionar o tenant do cliente.
- SmartETL agora alcança o Ollama local pela rede privada. O pipeline experimental `docetl/pipelines/nfse-local-llama-experiment.yaml` usa Llama 3.1 8B quantizado e normalização determinística; o modelo padrão DeepSeek não foi alterado. No documento atual, o Llama coincidiu com 2 dos 5 campos corrigidos manualmente; por isso segue experimental e não substitui o rascunho de produção.
- A auditoria de revisão agora grava somente os campos cujo valor realmente mudou. No evento de correção histórico, anterior a esse ajuste, a diferença exata continua disponível comparando `event_payload.before`/`after`. Isso permite usar correções para métricas por campo e testes de regressão sem copiar valores pessoais para o código ou serviços externos.
- O processo de feedback humano compartilhável entre projetos está em `domains/preprocessing/HUMAN-IN-THE-LOOP-ETL-FRAMEWORK.md`; a aplicação prática ao Invoice, incluindo limites atuais do Llama, está em `docs/PRODUCT-PLAN.md`.
- A extração estruturada usa pipeline do DocETL existente, precedida pela conversão local do Docling no SmartETL; não foi criado um novo serviço de extração.
- A migração RLS e a API passaram por teste em PostgreSQL descartável com dois tenants sintéticos; upload, deduplicação, revisão, aprovação, histórico e CSV estão cobertos pelo teste de fluxo da API. O warehouse ativo contém somente a fatura municipal aprovada e a NFS-e nacional pendente do tenant interno.
- A conta interna está mapeada ao tenant e aos grupos do Archive. O serviço mantém a propriedade dos novos documentos e concede visualização por objeto ao usuário que enviou; o isolamento documental multi-tenant do Archive ainda não está liberado para clientes.
- O plano propõe módulos por pacote; as regras de entitlement, a DAG de ingestão e o dashboard financeiro ainda não foram implementados.
