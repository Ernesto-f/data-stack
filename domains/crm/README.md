# CRM

Domínio reservado para um futuro CRM no ambiente demo/dev. Nenhuma aplicação, fornecedor, URL, client OIDC ou integração foi selecionada. Este documento registra a sequência de decisão e integração sem assumir uma plataforma específica.

## Objetivo e limites

O CRM deve organizar empresas, contatos, oportunidades e atividades comerciais e, quando existir um contrato de integração claro, oferecer contexto comercial aos produtos Pulse. Não deve se tornar fonte de autorização para dados financeiros/documentais: cada produto continua responsável pelo próprio acesso e isolamento por tenant.

O CRM não está implantado. Não há contas de cliente, dados comerciais, credenciais ou integração ativa. Não anunciar URL nem conceder permissões como se o produto já existisse.

## Decisões antes da seleção

Registrar requisitos e aprovadores para:

1. Usuários e processo: número de equipes, pipeline, atividades, importação/exportação, automação e relatórios.
2. Hospedagem e operação: SaaS versus instalação controlada, residência/região de dados, backup, disponibilidade, atualização e custo total.
3. Segurança: OIDC/SAML, MFA, SCIM ou provisionamento manual, RBAC, auditoria, retenção, exportação e exclusão de dados.
4. Multi-tenancy: se o CRM será exclusivamente interno (preferível para o primeiro corte) ou terá acesso de clientes. Se houver portal de cliente, definir isolamento de registros por organização e provar acesso cruzado negado.
5. Integração: APIs/webhooks, limites, idempotência, versionamento, autenticação serviço-a-serviço e tratamento de falhas.
6. Modelo de dados: identificador imutável da organização/tenant, regras para duplicatas e correspondência CRM ↔ tenant Pulse; não usar e-mail como chave de autorização.

Documentar as alternativas avaliadas, evidências, custo e decisão aprovada antes de criar infraestrutura ou registrar dados reais.

## Identidade e autorização planejadas

- Usar realm `pulse` com client OIDC próprio e redirect URIs exatos, se o CRM suportar OIDC. Se usar SaaS sem federação adequada, documentar a exceção, MFA, ciclo de vida e fronteira de credenciais.
- Separar grupos de função comercial (por exemplo, usuário, gerente e operador) dos grupos de acesso de aplicação e dos grupos/IDs de tenant. Os nomes são exemplos, não grupos existentes.
- Começar com acesso interno e leitura/escrita limitadas a equipes aprovadas. Não conceder `/pulse/admin` para acesso rotineiro ao CRM.
- Criar, alterar e desativar usuários por processo rastreável; alinhar desligamento do Keycloak e do CRM. Provisionamento manual deve ter dono e revisão periódica até eventual automação.
- Nunca confiar num `tenant_key` informado pelo navegador ou enviado por CRM para autorizar Invoice, Archive, Analytics ou Samia. O backend do produto deve validar a identidade/associação e aplicar suas próprias políticas.

## Plano de implantação em fases

### Fase 0 — Descoberta e decisão

Validar requisitos, fornecedor, modelo de hospedagem, classificação dos dados, base legal/consentimento de contatos, custos, responsáveis e critérios de saída. Aprovar mapa de dados e modelo de organização antes da importação.

### Fase 1 — Piloto interno

Implantar em ambiente demo/dev separado, com dados sintéticos. Configurar SSO/MFA quando suportado, funções mínimas, auditoria, backup/restauração e processo de conta. Testar desativação, exportação e exclusão. Sem acesso de cliente e sem sincronização para outros produtos.

### Fase 2 — Integrações controladas

Priorizar integrações somente leitura e com benefício definido:

| Sistema | Integração possível | Limite obrigatório |
| --- | --- | --- |
| Portal | Link contextual para ficha/conta ou acesso a aplicação | O Portal continua sendo navegação/SSO; o CRM verifica seu próprio papel. |
| Pulse Invoice | Exibir referência comercial/estado agregado autorizado | Sem PDF, OCR, valores ou endpoints de cliente até contrato de dados e tenant validado; não usar o CRM para escolher o tenant da API. |
| Pulse Archive | Referência a documentos autorizados ou metadados mínimos | Sem links públicos/signed URL persistentes; checar ACL no Archive a cada acesso. |
| Samia | Busca contextual opcional sobre conteúdo explicitamente autorizado | Isolar tenant e aplicar permissões documentais; não indexar contatos/notas sem aprovação. |
| Analytics | Relatórios agregados e filtrados por função/tenant | Grafana atual é interno; não conectar o CRM a dashboards de clientes sem RBAC/filtro validado. |
| Automation/Airflow | Eventos de negócio com allowlist e idempotência | Não disparar gravações em PLC/DB nem DAGs produtivas a partir do CRM sem revisão e mecanismo de pausa. |

### Fase 3 — Produção por cliente (se necessária)

Definir responsável contratual e operacional, ambientes e credenciais separados, tenant mapping confiável, SLA, monitoramento, restauração, retenção/exclusão e suporte. Executar teste de autorização negativa entre duas organizações antes de importar dados reais ou ativar usuários externos.

## Contrato técnico para integrações

- Preferir chamadas backend-a-backend com credenciais próprias por integração, escopo mínimo, rotação e revogação documentadas; nunca embutir segredo em frontend.
- Definir dono e finalidade de cada campo sincronizado, fonte de verdade, direção do fluxo, frequência, retenção e procedimento de correção/exclusão.
- Usar IDs externos imutáveis, idempotência, paginação, limites de taxa, validação de assinatura de webhook e fila/retry com dead-letter quando apropriado.
- Minimizar dados pessoais. Evitar sincronizar documentos, OCR ou conteúdo financeiro quando bastar um identificador/estado agregado.
- Registrar eventos de auditoria sem tokens, senhas, PDFs, conteúdo de notas ou dados pessoais desnecessários.
- Falhas de CRM não podem ampliar autorização nem bloquear a revogação de acesso a dados nos produtos Pulse.

## Checklist para liberar cada fase

- Decisão de produto e responsável aprovados; URL e ambiente documentados.
- Login, MFA, funções e desativação testados; grupos/claims conferidos no client correto.
- Dados sintéticos primeiro; teste cruzado de organização/tenant falha fechado.
- Fluxos de exportação, exclusão, backup/restauração e auditoria exercitados.
- Integrações têm escopo, dono, rate limit, idempotência, rotação e plano de desligamento.
- Guia de demonstração atualizado em `../GUIA-OPERACIONAL-DE-DEMO-E-ACESSOS.md`.
