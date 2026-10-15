# Handoff — Pulse Portal `/admin`

**Atualizado:** 2026-10-15  
**Estado:** Revisão de funcionalidades e boas práticas iniciada; Fases 1, 2, auditoria e reconciliação implementadas localmente; nada publicado na VPS.

## Objetivo

Completar gradualmente `https://portal.izatacore.com/admin/`, mantendo o control plane de empresas separado da administração interna de cada produto.

## Estado atual

Implementado localmente para a Fase 1 (ciclo de vida de tenants):

- Edição do nome da empresa e do host CRM.
- Ações de ativar, suspender e reativar um tenant.
- Preflight de ativação verificando host CRM e grupos Keycloak exigidos, incluindo o grupo admin do tenant para que o primeiro owner possa administrar membros.
- Confirmação explícita na interface de que o provisionamento manual da stack CRM foi feito.
- A API repete as verificações antes de ativar; na falha de sincronização dos grupos Keycloak tenta restaurar o estado anterior do tenant e sincronizar novamente.
- Testes de endpoint/função cobrem bloqueio de ativação por preflight incompleto, updates parciais e rollback quando a sincronização Keycloak falha.
- README operacional atualizado.

Fase 2 de memberships e operação (implementada/commitada):

- A edição de acesso pode mudar a função tenant-scoped; só owner/platform admin podem gerir admins e só platform admin pode alterar o owner.
- Impede demover/suspender o último owner ativo.
- Reativação de membership suspensa com seleção de produtos/assentos; valida tenant ativo e entitlement e sincroniza grupos, revertendo se Keycloak falhar.
- A edição de produtos/função restaura os dados anteriores se a sincronização de grupos falhar.
- Teste de regressão confirma que assento no limite bloqueia nova atribuição e que assento disponível permite atribuir.
- UI mostra ação de reativação e seletor de função conforme as permissões do ator.
- Visão operacional no `/admin`: contagem total/por estado, busca por nome/slug e filtro por estado; lista vazia de resultados esconde o detalhe selecionado.

Entrega de auditoria implementada localmente:

- Tabela `tenant_control_audit` criada idempotentemente pelo schema do serviço.
- Eventos de criação/edição de tenant, plano, convite, mudança de acesso/função, suspensão e reativação; detalhes operacionais mínimos, sem e-mail.
- Endpoint de consulta protegido para platform admins e limitado a 200 eventos por chamada; UI mostra as 25 ações recentes.

Reconciliação Keycloak:

- Endpoint restrito a platform admins compara grupos obrigatórios por tenant e grupos gerenciados esperados versus atuais por membership.
- UI permite executar uma verificação somente leitura e, após confirmação explícita, sincronizar apenas memberships divergentes; a ação corretiva fica bloqueada se faltarem grupos provisionados.
- Erros parciais retornam quantos memberships foram sincronizados e permitem repetir com segurança; ações corretivas são auditadas.
- Indisponibilidade do provedor retorna 503 em vez de falso sucesso.
- `/pulse/home` passou a ser tratado como grupo gerenciado, para detectar/remover o grupo quando o usuário perde todos os memberships ativos.

Melhorias de robustez aplicadas nesta revisão:

- Login com e-mail verificado agora ativa a membership e sincroniza os grupos antes de concluir `/v1/me`; falha do Keycloak retorna 503.
- Suspensão de membro restaura status, produtos e grupos se a sincronização falhar; repetição de uma suspensão já concluída é idempotente.
- Atualização de acesso lê os produtos anteriores dentro do mesmo lock transacional da membership.
- Erros de grupo ausente no Keycloak têm tipo próprio; indisponibilidade não é reportada como grupo faltante e preflight retorna 503.
- Atualização de entitlement exige revogar memberships antes de desativar um produto; falhas de sincronização pós-gravação retornam 503 com progresso explícito, em vez de erro 500 genérico.
- Leituras críticas de suspensão, função e reativação ficam sob lock da linha do tenant e membership; chamadas externas do Keycloak não são executadas dentro da transação SQL de entitlement.
- Primeiro login verificado agora reverte somente as memberships recém-ativadas se a sincronização falhar, permitindo retry seguro.
- Nome de membro após trim exige pelo menos dois caracteres; testes cobrem progresso parcial de sync e rollback do primeiro login.
- PATCH de tenant que corrige `crm_host` e ativa na mesma requisição agora valida o host candidato, sem exigir uma segunda chamada artificial.
- A política de grupos também aplica `READY_PRODUCTS` na sincronização, evitando conceder grupos de produtos planejados mesmo se o banco/catalogo for alterado indevidamente.
- Ruff corrigiu imports/estilo seguro e agora passa com as exceções documentadas `B008` (padrão FastAPI `Depends`) e `SIM117` (contextos async aninhados mantidos explícitos); não há outros avisos.

Arquivos da alteração atual:

- `platform/portal/admin/index.html`
- `platform/tenant-control/app/main.py`
- `platform/tenant-control/tests/test_tenant_lifecycle.py` (novo)
- `HANDOFF.md`
- `platform/tenant-control/tests/test_policies.py` (já tinha alteração local para reconhecer `/pulse/tenant-control-admin`)
- `platform/tenant-control/README.md` (já tinha alteração local documentando o grupo admin dedicado)

## Validação executada

- `PYTHONPATH=platform/tenant-control /tmp/opencode/tenant-control-venv/bin/pytest -q platform/tenant-control/tests` — **11 passaram**.
- `python -m py_compile platform/tenant-control/app/main.py platform/tenant-control/app/policies.py` — passou.
- `node --check` no JavaScript embutido de `platform/portal/admin/index.html` — passou.
- `git diff --check` nos arquivos da tarefa — passou.
- Última validação: suíte tenant-control **27 passaram**; compilação Python, sintaxe JS e `git diff --check` passaram.
- Revisão de autorização e fluxo concluída: preflight/update restritos a platform admins; a ativação exige grupo admin tenant-scoped mesmo sem CRM; suspender/reativar sincroniza grupos sem alterar os grupos de outros tenants.
- Não foi feita validação visual com sessão Keycloak no navegador.
- Há divergência conhecida entre `platform/portal/nginx/portal.izatacore.com.conf` (proxy completo da API) e `infra/nginx/locations-enabled/portal-admin.conf` (configuração Docker ativa previamente observada); não alterei essa infraestrutura sem validação do caminho de deployment.

## Produção / deployment

- As alterações acima são somente locais. Não foram enviadas ou aplicadas na VPS.
- O control plane ativo é servido na VPS `portal.izatacore.com` / host SSH com chave validada previamente como `knowledge.izatacore.com`.
- O banco de tenant-control foi consultado anteriormente e estava vazio (`tenants_total = 0`). Reconfirmar antes de mudanças operacionais.
- Não tocar em configurações secretas, bancos ou stacks de produto durante deploy sem plano de rollback.

## Próximos passos

1. Atualizar a documentação dos fluxos finais e revisar lacunas de endpoint/UI.
2. Aguardar autorização explícita antes de publicar na VPS; deployment não foi solicitado nem executado nesta etapa.
3. Se autorizado, publicar com backup das imagens e configuração anterior; não alterar dados nem stacks de produto.
4. Validar login, lista vazia, criação, preflight, ativação, suspensão/retomada, reativação, auditoria e reconciliação no painel implantado.

## Commits recentes

- `1a6e3d0` — `Commit pending repository updates` — commitou o working tree inteiro (472 arquivos; 278,799 inserções / 1,850 remoções). A exceção solicitada foi `files/notasficas/NFSe_55083048_6530635.pdf`, que permanece não rastreado junto de `files/`.
- `62289e5` — `Add tenant member reactivation and role management` — alterações de reativação/funções e testes.
- `a4bb48c` — `Test tenant member seat limit enforcement` — cobertura do limite de assentos.
- `bd5f52d` — `Add tenant operational dashboard and filters` — métricas e filtros de tenants no `/admin`.
- `3971f7d` — `Add tenant control administrative audit log` — eventos, endpoint e painel de auditoria admin.
- `ccd9898` — `Add read-only tenant Keycloak reconciliation` — endpoint/report, UI e detecção de drift.
- A entrega mais recente `Add confirmed Keycloak drift synchronization` está no commit HEAD deste handoff.
- `70f3b73` — `Harden tenant access synchronization` — primeiro login, rollback de suspensão e erros tipados do Keycloak.
- `33c7de7` — `Harden entitlement synchronization failures` — validação de desativação e erros 503 explícitos.
- `538b4d8` — `Make verified login activation retry-safe` — compensação do primeiro login.
- `8d1bd0b` — `Serialize tenant membership mutations` — locks do tenant/membership e validações sob transação.
- Healthcheck Docker da API adicionado em `infra/compose.tenant-control.yml`; `docker compose config`, suíte (**27 testes**), Python, JavaScript e `git diff --check` passaram.

## Cuidados de contexto

- O working tree contém várias alterações preexistentes ou não relacionadas em outras pastas. Preservá-las; não resetar nem limpar arquivos sem autorização.
- A alteração de `platform/tenant-control/tests/test_policies.py` e parte das mudanças no README antecedem a Fase 1 e devem ser preservadas.
- O grupo `/pulse/tenant-control-admin` foi escolhido para evitar conceder por engano os privilégios amplos do `/pulse/admin` (Grafana/Airflow).
