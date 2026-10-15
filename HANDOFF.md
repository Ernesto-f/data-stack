# Handoff — Pulse Portal `/admin`

**Atualizado:** 2026-10-15  
**Estado:** Fases 1, 2, auditoria e relatório de reconciliação commitados; ação corretiva de drift planejada; nada publicado na VPS.

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

Relatório de reconciliação Keycloak (somente leitura):

- Endpoint restrito a platform admins compara grupos obrigatórios por tenant e grupos gerenciados esperados versus atuais por membership.
- UI permite executar a verificação sem alterar Keycloak; indisponibilidade do provedor retorna 503 em vez de falso sucesso.
- `/pulse/home` passou a ser tratado como grupo gerenciado, para detectar/remover o grupo quando o usuário perde todos os memberships ativos.

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
- Última validação: suíte tenant-control **20 passaram**; compilação Python, sintaxe JS e `git diff --check` passaram.
- Revisão de autorização e fluxo concluída: preflight/update restritos a platform admins; a ativação exige grupo admin tenant-scoped mesmo sem CRM; suspender/reativar sincroniza grupos sem alterar os grupos de outros tenants.
- Não foi feita validação visual com sessão Keycloak no navegador.

## Produção / deployment

- As alterações acima são somente locais. Não foram enviadas ou aplicadas na VPS.
- O control plane ativo é servido na VPS `portal.izatacore.com` / host SSH com chave validada previamente como `knowledge.izatacore.com`.
- O banco de tenant-control foi consultado anteriormente e estava vazio (`tenants_total = 0`). Reconfirmar antes de mudanças operacionais.
- Não tocar em configurações secretas, bancos ou stacks de produto durante deploy sem plano de rollback.

## Próximos passos

1. Adicionar ação explícita e confirmada para aplicar sincronização de grupos aos membros divergentes, bloqueando-a se faltarem grupos provisionados.
2. Aguardar autorização explícita antes de publicar na VPS; deployment não foi solicitado nem executado nesta etapa.
3. Se autorizado, publicar com backup das imagens e configuração anterior; não alterar dados nem stacks de produto.
4. Validar login, lista vazia, criação, preflight, ativação, suspensão/retomada, reativação, auditoria e reconciliação no painel implantado.

## Commits recentes

- `1a6e3d0` — `Commit pending repository updates` — commitou o working tree inteiro (472 arquivos; 278,799 inserções / 1,850 remoções). A exceção solicitada foi `files/notasficas/NFSe_55083048_6530635.pdf`, que permanece não rastreado junto de `files/`.
- `62289e5` — `Add tenant member reactivation and role management` — alterações de reativação/funções e testes.
- `a4bb48c` — `Test tenant member seat limit enforcement` — cobertura do limite de assentos.
- `bd5f52d` — `Add tenant operational dashboard and filters` — métricas e filtros de tenants no `/admin`.
- `3971f7d` — `Add tenant control administrative audit log` — eventos, endpoint e painel de auditoria admin.
- A entrega de relatório de reconciliação Keycloak será commitada junto deste handoff.

## Cuidados de contexto

- O working tree contém várias alterações preexistentes ou não relacionadas em outras pastas. Preservá-las; não resetar nem limpar arquivos sem autorização.
- A alteração de `platform/tenant-control/tests/test_policies.py` e parte das mudanças no README antecedem a Fase 1 e devem ser preservadas.
- O grupo `/pulse/tenant-control-admin` foi escolhido para evitar conceder por engano os privilégios amplos do `/pulse/admin` (Grafana/Airflow).
