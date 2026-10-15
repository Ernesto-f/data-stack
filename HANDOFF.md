# Handoff — Pulse Portal `/admin`

**Atualizado:** 2026-10-15  
**Estado:** Fase 1 commitada; Fase 2 de memberships em implementação local; nada publicado na VPS.

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

Fase 2 em andamento (alterações ainda não commitadas):

- A edição de acesso pode mudar a função tenant-scoped; só owner/platform admin podem gerir admins e só platform admin pode alterar o owner.
- Impede demover/suspender o último owner ativo.
- Reativação de membership suspensa com seleção de produtos/assentos; valida tenant ativo e entitlement e sincroniza grupos, revertendo se Keycloak falhar.
- A edição de produtos/função restaura os dados anteriores se a sincronização de grupos falhar.
- Teste de regressão confirma que assento no limite bloqueia nova atribuição e que assento disponível permite atribuir.
- UI mostra ação de reativação e seletor de função conforme as permissões do ator.
- Visão operacional no `/admin`: contagem total/por estado, busca por nome/slug e filtro por estado; lista vazia de resultados esconde o detalhe selecionado.

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
- Depois das alterações da Fase 2: suíte tenant-control **15 passaram**; compilação Python, sintaxe JS e `git diff --check` passaram.
- Revisão de autorização e fluxo concluída: preflight/update restritos a platform admins; a ativação exige grupo admin tenant-scoped mesmo sem CRM; suspender/reativar sincroniza grupos sem alterar os grupos de outros tenants.
- Não foi feita validação visual com sessão Keycloak no navegador.

## Produção / deployment

- As alterações acima são somente locais. Não foram enviadas ou aplicadas na VPS.
- O control plane ativo é servido na VPS `portal.izatacore.com` / host SSH com chave validada previamente como `knowledge.izatacore.com`.
- O banco de tenant-control foi consultado anteriormente e estava vazio (`tenants_total = 0`). Reconfirmar antes de mudanças operacionais.
- Não tocar em configurações secretas, bancos ou stacks de produto durante deploy sem plano de rollback.

## Próximos passos

1. Completar testes de endpoint para as operações de função/reativação e rollback; revisar diff da Fase 2.
2. Aguardar autorização explícita antes de publicar na VPS; deployment não foi solicitado nem executado nesta etapa.
3. Se autorizado, publicar com backup das imagens e configuração anterior; não alterar dados nem stacks de produto.
4. Validar login, lista vazia, criação, preflight, ativação, suspensão/retomada e ciclo de reativação de membership no painel implantado.
5. Próximas fases planejadas: painel operacional (busca, filtros, métricas) e auditoria/reconciliação Keycloak.

## Commits recentes

- `1a6e3d0` — `Commit pending repository updates` — commitou o working tree inteiro (472 arquivos; 278,799 inserções / 1,850 remoções). A exceção solicitada foi `files/notasficas/NFSe_55083048_6530635.pdf`, que permanece não rastreado junto de `files/`.
- `62289e5` — `Add tenant member reactivation and role management` — alterações de reativação/funções e testes.
- `a4bb48c` — `Test tenant member seat limit enforcement` — cobertura do limite de assentos.
- Próxima entrega local em andamento: visão operacional e filtros de tenants no `/admin`.

## Cuidados de contexto

- O working tree contém várias alterações preexistentes ou não relacionadas em outras pastas. Preservá-las; não resetar nem limpar arquivos sem autorização.
- A alteração de `platform/tenant-control/tests/test_policies.py` e parte das mudanças no README antecedem a Fase 1 e devem ser preservadas.
- O grupo `/pulse/tenant-control-admin` foi escolhido para evitar conceder por engano os privilégios amplos do `/pulse/admin` (Grafana/Airflow).
