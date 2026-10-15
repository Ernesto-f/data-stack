# Handoff — Pulse Portal `/admin`

**Atualizado:** 2026-10-15  
**Estado:** Fase 1 implementada e revisão local concluída; ainda não publicada na VPS.

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

Arquivos da alteração atual:

- `platform/portal/admin/index.html`
- `platform/tenant-control/app/main.py`
- `platform/tenant-control/tests/test_tenant_lifecycle.py` (novo)
- `platform/tenant-control/tests/test_policies.py` (já tinha alteração local para reconhecer `/pulse/tenant-control-admin`)
- `platform/tenant-control/README.md` (já tinha alteração local documentando o grupo admin dedicado)

## Validação executada

- `PYTHONPATH=platform/tenant-control /tmp/opencode/tenant-control-venv/bin/pytest -q platform/tenant-control/tests` — **11 passaram**.
- `python -m py_compile platform/tenant-control/app/main.py platform/tenant-control/app/policies.py` — passou.
- `node --check` no JavaScript embutido de `platform/portal/admin/index.html` — passou.
- `git diff --check` nos arquivos da tarefa — passou.
- Revisão de autorização e fluxo concluída: preflight/update restritos a platform admins; a ativação exige grupo admin tenant-scoped mesmo sem CRM; suspender/reativar sincroniza grupos sem alterar os grupos de outros tenants.
- Não foi feita validação visual com sessão Keycloak no navegador.

## Produção / deployment

- As alterações acima são somente locais. Não foram enviadas ou aplicadas na VPS.
- O control plane ativo é servido na VPS `portal.izatacore.com` / host SSH com chave validada previamente como `knowledge.izatacore.com`.
- O banco de tenant-control foi consultado anteriormente e estava vazio (`tenants_total = 0`). Reconfirmar antes de mudanças operacionais.
- Não tocar em configurações secretas, bancos ou stacks de produto durante deploy sem plano de rollback.

## Próximos passos

1. Aguardar autorização explícita antes de publicar na VPS; deployment não foi solicitado nem executado nesta etapa.
2. Se autorizado, publicar com backup das imagens e configuração anterior; não alterar dados nem stacks de produto.
3. Validar login, lista vazia, criação, preflight, ativação e suspensão/retomada no painel implantado.
4. Próximas fases planejadas: gestão de memberships (função, reativação, convites e assentos), painel operacional (busca, filtros, métricas) e auditoria/reconciliação Keycloak.

## Cuidados de contexto

- O working tree contém várias alterações preexistentes ou não relacionadas em outras pastas. Preservá-las; não resetar nem limpar arquivos sem autorização.
- A alteração de `platform/tenant-control/tests/test_policies.py` e parte das mudanças no README antecedem a Fase 1 e devem ser preservadas.
- O grupo `/pulse/tenant-control-admin` foi escolhido para evitar conceder por engano os privilégios amplos do `/pulse/admin` (Grafana/Airflow).
