# Pulse Archive — Paperless-ngx

Paperless-ngx está publicado como **Pulse Archive** em `https://archive.izatacore.com`.

## Instância na VPS

- Paperless-ngx `2.20.14`, PostgreSQL 16 e Redis 7 em uma rede Compose própria.
- A instância foi criada vazia; nenhum documento ou banco local foi transferido.
- Frontend e login nativo estão atrás de HTTPS. A porta do container está vinculada somente ao loopback da VPS (`127.0.0.1:18095`).
- Cadastros estão desabilitados. A conta inicial é `admin`.
- Título da aplicação: **Pulse Archive**. A interface e o favicon usam os assets Pulse padrão, servidos pelo Nginx.
- Segredos, inclusive a senha admin gerada, ficam em `/srv/data-stack/domains/archive/.env` com permissão `600`; não estão no Git nem na imagem.

Para conferir os containers:

```bash
docker compose -f /srv/data-stack/domains/archive/docker-compose.yml ps
docker compose -f /srv/data-stack/domains/archive/docker-compose.yml logs -f paperless
```

Para consultar a senha admin gerada, leia apenas a variável `PAPERLESS_ADMIN_PASSWORD` do `.env` privado na VPS.

## Instância local existente

A instância anterior permanece intacta no computador local em `chatboot/v2-weknora-custom/WeKnora`, com os diretórios persistentes `weknora-paperless-data/{data,media,export}`. Se houver documentos demo/dev necessários no Archive da VPS, importe-os conscientemente depois.

## Isolamento

O Compose local anterior descreve uma instância compartilhada. O filtro por tags protege o fluxo de ingestão, mas não isola a lista de documentos na UI entre clientes. Não crie contas para clientes nem trate uma instância única como multi-tenant sem configurar e validar permissões Paperless independentes.

## Acesso externo planejado

O cliente escolheu acesso direto à UI do Archive pelo Keycloak central. Paperless 2.20.14 está configurado com OIDC e sincronização de grupos; um teste com duas identidades sintéticas confirmou o group sync. O ACL por documento continua necessário: group sync não aplica permissões aos documentos automaticamente. Cada documento precisa ter `view`/`change` para o grupo do tenant, e clientes não recebem permissão global de adicionar documentos. O upload deverá passar pelo Pulse Invoice Portal/API, que define owner e ACL do tenant.

O issuer central está publicado em `https://portal.izatacore.com/keylogin/realms/pulse`. Nenhuma conta real de cliente ou grupo de tenant foi criado; o login local do Paperless permanece como acesso de contingência até validar o rollout completo.

O grupo `/pulse/home/archive` concede apenas as permissões básicas de UI (`view_uisettings` e `view_document`). A visibilidade de cada documento continua dependendo da ACL de objeto para o grupo específico do tenant; essa associação não amplia acesso aos documentos por si só.

Os testes `tests/tenant_acl_test.py` e `tests/tenant_oidc_test.py` usam identidades e PDFs sintéticos. O primeiro valida ACL por documento em stack descartável; o segundo faz login no Portal primeiro e confirma que a sessão Keycloak é reutilizada no Archive e Analytics, além de verificar group sync e grupos OAuth2 Proxy. Contas, grupos e usuário temporário do Grafana são removidos ao final.
