# Boundary de acesso para cliente externo

## Estado observado

- O Portal estático e o SSO Pulse usam OAuth2 Proxy. Há somente a conta/grupo interno provisório `pulse-invoice-demo`; não há conta nem tenant de cliente.
- O endpoint atual `fileupload-api` recebe `route_key` e arquivo, não autentica nem deriva o tenant da identidade. Não reutilizar esse endpoint para notas fiscais externas.
- O cliente escolheu acesso direto à UI do Archive. OIDC via Keycloak central está configurado e testado com identidades sintéticas; ACL por tenant só foi validada em ambiente descartável, não na instância ativa.
- A migração de isolamento foi aplicada ao warehouse ativo. Há uma membership interna de demonstração, uma fatura municipal aprovada e uma NFS-e nacional pendente; não há dados de clientes externos.

## Contrato obrigatório da aplicação

1. Autenticar a pessoa por OIDC/Keycloak. O backend usa o `sub` de um token validado ou cabeçalho criado por proxy autenticado que remove e sobrescreve cabeçalhos recebidos do cliente.
2. Resolver `sub -> tenant_key` em `tenant_memberships`. O upload, formulário, URL ou querystring não podem escolher nem alterar o tenant.
3. Para cada transação PostgreSQL, executar com role não-superuser/non-owner `pulse_invoice_runtime`; definir `pulse_invoice.keycloak_subject` e resolver o tenant, depois definir `pulse_invoice.tenant_key` com `SET LOCAL`/`set_config(..., true)` na mesma transação.
4. Todas as queries financeiras passam pela RLS de `tenant-isolation.sql`. As conexões da aplicação não podem ser superuser, proprietárias das tabelas ou ter `BYPASSRLS`.
5. Para o acesso direto ao Archive, autenticar com OIDC nativo do Paperless e sincronizar grupos Keycloak. O browser nunca recebe token de serviço/admin do Paperless.
6. Clientes não recebem a permissão global de adicionar documentos ao Paperless; uploads passam pelo Portal/API. O MVP mantém o serviço como proprietário e atribui `view` por objeto apenas ao usuário autenticado que enviou o documento. Compartilhamento seguro com todo o grupo do tenant ainda precisa ser implementado e provado antes de adicionar outros usuários.
7. Logs, erros e URLs não devem conter o PDF, texto OCR, token OIDC, CPF/CNPJ ou cabeçalhos de sessão.

O tenant de uma identidade é único no MVP (`keycloak_subject` é chave primária da tabela de associação). Se uma pessoa precisar pertencer a mais de um cliente, o fluxo de seleção de tenant deve validar a associação no servidor antes de definir o contexto RLS.

## OIDC e ACL no Paperless

O Paperless-ngx 2.20.14 suporta OIDC via django-allauth, sincronização de grupos sociais e permissões por objeto. A configuração interna de demonstração tem grupos com `view_document` global; isso não prova isolamento de documentos e não deve ser copiado para grupos de clientes. O serviço Pulse Invoice não tem `view_document` global. O MVP aplica permissão de visualização por objeto para o usuário que enviou cada documento. O modelo de ACL por grupo de tenant permanece pendente para uso multiusuário/cliente.

Não habilitar `PAPERLESS_ENABLE_HTTP_REMOTE_USER`: o Paperless alerta que um cabeçalho `Remote-User` encaminhado do cliente pode contornar o login. Preferir OIDC nativo e group sync.

O issuer central está em `https://portal.izatacore.com/keylogin/realms/pulse`; o endpoint de discovery está acessível e a console administrativa é bloqueada pelo Nginx. Existe somente o tenant interno temporário `pulse-invoice-demo`; não há tenants/clientes externos provisionados.

## Aplicação e teste

`database/tenant-isolation.sql` cria o registry/membership, a role de runtime sem login, chaves estrangeiras compostas e políticas RLS; já foi aplicada ao warehouse ativo. `api/` valida JWT, resolve `sub -> tenant_key` e aplica contexto RLS por transação. O login `pulse_invoice_api` não é owner, superuser nem `BYPASSRLS`; ele recebe apenas membership de `pulse_invoice_runtime`. A API de upload/revisão está disponível pelo Portal; sem membership ativa, qualquer identidade recebe 403.

Teste sintético em PostgreSQL descartável:

```bash
psql -v ON_ERROR_STOP=1 -f domains/pulse-invoice/tests/tenant-isolation.test.sql
```

O teste PostgreSQL prova visibilidade isolada entre dois `sub`s, acesso às views com RLS, falha fechada sem tenant e rejeição de escrita/vínculo cruzado. `domains/archive/tests/tenant_acl_test.py`, executado em Paperless 2.20.14 descartável, prova ACL por grupo para lista, busca, detalhe, download e edição, além de negar upload direto. `domains/archive/tests/tenant_oidc_test.py` valida login primeiro no Portal, reutilização da sessão em Archive, Analytics e Automation, group sync do Paperless, papel FAB do Airflow e autorização por grupos no Portal, SmartETL, DataLogger e Automation. Esses resultados não substituem a atribuição de ACL por documento nem o provisionamento de identidades reais.
