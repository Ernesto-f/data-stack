# Pulse Portal — administração de tenants

Este serviço implementa a API usada por `/admin/` no Pulse Portal. Mantém o cadastro de tenants, módulos contratados, limites de assentos e memberships; o JavaScript do Portal nunca recebe credenciais administrativas nem tokens de Keycloak.

## Responsabilidades

- **Admin Pulse**: cadastra tenant, registra host/provisionamento e habilita entitlements/assentos após validar contrato e stack.
- **Admin do tenant**: convida funcionários para a própria empresa e seleciona somente produtos habilitados e assentos disponíveis. Só owner pode nomear outro admin.
- **Keycloak**: identidade global e claims/grupos tenant-scoped. O backend usa um client service-account privado; as permissões do ator são verificadas em `tenant_memberships` antes de cada operação.
- **Odoo CRM**: container e banco separados por tenant. No primeiro SSO, cria um usuário local interno somente depois da membership/entitlement autorizada e do e-mail verificado.

O banco `tenant_control` é apenas o registro comercial de tenants, entitlements, memberships e assentos. Ele não contém dados CRM nem substitui as regras de acesso dos produtos.

## Subir a API do Portal

1. Criar `infra/.env.tenant-control` com os valores de `infra/.env.tenant-control.example` e uma senha hexadecimal local (por exemplo, `openssl rand -hex 24`), pois a senha também compõe a URL PostgreSQL da API.
2. Em `infra/.env.auth`, configurar `TENANT_CONTROL_CLIENT_ID`/`TENANT_CONTROL_CLIENT_SECRET` e credenciais SMTP. O bootstrap cria o service client com apenas roles de consulta/gestão de usuários e grupos; SMTP é necessário para convites e verificação de e-mail.
3. Executar `python3 infra/keycloak/bootstrap_realm.py` para instalar o service client e o SMTP do realm.
4. Iniciar a profile de control plane:

```bash
cd /srv/data-stack/infra
docker compose --env-file .env.auth --env-file .env.tenant-control \
  -f compose.auth.yml --profile tenant-control up -d --build
```

5. Instalar `platform/portal/nginx/portal.izatacore.com.conf` no Nginx do host; a API fica presa a `127.0.0.1:18098`. O endpoint `/tenant-admin/api/` recebe um token sobrescrito por Nginx; tokens de browser não são encaminhados como cabeçalho confiável.
6. Servir `platform/portal/admin/index.html` como `/admin/` e recarregar o Nginx.

Para habilitar o primeiro Admin Pulse, crie uma conta verificada pelo fluxo privado de `infra/keycloak/create_user.py` e inclua o grupo adicional `pulse/admin`. Não conceda esse grupo a owners/admins de clientes.

## Fluxo de ativação

1. Um Admin Pulse registra o tenant e o entitlement/limite de assentos em `/admin/`.
2. Se CRM estiver contratado, a equipe Pulse prepara o stack isolado (`setup-secrets.sh`, `initialize-db.sh`, `deploy.sh`), client/grupos Keycloak e vhost TLS. Depois o Admin Pulse ativa o tenant; a API confirma o grupo Keycloak CRM antes da ativação.
3. Um Admin Pulse convida o primeiro owner. A API cria a identidade Keycloak com verificação de e-mail; só depois da verificação o owner acessa o tenant.
4. O owner convida funcionários e seleciona produtos/assentos disponíveis. As inclusões removem/atribuem grupos Keycloak tenant-scoped. A cada primeiro login, Odoo cria a conta interna local no banco daquele tenant.
5. Para suspender um funcionário ou tenant, use o Portal. A API remove os grupos do escopo suspenso e preserva identidades/memberships de outras empresas.

O fluxo exige SMTP funcional no realm. Sem SMTP, o endpoint recusa o convite e desfaz a identidade/grupos recém-criados; ele não retorna senha temporária ao browser.

## Estado de integrações

O CRM é o único produto marcado `ready` nesta versão. Knowledge Base, Analytics e Invoice aparecem no catálogo como `planned`; a API não concede memberships desses produtos até seus backends aplicarem tenant isolation/entitlements.

O control plane ainda não conecta a um gateway de billing. A equipe Pulse registra manualmente o produto/seat limit após confirmar a contratação; a integração com cobrança deve atualizar esses entitlements por backend numa etapa posterior.
