# Identidade compartilhada da plataforma

## Direção

Uma única instância Keycloak será o provedor de identidade compartilhado por todos os serviços. Cada aplicação recebe seu próprio OIDC client e roles/grupos de autorização; aplicações sem suporte OIDC nativo podem ficar atrás do OAuth2 Proxy. O Paperless-ngx usa OIDC nativo e sincronização de grupos para aplicar ACLs por documento.

## Artefatos existentes

- Keycloak está fixado em `26.7.0` por `infra/keycloak/Dockerfile`.
- OAuth2 Proxy está fixado em `v7.15.3` por `infra/oauth2-proxy/Dockerfile`.
- `infra/compose.auth.yml` descreve o realm compartilhado `pulse`, o client OAuth2 Proxy e as redes herdadas.
- As regras Nginx usam grupos como `/pulse/home/airflow` e `/pulse/home/cloudbeaver`.

## URL e fronteira pública desejadas

- Issuer público: `https://portal.izatacore.com/keylogin/realms/pulse`.
- As portas Keycloak `18083` e OAuth2 Proxy `4180` estão vinculadas somente a `127.0.0.1`; o Nginx público é o único caminho de entrada. Token/JWKS entre OAuth2 Proxy e Keycloak usam a rede Docker privada.
- OAuth2 Proxy valida o issuer e atende `auth_request` do Nginx.
- Cookie OAuth2 Proxy é Secure e o callback é HTTPS.
- A console administrativa Keycloak não fica pública. A porta `18083` permanece em `127.0.0.1` na VPS; para administrá-la, abra um túnel SSH local e acesse `http://localhost:18083/keylogin/admin/`.
- Remover configurações legadas de HTTP/insecure-email ao migrar os clients, sem expor segredos em repositório.

## Convenção de clients e grupos

- Um client OAuth/OIDC por aplicação nativa, com redirect URIs restritos ao domínio/callback exato. O provisionador cria um client Odoo por tenant, `pulse-crm-<tenant_key>`.
- O Pulse CRM/Odoo usa o módulo OCA `auth_oidc` com Authorization Code + PKCE e cliente confidencial. Cada tenant tem callback próprio `https://<tenant-host>/auth_oauth/signin`; não habilitar o fluxo implícito.
- O OAuth2 Proxy mantém seu próprio client para as rotas autenticadas pelo Nginx.
- Grupos de autorização de aplicação permanecem separados de grupos de tenant. Para o CRM, `/pulse/home/crm` identifica entitlement do produto e `/pulse/tenants/<tenant_key>/crm` identifica membership no tenant.
- Solicitar a claim `groups` apenas aos clients que precisam dela. O backend de Invoice ainda verifica o `sub` validado contra `tenant_memberships` e define o contexto PostgreSQL; nunca confia num `tenant_key` enviado pelo navegador.
- Paperless recebe grupos OIDC com nomes correspondentes aos grupos Paperless; ACL `view`/`change` é atribuída por documento ao grupo do tenant. A associação ao grupo, sozinha, não altera a ACL dos documentos.

## Situação da migração

O stack compartilhado original foi implantado na VPS usando Keycloak `26.7.0` e OAuth2 Proxy `v7.15.3`. A VPS mantém um banco Keycloak novo em `/srv/data-stack/infra/volumes/keycloak_db`; o volume local `infra/volumes/keycloak_db` foi preservado e não foi copiado. O realm `pulse` foi criado sem importar usuários/clientes do endpoint antigo `10.113.120.120`.

O issuer público `https://portal.izatacore.com/keylogin/realms/pulse` responde discovery OIDC. `/keylogin/admin/` retorna 404 externamente; a URL administrativa usa `localhost` e só responde pela porta de loopback `18083`, acessível via túnel SSH. OAuth2 Proxy está limitado ao loopback na porta `4180`, usa cookie Secure compartilhado em `.izatacore.com` e inicia o fluxo OIDC pelo issuer Pulse. O Proxy disponibiliza o access token a `auth_request`; o Nginx bloqueia o endpoint público `/oauth2/auth` para manter esse cabeçalho interno. Portal, SmartETL, DataLogger, Automation e CRM usam grupos de rota específicos. No CRM, o próprio Odoo também exige o grupo no ID token.

O realm base contém os clients `pulse-oauth2-proxy`, `pulse-archive`, `pulse-analytics`, `pulse-samia` e `pulse-automation`. O provisionador CRM cria um client confidential e o grupo `/pulse/tenants/<tenant_key>/crm` por tenant; `/pulse/home/crm` é o entitlement geral do produto. Odoo usa OCA `auth_oidc`; cada instância e banco ficam separados por tenant. A conta interna Odoo é local à instância e só é associada a um `sub` com e-mail verificado e membership daquele tenant.
