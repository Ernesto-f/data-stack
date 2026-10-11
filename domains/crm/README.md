# Pulse CRM — Odoo Community

Pulse é uma plataforma SaaS multiempresa. O CRM usa Odoo Community 19.0; o Compose é um **template de tenant**, não uma instância compartilhada para várias empresas.

## Isolamento e entitlements

Cada empresa recebe seu próprio projeto Compose com um container Odoo, um container PostgreSQL, banco, filestore, volume e segredos exclusivos. As imagens são compartilhadas; os dados persistentes não. Um hostname identifica o tenant (`crm.izatacore.com` para o demo e `<tenant>.crm.izatacore.com` para empresas provisionadas). Backups, restauração e desligamento são feitos por tenant.

O Keycloak é compartilhado, mas identidade não equivale a acesso a todos os tenants:

- `/pulse/home` permite abrir o Portal.
- `/pulse/home/crm` representa que o produto CRM está contratado para o usuário.
- `/pulse/tenants/<tenant_key>/crm` representa a membership específica naquele tenant.
- Os grupos comerciais locais do Odoo controlam as ações e registros dentro da instância daquela empresa.

O Odoo só aceita o login quando os dois grupos CRM, o `sub`, o issuer Pulse e `email_verified=true` correspondem. No primeiro login, associa o `sub` a um usuário Odoo interno, ativo e previamente cadastrado com o mesmo e-mail verificado. Não cria contas automaticamente. Como cada instância contém dados de uma empresa, uma membership em outro tenant não permite atravessar para este banco.

O control plane do Portal deverá registrar empresa, módulos contratados, limite de assentos e memberships. O administrador de uma empresa poderá convidar/desativar seus funcionários e distribuir apenas os assentos permitidos. O serviço Knowledge Base deve verificar separadamente o entitlement do tenant e, quando houver plano por assento, a membership do usuário.

**Estado de implementação:** o template Odoo/PostgreSQL e o provisionador Keycloak por tenant existem. O Portal atual ainda é um catálogo estático: criação de tenants/assinaturas, billing, convites self-service e alocação de assentos pelo administrador do cliente ainda precisam ser implementados. Até lá, a equipe Pulse opera o provisionador; use somente tenant demo/dados sintéticos.

## SSO por tenant

Para cada tenant, o provisionador cria um client confidencial `pulse-crm-<tenant_key>`, redirect URI exato `https://<tenant-host>/auth_oauth/signin` e grupo `/pulse/tenants/<tenant_key>/crm`. O addon OCA `auth_oidc` usa Authorization Code + PKCE. O addon Pulse valida o issuer do realm, assinatura RS256, audiência, e-mail verificado, entitlement CRM e membership tenant-specific.

O código do OCA `server-auth` é baixado por commit fixado no `Dockerfile` e incluído no container com a licença. O módulo `auth_oidc` é Beta e AGPL-3; revise upstream e licença antes de atualizar o pin.

## Provisionar um tenant demo/dev

Pré-requisitos: Keycloak/OAuth2 Proxy operacionais e arquivo `/srv/data-stack/infra/.env.auth` privado com credenciais administrativas do realm. O provisionador usa esse arquivo localmente, cria credenciais aleatórias por tenant e não as imprime.

```bash
cd /srv/data-stack/domains/crm
bash ./setup-secrets.sh demo 18095
bash ./initialize-db.sh demo
bash ./deploy.sh demo
```

Para uma nova empresa, escolha um slug estável e uma porta de loopback ainda livre:

```bash
bash ./setup-secrets.sh acme 18101
bash ./initialize-db.sh acme
bash ./deploy.sh acme
```

O provisionador cria `tenants/<slug>/.env`, `config/odoo.conf`, o client e grupo Keycloak e um vhost Nginx em `tenants/<slug>/nginx.conf`. Esses arquivos locais têm permissões restritas e são ignorados pelo Git. O tenant `demo` usa `https://crm.izatacore.com`; `acme` usa `https://acme.crm.izatacore.com`.

Instale o vhost gerado no Nginx do host, emita o certificado TLS, execute `nginx -t` e recarregue o serviço. O Odoo escuta somente em loopback e cada Compose project tem rede e volumes próprios. PostgreSQL não publica porta no host.

`initialize-db.sh <tenant>` instala `crm`, `auth_oidc` e `pulse_crm_sso` numa base nova e define a senha inicial local. Ele recusa uma base existente para não redefinir a senha do administrador. `deploy.sh <tenant>` compila/inicia somente aquele tenant. Não execute `docker compose down -v` sem backup aprovado.

## Usuários de uma empresa

Até existir o Portal de administração de tenants, a equipe Pulse cria as identidades pelo Keycloak e associa os dois grupos aplicáveis: `/pulse/home/crm` e `/pulse/tenants/<tenant_key>/crm`. Por exemplo, para `acme`, `/pulse/tenants/acme/crm`. Em seguida, um administrador daquela instância provisiona o usuário Odoo interno e seus grupos locais CRM. O primeiro login SSO liga a conta pelo e-mail verificado.

O administrador de `acme` não pode convidar usuários para outro tenant nem aumentar seu próprio entitlement. Esse fluxo self-service deverá ser implementado no control plane do Portal, com autorização no backend; não expor credenciais administrativas do Keycloak ao browser ou ao Odoo.

## Segurança e operação

- Não usar a instância `demo` para clientes, dados reais ou mais de uma empresa.
- Odoo ACLs são locais à instância/tenant. Produtos compartilhados (Knowledge Base, Invoice, Archive e Analytics) precisam validar seus próprios entitlements e isolamento por tenant.
- Nunca aceitar `tenant_key` da URL/request como autorização; a instância e a membership devem determinar o tenant no servidor.
- Não publicar Odoo/PostgreSQL sem o Nginx/TLS e o gate Keycloak do tenant.
- Para estado/logs, use o Compose e diretório do tenant: `docker compose --env-file tenants/<slug>/.env -p pulse-crm-<slug> -f docker-compose.yml ps` e `logs -f`.
- Tenants de produção precisam de servidor, volumes, segredos, backups, monitoramento e política de retenção contratados e separados do demo/dev.
