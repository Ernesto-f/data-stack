# Pulse CRM — Odoo Community

O Pulse CRM usa Odoo Community 19.0 com o aplicativo CRM e login OIDC pelo Keycloak Pulse. O Compose atual é **uma única instância demo/dev** para validar Odoo e SSO; não o use com duas empresas nem com dados reais.

## Modelo SaaS multiempresa desejado

Pulse é uma plataforma de módulos contratáveis por empresa. Cada tenant compra produtos (por exemplo CRM ou Knowledge Base); alguns produtos podem ser incluídos para toda a empresa e outros podem ter assentos atribuídos a usuários específicos. Usuários e memberships são identificados pelo `sub` do Keycloak, nunca apenas pelo e-mail.

Para o CRM, a recomendação inicial é provisionar **um serviço Odoo e uma base PostgreSQL por tenant**, com volumes, segredos, backup e URL próprios. A imagem Odoo pode ser compartilhada, mas dados e credenciais ficam separados. Assim, uma falha de regra `company_id` numa instância não permite ler os registros de outra empresa. Para cerca de dez empresas, essa fronteira é direta de operar; o custo é mais containers, atualizações e backups.

O fluxo alvo é:

1. Criar a empresa/tenant no cadastro de assinaturas e habilitar entitlements contratados.
2. Criar o administrador da empresa no Keycloak e vinculá-lo ao tenant e aos produtos pagos.
3. O administrador convida/desativa funcionários dentro da própria empresa e aloca assentos permitidos; nunca administra outro tenant.
4. Provisionar a instância Odoo do tenant e os usuários internos/grupos locais de CRM. O Keycloak continua sendo a identidade global; Odoo aplica as funções comerciais.
5. Ao contratar Knowledge Base, habilitar o entitlement da empresa e, se o plano for por assento, somente as memberships escolhidas. O gateway e o serviço Knowledge Base verificam o entitlement, não apenas a presença de um card no Portal.

No modelo final, a autorização precisa incluir tenant + produto + usuário/assento. `/pulse/home/crm` é apenas o gate geral desta demo. Instâncias de cliente deverão usar membership/grupo de tenant específico e callback OIDC exato; um client ou grupo global de CRM não pode autorizar acesso entre empresas.

**Ainda não implementado:** cadastro de tenants/planos, convite de funcionários por administrador do cliente, alocação de assentos, criação automatizada de instâncias e memberships tenant-scoped. A instância e o client únicos abaixo são só a prova técnica inicial de Odoo + SSO.

| Item | Configuração |
| --- | --- |
| URL | `https://crm.izatacore.com` |
| Keycloak client | `pulse-crm` |
| Grupo obrigatório | `/pulse/home/crm` |
| Odoo/PostgreSQL | Compose próprio; PostgreSQL 16 e Odoo publicados no host somente em `127.0.0.1:18095` |
| OIDC | OCA `auth_oidc`, Authorization Code + PKCE, cliente confidencial |

## Autorização de usuários

O Nginx exige `/pulse/home/crm` antes de encaminhar tráfego ao Odoo. O addon `pulse_crm_sso` também confere o grupo no ID token, exige `email_verified=true` e valida o issuer Pulse. No primeiro login, ele vincula o `sub` Keycloak a exatamente um usuário Odoo interno, ativo e previamente criado com o mesmo e-mail verificado. Logins posteriores usam o `sub` estável.

O fluxo falha fechado se não encontrar exatamente um usuário elegível, se o e-mail não estiver verificado, se faltar o grupo ou se a identidade já estiver vinculada a outra conta. O SSO não cria usuários automaticamente nem concede funções do Odoo: administradores do Odoo provisionam usuários internos e atribuem grupos locais do CRM. O perfil de usuário e seus grupos locais continuam sendo a autorização efetiva dentro do Odoo.

## OIDC e confiança

O client Keycloak é `pulse-crm`, confidential, com Standard Flow habilitado e redirect URI exato:

```text
https://crm.izatacore.com/auth_oauth/signin
```

O OCA `auth_oidc` implementa o fluxo Authorization Code com PKCE e valida assinatura RS256, audiência e validade do ID token. O addon Pulse ainda exige o issuer `https://portal.izatacore.com/keylogin/realms/pulse`, o grupo de CRM e e-mail verificado. O código do OCA `server-auth` é obtido por commit fixado no `Dockerfile`; o módulo `auth_oidc` é Beta e AGPL-3. Revise a versão/licença antes de atualizar o pin.

### Registrar o client no Keycloak

1. No arquivo privado `/srv/data-stack/infra/.env.auth`, definir `ODOO_OIDC_CLIENT_ID=pulse-crm` e criar `ODOO_OIDC_CLIENT_SECRET` aleatório e exclusivo.
2. Executar `python3 /srv/data-stack/infra/keycloak/bootstrap_realm.py`. Ele cria o client e o grupo `/pulse/home/crm`; não copiar o segredo para Git, tickets ou comandos exibidos em logs.
3. Provisionar o usuário operador com `/pulse/home/crm` usando `infra/keycloak/create_user.py`.

### Configurar o provider Odoo

O módulo cria o provider **Pulse SSO** desabilitado. No Odoo, entre como administrador local, ative o modo desenvolvedor e abra **Settings → Users & Companies → OAuth Providers**, informe o segredo privado do client e habilite o provider. Confirme:

- Auth Flow: **OpenID Connect (authorization code flow)**
- Client ID: `pulse-crm`
- Scope: `openid email profile`
- Authorization URL: `https://portal.izatacore.com/keylogin/realms/pulse/protocol/openid-connect/auth`
- Token URL: `https://portal.izatacore.com/keylogin/realms/pulse/protocol/openid-connect/token`
- JWKS URL: `https://portal.izatacore.com/keylogin/realms/pulse/protocol/openid-connect/certs`
- Body/button: `Entrar com Pulse`

O client scope `groups` é adicionado por padrão pelo bootstrap compartilhado Keycloak; a claim com caminhos completos permite que Odoo e Nginx exijam `/pulse/home/crm`.

## Inicialização da instância demo/dev single-tenant

Na VPS, com o repositório em `/srv/data-stack`:

```bash
cd /srv/data-stack/domains/crm
bash ./setup-secrets.sh
docker compose build odoo
bash ./initialize-db.sh
bash ./deploy.sh
```

`setup-secrets.sh` cria `.env` e `config/odoo.conf` locais com permissões restritas. `initialize-db.sh` instala `crm`, `auth_oidc` e `pulse_crm_sso` numa base nova e define a senha inicial do administrador local. Execute-o somente uma vez; ele recusa bases existentes para não sobrescrever a senha do administrador. `deploy.sh` inicia os serviços depois da configuração inicial.

O arquivo `nginx/crm.izatacore.com.conf` configura TLS, gate Keycloak/OAuth2 Proxy e proxy para `127.0.0.1:18095`. Instale-o no Nginx do host, valide com `nginx -t` e recarregue o serviço. O serviço Odoo não publica porta em interfaces externas e o PostgreSQL não publica porta no host.

Após subir o CRM:

1. Adicione o operador Odoo como usuário **Internal User** e atribua os grupos Sales/CRM apropriados.
2. Para o primeiro acesso administrativo, use o login local `admin` e a senha inicial `CRM_INITIAL_ADMIN_PASSWORD` do `.env` privado do CRM. Crie um usuário interno pessoal; não compartilhe a conta `admin`.
3. Confirme que o e-mail do usuário Odoo corresponde ao e-mail verificado no Keycloak.
4. Entre em `https://crm.izatacore.com` com uma identidade no grupo `/pulse/home/crm` e escolha **Entrar com Pulse**.
5. Confirme que o Odoo vinculou essa identidade somente ao usuário interno provisionado. Teste também usuário sem grupo, e-mail não verificado e usuário Odoo ausente: todos devem ser negados.

## Dados e operação

- Use apenas dados sintéticos/autorizados. Não importar listas de clientes reais para a demonstração.
- CRM é interno; Odoo ACLs e grupos locais não equivalem a isolamento entre organizações.
- Invoice, Archive, Samia e Analytics mantêm suas próprias políticas. O CRM não escolhe `tenant_key` e não recebe tokens de serviço desses produtos.
- Não publicar a interface de administração Odoo, o PostgreSQL, o `.env` nem `config/odoo.conf`.
- Para verificar estado: `docker compose ps`; para logs: `docker compose logs -f odoo crm-db`.
- Preserve os volumes `crm-db-data` e `crm-odoo-data`. Não execute `docker compose down -v` sem backup aprovado.
- Dados de produção de cliente devem ir para infraestrutura e banco separados, nunca para este Compose demo/dev.

## Integrações futuras

Começar por links contextuais e integração backend somente leitura. Antes de sincronizar empresas, contatos ou oportunidades com outros produtos, definir dono e finalidade de cada campo, consentimento/retention, IDs imutáveis, idempotência, escopos de serviço e testes de autorização negativa. O CRM não é fonte de autorização para dados documentais ou financeiros.
