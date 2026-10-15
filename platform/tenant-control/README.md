# Pulse Portal — administração de tenants

Este serviço implementa a API usada por `/admin/` no Pulse Portal. Mantém o cadastro de tenants, módulos contratados, limites de assentos e memberships; o JavaScript do Portal nunca recebe credenciais administrativas nem tokens de Keycloak.

## Responsabilidades

- **Admin do control plane** (`/pulse/tenant-control-admin`): cadastra tenant, registra host/provisionamento e habilita entitlements/assentos após validar contrato e stack. O grupo é separado dos papéis administrativos do Grafana e Airflow.
- **Admin Pulse legado** (`/pulse/admin`): continua autorizado no control plane para compatibilidade, mas também concede papel Admin no Grafana e Airflow; não é o grupo recomendado para novos administradores de tenants.
- **Admin do tenant**: convida funcionários para a própria empresa e seleciona somente produtos habilitados e assentos disponíveis. Só owner pode nomear outro admin.
- **Keycloak**: identidade global e claims/grupos tenant-scoped. O backend usa um client service-account privado; as permissões do ator são verificadas em `tenant_memberships` antes de cada operação.
- **SALUS CRM**: Odoo customizado, container e banco separados por tenant. No primeiro SSO, cria um usuário local interno somente depois da membership/entitlement autorizada e do e-mail verificado.

O banco `tenant_control` é apenas o registro comercial de tenants, entitlements, memberships e assentos. Ele não contém dados CRM nem substitui as regras de acesso dos produtos.

## Subir a API do Portal

1. Criar `infra/.env.tenant-control` com os valores de `infra/.env.tenant-control.example` e uma senha hexadecimal local (por exemplo, `openssl rand -hex 24`), pois a senha também compõe a URL PostgreSQL da API.
2. Em `infra/.env.auth`, configurar `TENANT_CONTROL_CLIENT_ID`/`TENANT_CONTROL_CLIENT_SECRET` e credenciais SMTP. O bootstrap cria o service client com apenas roles de consulta/gestão de usuários e grupos; SMTP é necessário para convites e verificação de e-mail.
3. Executar `python3 infra/keycloak/bootstrap_realm.py` para instalar o service client e o SMTP do realm.
4. Iniciar a profile de control plane:

```bash
cd /srv/data-stack/infra
docker compose --env-file .env.auth --env-file .env.tenant-control \
  -f compose.auth.yml -f compose.tenant-control.yml --profile tenant-control up -d --build
```

5. Instalar `platform/portal/nginx/portal.izatacore.com.conf` no Nginx do host; a API fica presa a `127.0.0.1:18098`. O endpoint `/tenant-admin/api/` recebe um token sobrescrito por Nginx; tokens de browser não são encaminhados como cabeçalho confiável.
6. Servir `platform/portal/admin/index.html` como `/admin/` e recarregar o Nginx.

O container da API possui healthcheck Docker em `/health`; ele confirma que o processo subiu depois da criação do pool/schema do banco. A disponibilidade do Keycloak é verificada sob demanda e retorna `503` quando uma operação depende do provedor.

Para habilitar um administrador do control plane, associe a conta verificada ao grupo `pulse/tenant-control-admin` no Keycloak. O script privado `infra/keycloak/create_user.py` oferece esse grupo na criação de contas. Não conceda grupos administrativos globais a owners/admins de clientes.

## Fluxo de ativação

1. Um Admin do control plane registra a empresa e o entitlement/limite de assentos em `/admin/`; o tenant começa em `provisioning`.
2. Se CRM estiver contratado, a equipe Pulse prepara o stack isolado (`setup-secrets.sh`, `initialize-db.sh`, `deploy.sh`), client/grupos Keycloak e vhost TLS. A tela executa o preflight de host e grupos Keycloak; o operador também confirma a disponibilidade da stack antes de ativar.
3. Um Admin Pulse convida o primeiro owner. A API cria a identidade Keycloak com verificação de e-mail; só depois da verificação o owner acessa o tenant.
4. O owner convida funcionários e seleciona produtos/assentos disponíveis. As inclusões removem/atribuem grupos Keycloak tenant-scoped. A cada primeiro login, Odoo cria a conta interna local no banco daquele tenant.
5. O Admin do control plane pode suspender/reativar o tenant. A API sincroniza os grupos dos membros para remover/restaurar acesso somente nesse tenant e tenta restaurar o estado anterior caso a sincronização falhe.
6. Admins autorizados podem alterar a função e os produtos/assentos de um membro; somente owner ou platform admin podem nomear administradores e somente platform admin pode alterar o owner. A API impede remover o último owner ativo.
7. Um membro suspenso pode ser reativado no Portal com nova seleção de produtos; a API valida entitlement/assentos e sincroniza os grupos Keycloak, revertendo a reativação se a sincronização falhar.
8. Para suspender um funcionário, use o Portal. A API remove os grupos do escopo suspenso e preserva identidades/memberships de outras empresas.
9. Operações administrativas concluídas são registradas em `tenant_control_audit`; a listagem é exclusiva para platform admins e não armazena e-mail dos membros.

O fluxo exige SMTP funcional no realm. Sem SMTP, o endpoint recusa o convite e desfaz a identidade/grupos recém-criados; ele não retorna senha temporária ao browser.

## Estado de integrações

O SALUS CRM é o único produto marcado `ready` nesta versão. Knowledge Base, Analytics e Invoice aparecem no catálogo como `planned`; a API não concede memberships desses produtos até seus backends aplicarem tenant isolation/entitlements.

O control plane ainda não conecta a um gateway de billing. A equipe Pulse registra manualmente o produto/seat limit após confirmar a contratação; a integração com cobrança deve atualizar esses entitlements por backend numa etapa posterior.
