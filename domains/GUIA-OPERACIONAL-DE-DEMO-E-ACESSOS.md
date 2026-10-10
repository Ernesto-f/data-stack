# Guia operacional de demonstrações e acessos Pulse

## Objetivo e escopo

Este guia orienta a preparação de contas e demonstrações dos produtos Pulse no ambiente **demo/dev**. O ambiente atual fica em uma VPS compartilhada para desenvolvimento e demonstração; não é o ambiente de produção de um cliente. Produção, credenciais, volumes e dados devem ser implantados separadamente para cada cliente.

O princípio operacional é conceder o menor acesso necessário, validar o login e a autorização no próprio produto e ampliar permissões apenas com aprovação explícita. Uma associação a grupo Keycloak pode liberar uma URL, mas não cria necessariamente uma conta, função, tenant ou ACL dentro da aplicação.

## Inventário de produtos e URLs

| Produto | URL demo/dev | Autenticação e acesso mínimo | Provisionamento adicional / observação |
| --- | --- | --- | --- |
| Pulse Portal | <https://portal.izatacore.com/> | Keycloak + OAuth2 Proxy; grupo `/pulse/home` | Contém atalhos para os produtos. Não substitui a autorização no produto de destino. |
| Pulse Archive | <https://archive.izatacore.com> | OIDC nativo do Keycloak; grupo `/pulse/home/archive` para o acesso básico à UI | Conta Paperless e ACL por documento/grupo são distintas. ACL ainda deve ser atribuída e testada; não presumir isolamento entre clientes. |
| Pulse SmartETL | <https://smartetl.izatacore.com> | OAuth2 Proxy exige `/pulse/home/smartetl` | Cada convidado usa sua própria identidade Pulse. |
| Pulse DataLogger | <https://datalogger.izatacore.com> | OAuth2 Proxy exige `/pulse/home/datalogger` | Node-RED inicia em safe mode; flows e integrações de escrita não devem ser ativados sem revisão de endpoints e credenciais. |
| Pulse Automation / Airflow | <https://automation.izatacore.com/airflow/> | OAuth2 Proxy exige `/pulse/home/airflow`; Airflow faz login OIDC nativo | Airflow cria usuário FAB no primeiro login como Viewer. Grupos `/pulse/home/airflow` → User; `/pulse/home/airflow-admin` ou `/pulse/admin` → Admin, conforme `webserver_config.py`. O script interativo de usuário não oferece hoje esses grupos. |
| Pulse Analytics / Grafana | <https://analytics.izatacore.com> | OAuth2 Proxy exige `/pulse/home/analytics`, seguido de login OIDC nativo | Mapeamento Grafana: `/pulse/admin` → Admin; `/pulse/home/analytics-admin` → Editor; demais usuários autenticados → Viewer. Conceder também o grupo de acesso Analytics a quem necessitar de papel Editor/Admin. Grafana é operacional/interno; não expor dashboards globais a clientes. |
| Samia / WeKnora | <https://knowledge.izatacore.com> | OIDC nativo; na tela de login, escolher **Pulse** | Cadastro self-service está habilitado no demo/dev. Conta da aplicação é vinculada ao e-mail OIDC e ao tenant interno do WeKnora; confirmar escopo dos dados antes de uma demonstração com conteúdo de cliente. |
| Pulse Invoice | <https://portal.izatacore.com/pulse-invoice/> | Sessão Pulse via Portal e API com token validado | Só existe tenant/membership interno `pulse-invoice-demo`; não há tenant externo. Não reutilizar o endpoint `fileupload-api` antigo para notas fiscais externas. |
| Pulse CRM / Odoo | <https://crm.izatacore.com> | OAuth2 Proxy exige `/pulse/home/crm`; Odoo usa OCA OpenID Connect por Authorization Code + PKCE | Provisionar usuário interno no Odoo com e-mail verificado igual ao Keycloak e grupos locais Sales/CRM. Não há criação automática de contas. |

URLs e políticas podem mudar; antes de uma sessão externa, confronte esta tabela com as configurações Nginx/Compose atuais e faça um login de teste. CloudBeaver, Guacamole e outras ferramentas administrativas não fazem parte do roteiro de cliente: seus grupos e rotas dão acesso a recursos operacionais.

## Arquitetura de identidade e autenticação

- Realm compartilhado: `pulse`; issuer OIDC: `https://portal.izatacore.com/keylogin/realms/pulse`.
- Keycloak e OAuth2 Proxy são publicados pelo Nginx. As portas administrativas `18083` (Keycloak) e `4180` (OAuth2 Proxy) ficam limitadas a loopback na VPS.
- A administração do Keycloak é feita por túnel SSH, seguindo `infra/AUTH-ARCHITECTURE.md`; a console administrativa não deve ser publicada na Internet.
- Serviços sem OIDC nativo podem usar OAuth2 Proxy/Nginx. Aplicações com OIDC nativo mantêm também funções, cadastro e permissões próprias.
- O Portal exige `/pulse/home`. Os grupos de produto são controles de autorização de rota; não são grupos de tenant por si só.
- Nunca copiar credenciais, tokens, PDFs ou conteúdo de cliente para este repositório ou para logs. Segredos ficam em arquivos `.env` privados no servidor.

## Grupos Keycloak e provisionamento atual

O script `infra/keycloak/create_user.py` é o fluxo atual para criar usuário. É interativo e exige terminal SSH alocado; valida e-mail informado, define senha temporária com pelo menos 16 caracteres e força troca no primeiro login. O grupo `/pulse/home` é incluído automaticamente.

Grupos permitidos pelo script no estado atual:

| Grupo | Uso documentado |
| --- | --- |
| `/pulse/home` | Portal; requerido pelo fluxo do script |
| `/pulse/home/smartetl` | Rota SmartETL protegida por OAuth2 Proxy |
| `/pulse/home/datalogger` | Rota DataLogger protegida por OAuth2 Proxy |
| `/pulse/home/analytics` | Grupo requerido pelo gate OAuth2 Proxy do Analytics |
| `/pulse/home/analytics-admin` | Papel Editor no Grafana |
| `/pulse/admin` | Administração Pulse e papel Admin no Grafana; não conceder para demonstração padrão |
| `/pulse/home/archive` | Acesso básico/grupo OIDC do Archive; ACL de cada documento permanece separada |
| `/pulse/home/crm` | Acesso à rota do Pulse CRM/Odoo; autorização interna também depende dos grupos locais do Odoo |

Outros grupos existem no bootstrap e em configuração de serviços — por exemplo `/pulse/home/airflow`, `/pulse/home/airflow-admin`, `/pulse/home/cloudbeaver` e `/pulse/home/guacamole` — mas não são aceitos pelo script interativo. Não contornar essa validação nem editar grupos diretamente sem procedimento de operador revisado. O acesso ao Airflow requer ainda correspondência entre o gate Nginx e os papéis FAB.

### Criar uma conta demo

1. Registre o pedido: pessoa responsável, finalidade, produtos, duração da conta e aprovador. Use um endereço de e-mail cuja posse possa ser confirmada.
2. Conecte-se à VPS em terminal interativo conforme a orientação operacional vigente. O script informa a conexão `ssh -tt sam@179.198.221.68` quando não há TTY.
3. Execute `python3 infra/keycloak/create_user.py` a partir do checkout implantado/atualizado do repositório.
4. Informe usuário, e-mail, nome e sobrenome. Para acesso somente ao Portal, deixe os grupos adicionais vazios. Acrescente apenas grupos disponíveis no script e aprovados para o caso.
5. Confirme a posse do e-mail e defina uma senha temporária única. Entregue-a por canal privado; não a registre em ticket, chat público ou documentação.
6. Peça à pessoa para entrar em `https://portal.izatacore.com/` e trocar a senha temporária.
7. Teste a URL e a função com a própria identidade criada. Para produtos com autorização interna, complete também a associação/ACL descrita abaixo.
8. Registre quem aprovou e quando revisar/remover a conta. Ao encerrar a demo, desabilite ou remova a identidade e revogue também acessos e compartilhamentos no produto.

O script cria identidade e associa grupos Keycloak apenas. Não cria contas/roles locais do Paperless ou Odoo, tenant Invoice, ACL documental nem Connections do Airflow.

## Acesso mínimo e operação por produto

### Portal

Conceder somente `/pulse/home` como base. A autenticação do Portal habilita a navegação e a sessão SSO; cada aplicação de destino deve continuar aplicando suas próprias permissões.

### Pulse Archive

Usar OIDC Pulse. O grupo `/pulse/home/archive` permite acesso básico à UI e group sync; não dá visibilidade automática a todos os documentos. Atribuir ACL `view` ou `change` somente aos objetos e grupos autorizados, validar listagem, busca, detalhe, download e edição com duas identidades de teste. O upload de clientes deve passar pelo Portal/API autorizado. A conta local administrativa é contingência de operador, não uma conta para demonstração.

O Archive ativo ainda não deve ser tratado como multi-tenant pronto para clientes. O teste de ACL em stack descartável não equivale a habilitar e verificar ACL por tenant na instância ativa.

### Pulse SmartETL

Adicionar `/pulse/home/smartetl` e testar o redirecionamento/login OIDC. Demonstrar com documentos sintéticos ou autorizados; saídas JSON requerem revisão antes de importação para Samia.

### Pulse DataLogger

O grupo `/pulse/home/datalogger` protege a rota OAuth2 Proxy. Não conceder edição de flows nem ativar execuções agendadas/escritas em PLC ou banco sem uma revisão operacional explícita. O modo atual é safe mode.

### Pulse CRM / Odoo

Exigir `/pulse/home/crm` e usar o botão **Entrar com Pulse**. O addon OIDC valida issuer, assinatura/audiência, e-mail verificado e grupo CRM. Um login só pode ser associado a um usuário Odoo interno, ativo e previamente provisionado com o mesmo e-mail; grupos locais do Odoo determinam os acessos a CRM. O ambiente é interno e não tem isolamento multi-tenant.

### Airflow / Pulse Automation

O grupo de acesso à rota é `/pulse/home/airflow`; dentro do Airflow, o login OIDC cria perfil FAB como Viewer por padrão e sincroniza papéis no login. O código eleva `/pulse/home/airflow` a User e `/pulse/home/airflow-admin` ou `/pulse/admin` a Admin. Como o provisionador atual não aceita esses grupos, a criação/associação deve ser feita pelo operador autorizado conforme o procedimento Keycloak vigente, e testada nas duas camadas. Não conceder Admin a convidados. As DAGs permanecem pausadas até Connections de teste serem revisadas.

### Analytics / Grafana

Grafana usa OIDC e mapeia grupos para Viewer, Editor e Admin. A rota Nginx também exige `/pulse/home/analytics`; conceda esse grupo junto ao papel adicional de Editor/Admin quando necessário. Conceder Editor apenas para manutenção aprovada; Admin é acesso operacional. Revisar dashboards/datasources para remover dados ou métricas internas antes de qualquer demonstração externa.

### Samia / WeKnora

Entrar pelo botão Pulse no OIDC. Self-registration está habilitado no ambiente demo/dev. Validar previamente qual tenant e quais bases de conhecimento a identidade enxerga; não carregar documentos reais de um cliente em um tenant compartilhado sem isolamento confirmado. Para uma demo controlada, prefira conta previamente criada e conjunto sintético aprovado.

### Pulse Invoice

A API valida token OIDC, deriva o tenant de `sub` por meio de `tenant_memberships` e aplica RLS no PostgreSQL. Não aceitar tenant vindo de formulário, URL ou querystring. O tenant atual é interno e só há dados de demonstração; nenhuma identidade externa deve ser associada ao tenant interno. Embora os testes descartáveis de RLS/API tenham passado, ACL multi-tenant no Archive ativo segue pendente. Não usar com cliente ou documentos reais antes de completar e validar o isolamento ponta a ponta.

## Ampliar permissões com controle

1. Identifique a ação concreta necessária e o recurso correspondente; evite pedidos genéricos como “admin”.
2. Confirme se o controle é Keycloak, proxy, papel interno, tenant, ACL por objeto ou credencial de integração.
3. Obtenha aprovação do responsável do produto/dado e aplique a permissão mais estreita possível, com prazo de revisão.
4. Teste uma ação permitida e uma negada com a identidade alvo; registre o resultado sem dados sensíveis.
5. Remova o acesso elevado após a atividade e repita teste negativo. Revise acessos expirados periodicamente.

## Roteiro de demonstração

1. Defina objetivo, participantes, produtos e limites dos dados que serão exibidos.
2. Use contas individuais e conjunto de dados sintético ou autorizado. Não usar a conta administrativa como identidade de convidado.
3. Teste login, logout, SSO, links, papel mínimo e comportamento de negação antes da reunião.
4. Demonstre primeiro o Portal e depois apenas os produtos autorizados. Não navegar em consoles administrativas, documentos de outros tenants ou dashboards internos.
5. Para Invoice, apresentar apenas o fluxo demo/dev e deixar claro que o isolamento Archive multi-tenant ainda está pendente.
6. Após a sessão, remova contas temporárias e ACLs/compartilhamentos, revise logs e registre problemas sem copiar conteúdo pessoal.

## Segredos, dados e auditoria

- Não inserir senhas, tokens, chaves, cookies, URLs assinadas ou conteúdo OCR em Markdown, Git, gravações ou tickets.
- Não reutilizar dados/credenciais da demo em produção. Cada cliente deve ter ambiente e segredos próprios.
- Confirmar autenticação real e autorização por objeto/tenant; estar atrás de login não prova isolamento dos dados.
- Registrar aprovação, escopo, concessão, validação e revogação sem armazenar segredos ou documentos.
- Manter portas de administração em loopback e seguir o túnel SSH documentado em `infra/AUTH-ARCHITECTURE.md`.

## Limitações conhecidas e próximos passos

- O provisionador interativo não inclui grupos de Airflow. Uniformizar e validar esse fluxo antes de oferecer autoatendimento.
- Archive ainda requer validação de ACL na instância ativa para uso multi-tenant; Invoice não tem cliente externo provisionado.
- Samia permite cadastro no demo/dev; confirmar isolamento de tenant e conhecimento antes de convidar público externo.
- CRM é demo/dev interno em Odoo; provisionamento local e SSO estão descritos em `crm/README.md`. Não liberar acesso externo nem tratá-lo como multi-tenant.
- Revisar este guia sempre que mudar Nginx, Keycloak, grupos, Compose, cadastro ou política de tenant.

## Referências

- `README.md` — situação e regras dos domínios demo/dev.
- `../infra/AUTH-ARCHITECTURE.md` — arquitetura OIDC, fronteira pública e administração.
- `../infra/keycloak/create_user.py` — criação interativa e lista atual de grupos suportados.
- `../infra/keycloak/bootstrap_realm.py` — bootstrap de clients e grupos.
- `../infra/nginx/locations-enabled/portal.conf`, `airflow.conf`, `analytics.conf`, `nodered.conf` — gates e proxy Nginx.
- `automation/README.md`, `archive/README.md`, `rag/README.md`, `data-platform/webserver_config.py` — detalhes operacionais por produto.
- `pulse-invoice/security/README.md` e `pulse-invoice/docs/PRODUCT-PLAN.md` — contrato de tenant e estado do MVP Invoice.
- `crm/README.md` — planejamento pendente do CRM.
