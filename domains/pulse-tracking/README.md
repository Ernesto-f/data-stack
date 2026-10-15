# Pulse Tracking (Traccar)

Serviço Pulse para rastreamento de veículos e ativos, construído sobre o
[Traccar](https://github.com/traccar/traccar). O Traccar oferece mapa em tempo
real, posições, relatórios, geofences, alertas e API REST/WebSocket.

## Integração no Pulse

- O Nginx exige uma sessão Keycloak/OAuth2 Proxy com o grupo
  `/pulse/home/tracking`; o Traccar usa OIDC nativo com o mesmo realm e pula o
  formulário de senha local. Usuários do grupo `/pulse/home/tracking-admin`
  recebem papel administrador no Traccar.
- O branding suportado pelo produto pode ser definido em **Settings → Server**:
  nome `Pulse Tracking`, nome/descrição, logo, logo invertido e cores.
- A aplicação pode fornecer posições à API REST/WebSocket do Traccar. Para
  análises, o próximo passo é um adaptador controlado para carregar dados úteis
  no warehouse/Grafana; não conecte Grafana diretamente às tabelas internas do
  Traccar.
- Não conectar ainda Node-RED, PLCs, equipamentos reais nem integrações de
  terceiros.

## Ambiente inicial

- Imagem oficial `traccar/traccar:6.16.0` e TimescaleDB/PostgreSQL.
- Painel/API publicados somente em `127.0.0.1:18092`, atrás do Nginx.
- A porta OsmAnd `5055/TCP` está publicada apenas em loopback como suporte a
  testes fictícios locais; nenhuma porta de protocolo GPS está aberta na internet.
- Persistência em volumes Docker. Não use `docker compose down -v`.

## Preparar na VPS

1. Criar `.env` com `bash ./setup-secrets.sh`.
2. Iniciar com `docker compose up -d` e conferir `docker compose ps`.
3. O login no Traccar é feito por OIDC com o realm Pulse; o grupo
   `/pulse/home/tracking` autoriza usuários e `/pulse/home/tracking-admin`
   concede administração. Cadastros no formulário local ficam desativados.
4. Para teste OsmAnd, usar o identificador do dispositivo fictício e enviar
   lat/lon por `http://127.0.0.1:15055/`; não transmitir coordenadas reais.

Na implantação, a conta OIDC é associada pelo e-mail verificado. Contas locais
existentes permanecem na base, mas o acesso web exige OIDC quando
`OPENID_FORCE=true`.

Domínio: `tracking.izatacore.com` (A record apontando para o IP da VPS), com
TLS e gate OAuth2 Proxy configurados pelo Nginx.
