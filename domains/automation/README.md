# Automação — demo/dev

## Escopo inicial

- **Node-RED**: entra neste domínio; os flows, credenciais criptografadas e nodes instalados existentes vêm de `infra/volumes/nodered_data/`.
- **n8n**: fora do escopo por enquanto. Os arquivos existentes em `infra/n8n/` não serão implantados.

O serviço tem Compose próprio em `docker-compose.yml`, os flows/configuração persistentes migrados de `infra/volumes/nodered_data/` para `/srv/data-stack/domains/automation/data/` e o subdomínio `datalogger.izatacore.com`. `nodered.izatacore.com` redireciona para o novo endereço. A interface é identificada como **Pulse DataLogger** e usa o logo e favicon Pulse padrão. Foram mantidos `flows.json`, `flows_cred.json`, `settings.js`, configurações de usuários e os nodes instalados; cache npm, logs e backups antigos ficaram de fora. As dependências foram reinstaladas a partir do `package-lock.json`.

A interface usa Keycloak/OAuth2 Proxy no Nginx; o grupo `/pulse/home/datalogger` é obrigatório. O container está em safe mode: os flows aparecem para revisão, mas os injects agendados e as escritas em PLC/DB não são executados. Depois de conferir endpoints e credenciais de demo/dev, remova `--safe` de `command` e recrie o container para iniciar os flows.

Para subir e operar:

```bash
docker compose -p automation -f domains/automation/docker-compose.yml up -d
docker compose -p automation -f domains/automation/docker-compose.yml ps
```

A porta 1882 do container está presa ao loopback e só o Nginx publica o subdomínio com HTTPS. Cada pessoa entra com sua própria identidade Pulse.
