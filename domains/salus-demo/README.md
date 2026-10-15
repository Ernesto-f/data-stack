# SALUS demo VPS

Stack demo isolada do restante do servidor. Usa os clones limpos em `salus-erp/`,
`SALUS/` e `salus-ambiental/`. Não usa Traefik; o Nginx do host encaminha para
portas publicadas apenas em `127.0.0.1`.

## Domínios

- `https://demoerp.izatacore.com`: SALUS ERP.
- `https://salusambientaldemo.izatacore.com`: portal do cliente Ambiental.
- O backoffice Ambiental fica em `https://salusambientaldemo.izatacore.com/salus`.
- O webhook de WhatsApp do ERP responde 404 nesta demo; integrações externas não
  são ligadas.

## Implantação

Na VPS, neste diretório:

1. `bash ./setup-secrets.sh` cria credenciais aleatórias privadas e configs do Odoo.
2. `bash ./deploy.sh` constrói as imagens, instala os módulos sem demo data Odoo,
   cria um pequeno seed fictício no ERP e inicia os serviços.
3. Para a primeira emissão TLS, instale os arquivos em `nginx/acme/` em
   `/etc/nginx/sites-available/`, ative-os em `sites-enabled`, valide `nginx -t`
   e recarregue o Nginx.
4. Emita os certificados Let's Encrypt para cada host com o plugin Nginx.
5. Instale os arquivos definitivos de `nginx/` (fora de `nginx/acme/`) em
   `/etc/nginx/sites-available/`, valide `nginx -t` e recarregue o Nginx.

Postgres e volumes são independentes por produto; nenhuma porta de banco é
publicada no host. Não rode `docker compose down -v` nesta stack: isso apaga os
bancos demo.

O primeiro usuário Odoo é `admin`; troque a senha pela interface após entrar.
O usuário/senha inicial do portal Ambiental deve ser criado no backoffice, pois
o banco começa sem contas de cliente.
