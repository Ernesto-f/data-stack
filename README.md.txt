

FIRST DOCKER UP
# 1) Base + Monitoring
docker compose -f infra/docker-compose.yml up -d --build \
  --profile base --profile monitoring

# 2) Analytics (Superset) + Orchestrator (Airflow)
docker compose -f infra/docker-compose.yml up -d --build \
  --profile analytics --profile orchestrator

# 3) Proxy (Nginx)
docker compose -f infra/docker-compose.yml up -d --build \
  --profile proxy


SUPERSET DOCKER UP
docker compose -f infra/docker-compose.yml run --rm superset superset fab create-admin
docker compose -f infra/docker-compose.yml run --rm superset superset db upgrade
docker compose -f infra/docker-compose.yml run --rm superset superset init







GIT HUB COMMANDS

# ignore segredos e dados locais
echo "env/.env" >> .gitignore
echo "env/secrets/" >> .gitignore
echo "volumes/" >> .gitignore


git init
git add .
git commit -m "chore: infra de dados (postgres, minio, airflow, superset, nginx, prometheus)"
git branch -M main



# se ainda não tem, gere e cadastre a chave em GitHub > Settings > SSH keys
ssh-keygen -t ed25519 -C "seu-email@exemplo.com"
cat ~/.ssh/id_ed25519.pub

git remote add origin git@github.com:SEU_USUARIO/data-stack.git
git push -u origin main



criando volumes na vps

# CRIE TUDO DE UMA VEZ
sudo mkdir -p /opt/data-stack/volumes/{postgres/data,minio/data,airflow/logs,superset/home,prometheus/data,nginx/{certs,auth}}
sudo mkdir -p /opt/data-stack/backups/{postgres,minio}
sudo chown -R $USER:$USER /opt/data-stack
# boas permissões
chmod 700 /opt/data-stack/volumes/nginx/certs
chmod 750 /opt/data-stack/backups /opt/data-stack/backups/* || true


O que é cada uma

/opt/data-stack/volumes/postgres/data → dados do Postgres (warehouse)

/opt/data-stack/volumes/minio/data → buckets do MinIO (bronze/silver/gold/backups)

/opt/data-stack/volumes/airflow/logs → logs do Airflow

/opt/data-stack/volumes/superset/home → estado do Superset (quando você subir)

/opt/data-stack/volumes/prometheus/data → TSDB do Prometheus (quando você subir)

/opt/data-stack/volumes/nginx/certs → certificados TLS (quando usar Nginx/HTTPS)

/opt/data-stack/volumes/nginx/auth → (opcional) arquivos .htpasswd p/ Basic Auth

/opt/data-stack/backups/* → dumps do Postgres e cópias dos objetos do MinIO

Essas paths já estão referenciadas no infra/docker-compose.yml que montamos.