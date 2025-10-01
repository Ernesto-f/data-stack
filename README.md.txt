

FIRST DOCKER UP
# Base
docker compose -f infra/docker-compose.yml --profile base up -d
# Orquestrador
docker compose -f infra/docker-compose.yml --profile orchestrator up -d
# Analytics
docker compose -f infra/docker-compose.yml --profile analytics up -d
# Proxy
docker compose -f infra/docker-compose.yml --profile proxy up -d
# Monitoring
docker compose -f infra/docker-compose.yml --profile monitoring up -d



SUPERSET DOCKER UP
docker compose -f infra/docker-compose.yml run --rm superset superset fab create-admin
docker compose -f infra/docker-compose.yml run --rm superset superset db upgrade
docker compose -f infra/docker-compose.yml run --rm superset superset init