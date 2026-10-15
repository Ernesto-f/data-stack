# Teste de ACL por tenant no Paperless

Este teste usa somente o stack descartável definido em `tenant-acl.compose.yml`, com PostgreSQL/Redis temporários, dois usuários/grupos sintéticos e PDFs gerados durante o teste. Ele não conecta ao Archive ativo.

```bash
docker compose -f domains/archive/tests/tenant-acl.compose.yml -p pulse-archive-tenant-acl-test up -d
python -m pip install -r domains/archive/tests/requirements.txt
python domains/archive/tests/tenant_acl_test.py
docker compose -f domains/archive/tests/tenant-acl.compose.yml -p pulse-archive-tenant-acl-test down -v --remove-orphans
```

Verifica que cada identidade lista, pesquisa, abre, baixa e edita somente seus documentos e que a permissão global de upload não está concedida. A preparação das identidades é feita por um comando Django apenas dentro do container descartável.

O teste `tenant_oidc_test.py` usa duas identidades sintéticas no realm Pulse: autentica primeiro pelo Portal, confirma reutilização da sessão no Archive, Analytics e Automation e valida os grupos do OAuth2 Proxy no Portal, SmartETL, DataLogger e Automation. Também verifica o group sync no Archive e o papel FAB do Airflow. Contas, grupos e usuários temporários do Grafana/Airflow são removidos; nenhum documento é criado. Requer SSH `sam@179.198.221.68` e acesso ao `.env.auth` privado da VPS; execute apenas no realm de desenvolvimento.
