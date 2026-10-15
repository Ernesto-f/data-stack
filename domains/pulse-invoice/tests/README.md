# Testes e amostras

Validar o pipeline `docetl/pipelines/nfse-local-extraction.yaml` com texto/Markdown sintético sem PII. Para documentos reais, executar somente em ambiente aprovado, com Docling local e operações `code_map`/`code_reduce`; não incluir PDF, Markdown convertido ou saída com PII nesta pasta ou no Git.

A saída do pipeline permanece pendente de revisão humana e não deve ser publicada no warehouse durante esta prova.

## API e isolamento por tenant

O teste executa a migração, cria dois tenants sintéticos em PostgreSQL descartável e usa um login de aplicação sem `SUPERUSER`/`BYPASSRLS`. Ele valida o contexto por transação, o mapeamento de `sub`, a filtragem da RLS, o fechamento por padrão sem associação e a impossibilidade de escolher outro tenant por query string.

```bash
docker compose -f domains/pulse-invoice/tests/api-isolation.compose.yml \
  up --build --abort-on-container-exit --exit-code-from tests
docker compose -f domains/pulse-invoice/tests/api-isolation.compose.yml down -v --remove-orphans
```

O stack usa somente `tenant-a`/`tenant-b` sintéticos e não conecta ao warehouse ativo. O teste cobre autenticação, isolamento RLS, upload com integrações stubadas, deduplicação, correção, auditoria apenas de campos alterados, salvamento sem alterações, aprovação, histórico e CSV. A extração Docling/DocETL é exercitada separadamente com PDF sintético contra os serviços locais da VPS. A implantação de demonstração está atrás do Portal/OAuth2 Proxy; tenants externos continuam bloqueados até validação de ACL documental e isolamento multi-tenant do Archive.
