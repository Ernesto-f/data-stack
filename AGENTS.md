# Regras do projeto

## Fluxo obrigatório de mudanças

- Toda alteração deve ser feita em uma branch dedicada; não trabalhar diretamente em `main` para preparar uma entrega.
- Antes de publicar código, executar as validações relevantes e abrir uma Pull Request.
- O deploy normal só pode acontecer depois do merge da Pull Request na branch principal.
- Na VPS, o deploy deve atualizar o checkout com `git pull`/`git fetch` do commit merged e então reconstruir/reiniciar somente os serviços necessários.
- Não usar `rsync`, cópia manual de arquivos ou edição manual na VPS como fluxo normal de publicação.
- Deploy direto sem PR/merge só é permitido para incidente ou emergência, com autorização explícita, backup, registro no `HANDOFF.md` e plano de reversão.

## Segurança e operação

- Nunca incluir segredos, dumps, arquivos de teste ou dados privados em commits.
- Fazer backup antes de alterar bancos, volumes, configurações ou imagens na VPS.
- Não executar `docker compose down -v`, resets destrutivos ou operações equivalentes sem autorização explícita.
- Após o deploy, validar healthchecks, logs, rotas afetadas e o fluxo funcional principal.
- Registrar no `HANDOFF.md` o commit implantado, validações, backup e qualquer exceção ao fluxo.

## Commits e revisão

- Manter commits pequenos e com uma entrega coerente.
- Preservar alterações preexistentes; não usar reset/checkout destrutivo para limpar o working tree.
- Antes de abrir a PR, confirmar que a branch está sincronizada com o remoto e que não há divergência não revisada.
