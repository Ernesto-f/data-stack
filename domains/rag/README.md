# RAG — Samia (WeKnora)

O domínio RAG usa a distribuição customizada do WeKnora. A VPS roda o código-fonte dessa distribuição; credenciais e dados permanecem no servidor.

- Fonte local: `chatboot/v2-weknora-custom/WeKnora/offline-bundle-20260929T011606Z`
- Destino planejado: `/srv/data-stack/domains/rag/weknora`
- Chat: DeepSeek Flash, configurado no WeKnora.
- Embeddings: Ollama local com `bge-m3:latest`.

O bundle offline não foi utilizado: ele contém runtime e modelos de chat Ollama que não fazem parte deste ambiente. O deploy partiu do código-fonte e construiu as imagens na VPS. Credenciais permanecem apenas no `.env` do servidor, fora do Git.

## Instância demo/dev na VPS

O código-fonte de `v2-weknora-custom` foi copiado para `/srv/data-stack/domains/rag/weknora`, sem `.git`, arquivos `.env` locais, dados Paperless, diretório de documentos nem bundle offline. O `.env` da VPS foi criado a partir do exemplo com segredos novos; está com permissão restrita e não foi copiado para este repositório. A interface mantém o nome **Samia** e usa o logo Pulse padrão compartilhado com os demais produtos.

Os serviços core e os opcionais descritos abaixo estão iniciados. Frontend e API estão vinculados somente ao loopback da VPS nas portas `18080` e `18081`; não estão publicados diretamente na internet. Para abrir a interface por túnel SSH:

```bash
ssh -L 18080:127.0.0.1:18080 sam@<IP-DA-VPS>
```

O endereço público configurado é `https://knowledge.izatacore.com`. Frontend e API continuam ligados somente ao loopback; Nginx publica o site com HTTPS e redireciona HTTP para HTTPS. O cadastro está habilitado para o ambiente demo/dev. DeepSeek Flash é o modelo de chat ativo; sua chave fica criptografada na configuração persistente do WeKnora. Se o seu DNS ainda estiver em cache, aguarde a atualização do resolvedor.

O login OIDC nativo do WeKnora está configurado com o realm Pulse e o client `pulse-samia`. Na tela de login, escolha **Pulse**; com uma sessão ativa no Portal, o Keycloak deve reutilizar a autenticação. A conta do WeKnora continua vinculada ao e-mail OIDC e ao tenant do aplicativo.

## Página de cadastro

O pacote temporário `registration-update` foi aplicado e removido da VPS. O código-fonte persistente já contém a rota `/register` (`frontend/src/router/index.ts`) e a tela condicional (`frontend/src/views/auth/Login.vue`). A atualização definiu o toggle no `.env` e no Compose e carregou a imagem de frontend atualizada, retagueada como `wechatopenai/weknora-ui:izatacore-custom`; `WEKNORA_VERSION=izatacore-custom` permanece definido para recriações. A página está disponível em `https://knowledge.izatacore.com/register`; o backend permite cadastro em modo self-serve (`DISABLE_REGISTRATION=false`).

Para operar os containers, conecte novamente por SSH após a inclusão de `sam` no grupo `docker`, ou use `sudo docker compose`:

```bash
cd /srv/data-stack/domains/rag/weknora/WeKnora
sudo docker compose --env-file .env ps
sudo docker compose --env-file .env logs -f
```

## Perfis opcionais

Um profile é uma etiqueta no Docker Compose para iniciar serviços opcionais; os serviços core continuam incluídos. Ative mais de um profile repetindo a opção:

```bash
sudo docker compose --profile neo4j --env-file .env up -d
```

- `neo4j`: banco de grafo usado pelo GraphRAG. Está ativo; `NEO4J_ENABLE=true` conecta o app a ele.
- `langfuse`: observabilidade de chamadas de LLM; acrescenta interface/worker Langfuse, ClickHouse e um MinIO exclusivo para artefatos de observabilidade. Reutiliza PostgreSQL e Redis. Foi testado e desligado; não faz parte do conjunto ativo.

O perfil `minio` do Compose é outro serviço, destinado ao armazenamento de documentos. Não está ativo porque os documentos virão do Paperless.

O Browser do Neo4j respondeu HTTP 200. A interface está apenas no loopback da VPS:

| Interface | Porta local da VPS | Acesso via SSH tunnel |
| --- | ---: | --- |
| Neo4j Browser | 17474 | `http://localhost:17474` |

Crie o túnel para abrir as interfaces no computador:

```bash
ssh -L 17474:127.0.0.1:17474 sam@<IP-DA-VPS>
```

Para o Neo4j Browser, o usuário é `neo4j`; a senha fica no `.env` da VPS (`NEO4J_PASSWORD`). A senha é exclusiva desse ambiente. Se ativar Langfuse depois, será preciso criar um projeto/API key e preencher `LANGFUSE_PUBLIC_KEY` e `LANGFUSE_SECRET_KEY` no `.env`.

O Langfuse foi desligado após o teste e seus containers, volumes e banco auxiliar foram removidos. Se quiser experimentá-lo depois, acrescente `--profile langfuse` ao comando `up`. Para pausar apenas Neo4j, sem parar WeKnora/PostgreSQL/Redis:

```bash
sudo docker compose --profile neo4j --env-file .env stop neo4j
```

## Ollama para embeddings

Ollama roda como serviço do Ubuntu, preso ao gateway da bridge Docker (`172.17.0.1:11434`), sem porta pública. O modelo `bge-m3:latest` está instalado e registrado como embedding default local, com dimensão 1024; DeepSeek Flash é usado para chat e enriquecimento do DocETL.

- `domains/rag/ollama/ollama.service.d/override.conf`: bind local do serviço Ollama.
- `WeKnora/config/builtin_models.yaml`: declara somente o embedding BGE-M3 local.
- `WeKnora/.env` na VPS: `OLLAMA_BASE_URL=http://host.docker.internal:11434`.

O endpoint foi validado do host e do container WeKnora; BGE-M3 retornou vetores de 1024 dimensões. A VPS não detectou GPU, então o embedding roda em CPU. Para operações futuras:

```bash
sudo systemctl status ollama
OLLAMA_HOST=http://172.17.0.1:11434 ollama list
OLLAMA_HOST=http://172.17.0.1:11434 ollama pull bge-m3
```
