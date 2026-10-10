# Pré-processamento — DocETL

O **Pulse SmartETL** é a etapa de preparação de documentos antes da ingestão no Samia (WeKnora). A implementação de origem está em `/media/sam/Arquivos/arquivos/Projetos/chat-industial-v1/docetl`; na VPS fica em `/srv/data-stack/domains/preprocessing/docetl`. O deploy deve excluir `.env`, `.git`, uploads locais e artefatos gerados.

O processo reutilizável para revisão humana, rótulos, avaliação e promoção de ETL está em `domains/preprocessing/HUMAN-IN-THE-LOOP-ETL-FRAMEWORK.md`.

## Fluxo

1. Selecionar documentos de demo/dev vindos do Paperless.
2. Converter PDFs e outros formatos em texto/Markdown usando o Docling integrado ao projeto DocETL.
3. Executar o pipeline DocETL para normalizar, dividir por página, enriquecer e validar os trechos.
4. Revisar a saída JSON, preservando texto original, fonte e referência da página.
5. Importar manualmente os arquivos aprovados no Samia; o WeKnora gera os embeddings usando BGE-M3.

O pipeline local `GD_Analysis.prod.yaml` produz registros com `retrievable_text`, metadados, referências e auditoria; seu modelo foi alinhado para `deepseek/deepseek-flash`. A transferência da saída DocETL para o WeKnora ainda não é automática; não se deve conectar os diretórios de saída diretamente ao índice antes de validar o formato de ingestão e revisar os resultados.

## Modelo para as operações DocETL

O modelo padrão para extração e enriquecimento continua `deepseek/deepseek-flash`, via API DeepSeek compatível com LiteLLM; a interface também mantém esse default. A chave fica no `.env` privado da VPS. O endpoint Ollama local está disponível apenas pela rede privada Docker em `OLLAMA_API_BASE=http://host.docker.internal:11434/`; um pipeline pode selecionar explicitamente `ollama/llama3.1:8b` sem alterar o default DeepSeek. O embedding BGE-M3 do Ollama continua separado, para indexação no Samia. O experimento NFS-e está em `domains/pulse-invoice/docetl/pipelines/nfse-local-llama-experiment.yaml` e não está ligado ao fluxo de produção.

O serviço está em `https://smartetl.izatacore.com`. A raiz redireciona direto para o workspace; as páginas de showcase foram removidas. Frontend, API e Docling estão saudáveis; frontend e API escutam somente no loopback da VPS nas portas `13003` e `13008`. O Nginx publica HTTPS e exige uma sessão Keycloak/OAuth2 Proxy com o grupo `/pulse/home/smartetl`. Não exponha diretamente as portas internas. O Dockerfile não copia `.env` para a imagem e `.dockerignore` exclui arquivos de ambiente, uploads e dados locais do contexto de build.

Para o Docling local, o Dockerfile fixa `torch==2.8.0` e `torchvision==0.23.0` do índice CPU para manter o operador NMS disponível no Transformers. Os pesos Docling ficam no volume `data/docetl-data/huggingface`; a conversão foi validada com `HF_HUB_OFFLINE=1`.

Para conferir ou operar os containers na VPS:

```bash
docker compose -f /srv/data-stack/domains/preprocessing/docetl/docker-compose.yml ps
docker compose -f /srv/data-stack/domains/preprocessing/docetl/docker-compose.yml logs -f docetl docling-server
```

## Separação de dados

- Esta instalação é somente para demo/dev; não use documentos ou credenciais de clientes de produção.
- Armazene entradas, intermediários e saídas do DocETL em volume próprio, sem misturar com o volume do WeKnora.
- Preserve os originais no Paperless e mantenha referência de documento/página nos resultados.
- Não versionar `.env`, chaves de provedor, uploads, intermediários nem saídas processadas.
