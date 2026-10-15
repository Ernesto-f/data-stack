# Pipelines DocETL

- `nfse-local-extraction.yaml`: pipeline específico de NFS-e que reutiliza os operadores existentes do DocETL (`split`, `code_map` e `code_reduce`). Como só usa operações de código, não chama LLM, DeepSeek nem API de modelo.
- O PDF deve ser convertido para Markdown pelo Docling do SmartETL em modo local (`use_docetl_server=false`, sem `custom_docling_url`). O endpoint Docling do SmartETL deve ser executado apenas na infraestrutura aprovada para os dados; a instância compartilhada de demo/dev não é ambiente de produção para documentos de clientes.
- Os pesos locais necessários estão em `/docetl-data/huggingface` na VPS. O runtime SmartETL foi reconstruído com `torch==2.8.0` e `torchvision==0.23.0`; a amostra passou no Docling e no pipeline DocETL com rede desativada no container de teste.
- A saída fica em estado `pending_review`. Para esta amostra, `recipient_name` tem confiança reduzida e precisa de confirmação humana; `competence_date` e `due_date` permanecem nulos.
- A entrada do DocETL é um JSON local com `source_document_id`, `source_sha256` e `markdown`. Execute o YAML em um diretório de trabalho isolado que contenha `invoice_input.json`; o resultado `invoice_extraction.json` fica como sugestão pendente de revisão.
- `iss_rate_percent` é a alíquota do ISS em porcentagem; não é o valor monetário de `tax_iss`. A API persiste os dois campos separadamente e mostra evidência/página para revisão.
- A extração registra `layout_family`: `municipal_classic_v1` é o perfil estabelecido nesta amostra; `nfse_nacional_candidate_v1` usa regras candidatas para Emitente/Destinatário, mas permanece com sinal reduzido e revisão obrigatória. `nfse_nacional_unmapped`, `unclassified` e documentos com páginas de famílias mistas também ficam pendentes. Um perfil só se torna estabelecido após exemplos rotulados daquela família.
- A saída não define `tenant_key`: a aplicação deve obtê-lo da sessão autenticada do cliente externo, jamais da entrada do documento.
- Não usar `GD_Analysis.prod.yaml`, que é o pipeline de manuais/RAG, nem importar os resultados para o warehouse antes de revisão humana e validação do isolamento por tenant.
