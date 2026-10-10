# Plano: Pulse Archive + DocETL + Airflow + Grafana

**Status:** planejamento; a integração não está implantada.
**Objetivo:** controlar documentos e evidências ISO no Pulse Archive, usar DocETL para sugerir metadados, manter revisão humana e publicar indicadores no Grafana por meio do warehouse PostgreSQL.

## Arquitetura alvo

```text
Pessoa
  │ envia documento / corrige e aprova metadados
  ▼
Pulse Archive (Paperless-ngx) ── API ──► Airflow
                                         │
                                         ├── obtém o documento e cria entrada de trabalho
                                         ├── solicita conversão para texto/Markdown ao Docling
                                         ├── chama o pipeline DocETL de extração
                                         ├── grava sugestões e rastreabilidade no Archive
                                         └── após aprovação humana, publica dados no warehouse
                                                                    │
                                                                    ▼
                                                         Grafana / Pulse Analytics
                                                         PostgreSQL read-only
```

O Archive permanece como repositório dos documentos e ponto da revisão humana. O DocETL produz sugestões estruturadas, não aprova evidências. O Airflow orquestra o processamento e publica dados analíticos. O Grafana consulta somente o banco de dados analítico; não consulta a API do Archive nem do DocETL.

## Estado verificado

- **Pulse Archive:** Paperless-ngx 2.20.14, vazio, atrás de HTTPS. A versão oferece campos personalizados, etiquetas, visualizações salvas, histórico de alterações e workflows agendados com base em campos de data e offsets. Workflows podem acionar e-mail ou webhook.
- **Limites do Archive:** SMTP não está configurado. Não assumir controle formal de aprovação ou encadeamento de revisões de arquivo na versão instalada; enquanto isso não for confirmado, cada revisão deve ser um documento separado ligado por código/revisão e campo `Documento Link`. A instância é compartilhada e ainda não foi validada para isolamento entre clientes.
- **DocETL / Pulse SmartETL:** a instalação usa DeepSeek Flash. O endpoint `POST /run_pipeline` executa de forma síncrona e recebe o caminho de um YAML dentro do container; retorna status/custo, enquanto a saída do pipeline é gravada em arquivo. Há endpoints `/fs/*` para gravar configuração, arquivos e ler saídas, além de `/api/convert-documents` para conversão.
- **Segurança da API DocETL:** a aplicação não autentica individualmente as rotas de execução/arquivos; o acesso externo é protegido por Keycloak/OAuth2 Proxy e exige `/pulse/home/smartetl`. Os endpoints de filesystem aceitam caminhos no container. Não colocar DocETL em uma rede Docker ampla nem conceder a Airflow acesso a eles sem restringir o acesso e os caminhos.
- **Airflow:** Airflow 3.1.4, `LocalExecutor`, providers HTTP e PostgreSQL instalados. As 20 DAGs existentes estão pausadas. Airflow está na rede `data-stack_internal`; DocETL e Archive estão em redes separadas.
- **Warehouse:** PostgreSQL 16 em `data-platform-warehouse`, disponível na rede `data-stack_internal` e publicado no host somente em `127.0.0.1:15432`.
- **Grafana:** está na mesma rede Docker do warehouse. A configuração provisionada atualmente tem somente Prometheus; ainda não há datasource PostgreSQL.
- **Conteúdo:** nenhum documento foi lido ou processado durante esta investigação. Os arquivos `GD_Analysis.prod.yaml` existentes são voltados à extração/chunking de manuais para RAG e não são o pipeline ISO.

## Fluxo planejado

### 1. Receber e classificar no Archive

O usuário envia o documento ao Archive e marca-o para extração, por exemplo com a etiqueta `ISO / A extrair`. Manter o original e os metadados do Paperless como fonte de verdade.

### 2. Airflow identifica e prepara

Uma DAG manual ou agendada consulta a API do Paperless com uma conta de serviço de privilégio mínimo, seleciona documentos pendentes, registra ID, checksum/versão e baixa o arquivo pela API. A chave de idempotência deve incluir pelo menos o ID do documento, checksum do arquivo e versão do pipeline.

### 3. Docling converte; DocETL extrai

Airflow solicita Markdown/texto ao endpoint de conversão DocETL/Docling, prepara um JSON de entrada no espaço de trabalho persistente do DocETL e chama `/run_pipeline` com o caminho do YAML ISO. O pipeline deverá devolver campos tipados, origem/página da evidência e uma indicação de confiança ou necessidade de conferência.

Como `/run_pipeline` é síncrono e não tem `job_id`/polling, a tarefa HTTP precisa de timeout amplo e sem retry automático enquanto não houver idempotência confirmada. Para processos longos, considerar adicionar à API DocETL uma fila/job assíncrona com `job_id`, status e resultado, em vez de manter uma requisição aberta.

### 4. Revisão humana no Archive

Airflow escreve os valores sugeridos como metadados do documento e define `Status de revisão = A revisar`. Uma pessoa confere o documento e corrige/aprova os campos no Archive. Não publicar como conformidade aprovada antes dessa ação.

Campos iniciais a validar com o usuário:

- código e título controlado;
- norma e cláusula/requisito;
- tipo documental;
- revisão e estado (rascunho, a revisar, aprovado, obsoleto);
- responsável;
- datas de emissão/aprovação e próxima revisão/validade;
- documentos/evidências relacionados;
- fonte/página extraída e estado/confiança da sugestão.

Usar tipos Paperless adequados (`Date`, `Select`, `Text`, `Boolean`, `Document Link`) e etiquetas hierárquicas/visualizações para organizar por norma e estado.

### 5. Publicar apenas registros aprovados no warehouse

Uma segunda tarefa/DAG lê os documentos com estado `Aprovado` pela API Paperless e faz upsert no warehouse. Proposta de separação:

- `iso_staging`: resultado de extração e estado de revisão, sem conteúdo integral do documento;
- `iso_reporting`: dados curados usados em indicadores, atualizados apenas a partir de documentos aprovados.

Manter `paperless_document_id`, checksum, código/revisão, norma/cláusula, datas, estado, responsável, timestamp de revisão, versão do pipeline e rastreabilidade da página/origem. Os documentos e textos completos permanecem no Archive.

### 6. Grafana consulta somente o mart

Provisionar um datasource PostgreSQL para `data-platform-warehouse:5432` na rede existente. Criar uma role `grafana_iso_reader` com apenas `SELECT` nas views/tabelas de `iso_reporting`; a DAG usa uma role distinta com escrita limitada ao schema necessário. Não usar o usuário administrador do warehouse no datasource do Grafana.

Indicadores possíveis: documentos aprovados por norma/cláusula, pendentes de revisão, vencidos, com revisão em 30/60/90 dias, evidências sem responsável e cobertura documental. O Grafana consulta as views do mart e não faz chamadas HTTP a Archive/DocETL.

## Conectividade e credenciais

Para a execução, criar redes Docker dedicadas ligando apenas os serviços necessários (Airflow ↔ DocETL e Airflow ↔ Archive), ou desenhar um gateway autenticado equivalente. Não expor DocETL, Paperless, PostgreSQL nem API do warehouse na internet. Criar Connections/secretos Airflow próprios para a conta Paperless e para a API DocETL.

Antes de liberar a integração, rever a autenticação da API DocETL, limitar namespaces e caminhos graváveis/lidos, configurar timeout/concurrency e garantir logs sem tokens, conteúdo integral, chaves ou dados sensíveis.

## Teste de aceite — somente dados sintéticos

1. Usar um PDF fictício com cláusulas e datas inventadas.
2. Confirmar upload, seleção por etiqueta e download via API do Archive.
3. Confirmar conversão, execução DocETL e saída validada, com ID/checksum e referências de página.
4. Confirmar que a extração aparece como `A revisar` no Archive e ainda não entra nos indicadores aprovados.
5. Alterar manualmente para `Aprovado`; a DAG de publicação deve fazer upsert idempotente no warehouse.
6. Confirmar que o dashboard consulta apenas as views do warehouse e apresenta os totais esperados.
7. Confirmar que reexecução da mesma versão/checksum não duplica registro nem dispara custos inesperados.

Manter as DAGs pausadas até o teste ser aprovado. Não executar sobre documentos reais até confirmar a política de envio de conteúdo ao DeepSeek, as normas/ campos desejados e as regras de acesso para usuários/clientes.

## Próximas decisões

1. Confirmar normas e cláusulas no primeiro escopo, além dos campos e estados de revisão.
2. Decidir se documentos reais podem ser processados pelo DeepSeek externo; se não, escolher modelo local/privado.
3. Confirmar se o Archive será de uso interno da Izatacore ou se haverá acesso de clientes; isolamento entre clientes precisa de projeto e testes próprios.
4. Implementar redes/credenciais mínimas, pipeline ISO sintético, DAG pausada, schemas/roles no warehouse e datasource PostgreSQL do Grafana.
5. Executar o teste de aceite com PDF fictício; só então habilitar ingestão aprovada.
