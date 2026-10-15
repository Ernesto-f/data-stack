# Planejamento de produto — Pulse Invoice

**Nome de trabalho:** Pulse Invoice.  
**Status:** MVP interno de demonstração implantado em `https://portal.izatacore.com/pulse-invoice/`. O fluxo aceita PDF digital, envia o original ao Archive, converte com Docling, extrai com pipeline DocETL determinístico sem LLM, cria rascunho para revisão, registra correções/aprovação/rejeição e exporta CSV. O isolamento por RLS e o fluxo da API passaram em PostgreSQL descartável (14 testes); um PDF sintético também passou pelos serviços Docling/DocETL ativos. Há uma NFS-e municipal aprovada e uma NFS-e nacional pendente, corrigida manualmente como primeiro exemplo desse perfil. O Llama local foi testado contra essas correções e segue experimental. `municipal_classic_v1` está estabelecido; `nfse_nacional_candidate_v1` continua em validação com novos exemplos. Não há cliente externo provisionado; ACL multi-tenant do Archive ainda precisa de validação antes do uso por cliente.

## Objetivo do produto

Transformar notas fiscais recebidas pelo cliente em registros financeiros organizados, conferíveis e pesquisáveis. O cliente envia o documento, confere os campos extraídos e, após aprovação, consulta suas notas, valores e pendências no portal ou pergunta ao Samia.

O diferencial do produto é fechar o ciclo entre documento original, extração assistida, validação humana, dados estruturados e consulta em linguagem natural. O produto não deve apresentar uma extração de IA como dado contábil aprovado sem revisão. A recomendação é lançar primeiro como **Pulse Invoice** para NFS-e e, se houver aderência, ampliar a plataforma para outros documentos financeiros e operacionais.

## Proposta de MVP

Começar com notas fiscais eletrônicas de serviço (NFS-e) em PDF digital com camada de texto. PDFs digitalizados falham fechados porque OCR não está habilitado neste MVP. XML, NF-e de produto, captura por e-mail, importação em lote e integração com ERP ficam para fases posteriores.

## Empacotamento por plano

Os pacotes devem habilitar recursos sobre o mesmo núcleo de produto; não devem exigir pipelines ou modelos de dados incompatíveis por cliente. Limites de volume, usuários, armazenamento e chamadas de modelo também podem ser definidos por pacote.

| Pacote de trabalho | Experiência do cliente | Recursos incluídos |
| --- | --- | --- |
| **Essencial** | Enviar uma NFS-e, conferir o resultado, corrigir/aprovar e exportar os registros. | Upload manual, arquivo original no Archive, OCR, extração de campos padrão, revisão humana, histórico e exportação CSV. Quotas simples de documentos/armazenamento. |
| **Profissional** | Processar lotes recorrentes e acompanhar vencimentos, pendências e gastos. | Tudo do Essencial, mais automação pelo Airflow, lembretes, dashboard do cliente no Portal e perguntas ao Samia sobre documentos e dados aprovados. |
| **Enterprise** | Adequar o fluxo ao processo e ao ambiente do cliente. | Tudo do Profissional, mais múltiplos tipos documentais, centros de custo/filiais, etapas de aprovação, integrações API/ERP, mapeamentos e relatórios próprios, SSO e opção de modelo local ou implantação isolada. |

São nomes e divisão iniciais para discutir, não uma definição comercial nem de preço. A decisão final sobre quais capacidades pertencem a cada pacote deve preceder a implementação dos controles de entitlement.

O **núcleo comum** continua sendo: original preservado, dados com proveniência, estado de revisão e isolamento por tenant. Os pacotes habilitam automação, analytics, conversa e integrações. O Grafana da plataforma é operacional/interno; a visualização de cliente fica no Portal e respeita o entitlement e o tenant.

### Entrada do cliente

Na experiência do cliente, evitar termos técnicos como “pipeline”, “DAG”, “DocETL” ou “embedding”. A primeira versão pode oferecer:

1. **Enviar nota fiscal** — upload de um ou mais PDFs.
2. **Contexto opcional** — empresa/unidade, centro de custo ou projeto.
3. **Acompanhar processamento** — recebida, extraindo dados, aguardando conferência, aprovada ou com erro.
4. **Conferir campos** — tela lado a lado com o PDF original e os valores sugeridos, com indicação de página/fonte e campos de baixa confiança.

O cliente inicial é externo. O identificador do cliente/tenant deve vir da sessão autenticada e autorizada no Portal; nunca deve ser aceito livremente de um campo de upload, argumento de API ou formulário de revisão.

**Primeiro percurso:** cliente abre “Enviar nota”, escolhe o PDF e, opcionalmente, informa unidade/centro de custo. O sistema mostra protocolo e estado, sem pedir que o cliente escolha “modelo”, “pipeline” ou configurações de OCR.

### Campos de saída sugeridos

**Identificação:** tipo de documento, número, série/verificação, código de serviço e período de competência.  
**Partes:** prestador e tomador, nome e CNPJ/CPF.  
**Datas:** emissão, competência, vencimento ou pagamento quando constarem no documento.  
**Valores:** subtotal, deduções, ISS, alíquota de ISS, outros tributos e total, com moeda.  
**Contexto:** descrição do serviço, centro de custo/projeto informado pelo cliente e classificação sugerida.  
**Rastreabilidade:** página e trecho-fonte de cada campo, confiança da extração, hash do arquivo, ID do documento no Archive, versão do pipeline/modelo e histórico de revisão/aprovação.

Não inferir valores ausentes (por exemplo, vencimento, status de pagamento ou categoria contábil). Deixar o campo vazio e solicitar confirmação quando não houver evidência clara.

### Saídas e valor para o cliente

- Lista pesquisável de notas e status de conferência.
- Detalhe com original, dados estruturados, fonte/página e histórico de correções.
- Exportação CSV/XLSX e, posteriormente, API para ERP/contabilidade.
- Respostas do Samia sobre o conteúdo de uma nota, com referência ao documento.
- Consultas numéricas (total por período, prestador, centro de custo) usando dados aprovados do banco estruturado, não apenas busca semântica sobre o texto do PDF.
- Painel do cliente no Portal com totais, evolução e pendências, quando houver isolamento por tenant validado.

**Tela de conferência:** apresentar o PDF e, ao lado, os campos extraídos com página de origem e aviso nos itens incertos. O cliente pode corrigir, aprovar, rejeitar como documento incorreto ou deixar pendente. A saída aprovada inclui número de registro, dados estruturados, original vinculado e opções de exportação/pergunta ao Samia.

## Como encaixa nos serviços Pulse

| Serviço | Responsabilidade |
| --- | --- |
| **Pulse Portal** | Entrada do cliente, acompanhamento, tela de conferência e resultados próprios do cliente. |
| **Pulse Archive** | Fonte de verdade do original, OCR, metadados revisados e histórico documental. |
| **Pulse SmartETL / DocETL** | Conversão local com Docling para Markdown e pipeline fiscal baseado nos operadores existentes do DocETL. |
| **Pulse Automation / Airflow** | Descobrir notas pendentes, chamar DocETL, validar a saída, registrar estados e publicar aprovados. |
| **PostgreSQL do warehouse** | Tabelas de estágio e fatos aprovados para métricas e integrações. |
| **Pulse Analytics / Grafana** | Operação interna do serviço e qualidade do processamento. O Grafana atual é admin/internal; não expor um painel global a clientes sem RBAC/filtro por tenant testado. |
| **Samia / WeKnora** | Conversa sobre documentos indexados do Archive. Para totais e agregações, usar uma API/ferramenta somente leitura sobre views aprovadas do warehouse. |

## Fluxo técnico proposto

1. O Portal recebe o PDF e identifica o tenant pela sessão.
2. O original entra no Pulse Archive e recebe um estado/tag inicial, como `Invoice / A extrair`.
3. Uma DAG pausada do Airflow consulta a API do Archive, baixa os documentos pendentes e registra ID, checksum e tentativa.
4. Em ambiente aprovado, SmartETL/Docling converte localmente o arquivo para Markdown; DocETL usa `split`, `code_map` e `code_reduce` para extrair e reunir campos com referência de página. O piloto sem LLM evita chamadas ao modelo DeepSeek.
5. DocETL devolve sugestões tipadas e rastreabilidade; Airflow valida formato, campos obrigatórios, consistência aritmética, duplicata e regras fiscais básicas.
6. Airflow escreve os campos sugeridos no Archive com estado **Aguardando conferência** e uma linha de extração no schema de estágio.
7. A pessoa confere e aprova/corrige pelo Portal/Archive. A aprovação humana é o limite entre sugestão e dado confiável.
8. Uma tarefa agendada lê somente documentos aprovados pela API e faz upsert idempotente no schema de reporting do warehouse.
9. O Samia consulta o documento original/indexado e, para perguntas numéricas, usa uma ferramenta/API de consulta read-only com filtros obrigatórios pelo tenant.
10. O Grafana lê apenas tabelas/views agregadas do warehouse para métricas internas. O Portal mostra os resultados segregados por cliente.

## Contrato de dados e idempotência

Separar pelo menos duas áreas:

- `invoice_stage`: uma tentativa de extração por `tenant_id`, `paperless_document_id`, `file_sha256` e `pipeline_version`; inclui resultado JSON, evidências por campo, confiança e estado de revisão.
- `invoice_reporting`: fatos normalizados aprovados, com chaves de cliente/documento e valores/datas necessários aos relatórios.

As tabelas-base permanecem comuns aos pacotes. A aplicação decide quais rotas, menus, cotas e workflows cada plano pode usar; não depender de um cliente receber ou não colunas diferentes no banco.

O DDL inicial foi criado em `domains/pulse-invoice/database/warehouse-schema.sql` e aplicado ao schema `pulse_invoice` do warehouse. Ele prepara `extraction_runs`, `invoices`, `invoice_lines`, `review_events` e as views `v_dashboard_monthly`, `v_dashboard_review_queue` e `v_dashboard_due_dates`. Há uma NFS-e municipal aprovada e uma NFS-e nacional pendente, com seus runs e eventos de revisão; a nota municipal contém `iss_rate_percent` separado de `tax_iss`. `invoice_lines` permanece vazia. As views são a futura fonte do Grafana; ainda não há role/datasource de Grafana para esse schema.

O DDL canônico acrescentou `payment_date` para preservar datas de quitação explicitamente citadas na nota. A coluna está presente no warehouse ativo.

Uma nova tentativa do mesmo arquivo não cria uma segunda fatura aprovada. Uma alteração do original (checksum diferente) cria uma nova tentativa/revisão. Para detectar duplicidade comercial, avaliar chave composta com tenant, CNPJ do prestador, número/série e data, sem descartar automaticamente documentos.

Guardar no warehouse apenas campos normalizados e a rastreabilidade necessária. O PDF e o texto integral permanecem no Archive; não copiar texto/chunks para painéis ou logs.

## Privacidade e isolamento

- A amostra local contém identificadores pessoais/financeiros. Não deve ser enviada a um modelo externo sem autorização explícita.
- Para o primeiro ensaio, reutilizar o Docling local do SmartETL e os operadores de código do DocETL (`code_map`/`code_reduce`); não chamar DeepSeek. A instância SmartETL atual é demo/dev, não um serviço segregado para documentos de cliente.
- A conversão local atual do SmartETL configura Docling com OCR desligado. Serve para PDFs digitais como a amostra; PDF escaneado precisará de OCR explicitamente local ou deverá falhar fechado.
- O DocETL instalado inclui operações `code_map` e `code_reduce`, que executam Python sem LLM; a referência de implementação existente está em `GD_Analysis.code-map-learning.yaml`. Não reutilizar o pipeline de manuais/RAG `GD_Analysis.prod.yaml` para dados fiscais.
- Os pesos públicos do Docling foram baixados para a cache local da VPS, com autorização. O PDF foi processado depois com rede desativada no container de teste; nenhum conteúdo foi enviado ao DeepSeek ou a outro serviço externo.
- O runtime SmartETL foi reconstruído com o par CPU compatível `torch==2.8.0`/`torchvision==0.23.0`; a importação de `AutoImageProcessor` e a operação NMS foram verificadas, e os serviços ficaram saudáveis após a implantação.
- O teste deixou `recipient_name` com confiança reduzida e revisão humana obrigatória. `due_date` e `competence_date` permaneceram ausentes; não foram inferidas.
- `pdftotext` local confirmou que a amostra tem camada textual, mas sua ordem de layout não extraiu com segurança o número da nota e o ISS; essa saída não foi considerada válida nem publicada.
- O SmartETL/DocETL compartilhado pode usar DeepSeek em outros pipelines. O pipeline deste MVP contém somente operadores `split`, `code_map` e `code_reduce`, e o teste sintético confirmou a execução sem chamada de LLM; manter essa restrição ao processar documentos reais.
- A configuração atual do Archive é uma instância única e ainda não foi validada como multi-tenant. Antes de clientes reais, provar que usuários de um tenant não conseguem listar, buscar, abrir ou compartilhar documentos de outro.
- O tenant e membership internos provisórios de demonstração já estão criados. Não importar documentos de cliente até provisionar sua associação OIDC→tenant e provar autorização por tenant em listagem, busca, download, edição, exportação, links compartilhados, tarefas assíncronas e consultas ao warehouse. A integração documental multi-tenant do Archive segue pendente.
- O contrato e os limites de autenticação estão em `domains/pulse-invoice/security/README.md`. `domains/pulse-invoice/api/` valida access tokens OIDC, resolve `sub` para `tenant_key` e aplica contexto RLS por transação. O Portal encaminha o token de sessão ao serviço no loopback; testes descartáveis passaram para autenticação, tenant, upload stubado, revisão, aprovação e exportação. O warehouse não contém registros de fatura.
- Separar credenciais de serviço por função, aplicar acesso mínimo, manter trilha de auditoria para revisões e não registrar conteúdo/PII em logs. Identificadores de tenant devem ser derivados da identidade autenticada no servidor.
- Airflow, DocETL e Archive devem falar por redes/credenciais de serviço restritas; a API de filesystem do DocETL não deve ficar exposta numa rede ampla.
- O Grafana atual é para operadores Pulse. Para o cliente, preferir Portal/API com autorização por tenant.

## Critérios de aceite do piloto

1. Usar a nota de teste sem alterações no original e obter campos com referências de página.
2. Distinguir corretamente campos extraídos, ausentes e incertos; conferir valores aritméticos e datas.
3. Exigir aprovação manual antes da tabela de reporting.
4. Reexecutar o mesmo arquivo sem duplicar fatos ou gerar custos inesperados.
5. Fazer o Samia responder perguntas sobre a nota com citação da origem; validar que totais vêm do dado estruturado aprovado.
6. Confirmar que o Grafana consulta somente views de reporting e que o tenant não aparece fora do seu escopo.

## Decisões de produto e segurança

1. O cliente inicial é externo; isolamento/autorização por cliente é um bloqueio de produção e deve ser provado antes da integração externa.
2. Manter o primeiro ensaio local-only; qualquer uso posterior do DeepSeek precisa de aprovação explícita.
3. Definir quais campos são obrigatórios, como tratar impostos/valores ausentes e quais categorias/centros de custo o cliente pode escolher.
4. Definir o pacote de lançamento e se o primeiro piloto terá somente a conferência controlada ou também uma tela cliente no Portal.

## Referências do DocETL

- [Best Practices](https://ucbepic.github.io/docetl/best-practices/): começar simples, modularizar e validar a saída.
- [Quick Start — Creating the Pipeline](https://ucbepic.github.io/docetl/tutorial/#creating-the-pipeline): datasets, operações e execução explícita.
- [Code Operations](https://ucbepic.github.io/docetl/operators/code/): `code_map`/`code_reduce` são operações Python determinísticas e não fazem chamadas de LLM.

O pipeline do produto está em `domains/pulse-invoice/docetl/pipelines/nfse-local-extraction.yaml`. O ensaio usou o runtime SmartETL/Docling existente e DocETL no container rebuildado. O Dockerfile do repositório SmartETL já tinha mudanças locais do usuário; foi acrescentado somente o pin Torch/Torchvision, preservando as demais alterações.

A migração RLS está em `domains/pulse-invoice/database/tenant-isolation.sql`; os testes descartáveis estão em `domains/pulse-invoice/tests/`. O warehouse ativo tem políticas e roles restritas, o tenant/membership interno, uma nota aprovada e outra pendente de revisão.
