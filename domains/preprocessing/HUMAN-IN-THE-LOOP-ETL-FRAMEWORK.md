# Framework Human-in-the-Loop para ETL

Este framework descreve como transformar revisão humana em melhoria mensurável de extrações ETL. Aplica-se a faturas, formulários, contratos, cadastros e outros fluxos documentais; cada projeto adapta o esquema e as regras, sem alterar os princípios de auditoria, evidência e avaliação.

## Objetivo e definição de qualidade

O extrator produz **sugestões**. Uma pessoa autorizada verifica a sugestão contra a fonte, corrige o que estiver errado e decide se o registro pode avançar. A correção salva é um rótulo humano para avaliar uma versão do ETL.

“100%” deve sempre nomear seu escopo. É possível exigir 100% de correspondência em um conjunto de aceitação rotulado, ou 100% de completude nos registros aprovados após conferência. Não se deve prometer 100% de acerto automático para documentos futuros, layouts desconhecidos ou fontes degradadas.

## Ciclo operacional

### 1. Registrar a origem e a versão

Para cada documento/registro, guardar identificador da fonte, tenant/projeto, checksum, data de ingestão e versão do parser/pipeline/modelo. O original permanece na fonte documental; o ETL guarda os campos necessários e as referências de proveniência.

### 2. Gerar candidatos por campo

Cada campo sugerido deve, quando aplicável, carregar:

- valor bruto e valor normalizado;
- referência à página/seção e trecho que sustenta o valor;
- confiança ou classificação de evidência;
- nome e versão do extrator que produziu a sugestão.

Campo ausente ou ambíguo permanece nulo. Não substituir ausência por zero, data presumida ou categoria padrão, salvo se a regra de negócio declarar explicitamente esse default.

### 3. Aplicar validações antes da revisão

Validar tipos, formatos e relações determinísticas antes de apresentar o resultado: datas válidas, CNPJ/CPF e dígitos verificadores quando aplicável, moeda, somas e retenções, coerência entre campos e evidência textual. Falha ou divergência reduz a confiança e envia o campo para revisão; não deve ser “corrigida” silenciosamente por outro modelo.

## Regra reproduzível para confiança por campo

`confidence` representa a probabilidade de **correspondência exata com o valor que um revisor aceitaria**, após a normalização definida para aquele campo. Não é a certeza declarada pelo LLM, nem uma nota atribuída manualmente porque uma regex encontrou algo.

### Enquanto não houver dados suficientes

Não exibir porcentagem. Exibir um estado de evidência, calculado por regras versionadas:

- `direta`: valor ligado a um trecho/página identificável e passou nas validações;
- `inferida`: obtido por posição, layout ou interpretação do modelo, sem correspondência direta inequívoca;
- `conflitante`: candidatos discordam ou alguma validação falhou;
- `ausente`: não há valor com evidência suficiente.

O documento continua em revisão humana quando campos obrigatórios estão `inferidos`, `conflitantes` ou `ausentes`. Constantes como `0.99`, `0.94` e `0.75` podem ser pesos internos temporários, mas não devem ser rotuladas como percentuais de confiança.

### Quando publicar uma probabilidade

1. Definir `y=1` se o candidato normalizado for exatamente igual à correção humana; caso contrário `y=0`.
2. Calcular por `campo + versão do extrator + família de layout`, nunca misturando campos ou versões.
3. Usar somente revisões humanas concluídas. Dividir treino/calibração/teste por documento e layout para evitar que páginas do mesmo formulário vazem entre conjuntos.
4. Calibrar os sinais do extrator com Platt scaling ou isotonic regression na amostra de calibração. Não usar a autoclassificação de confiança do LLM como verdade.
5. Publicar `P(y=1)` apenas quando houver pelo menos 100 exemplos revisados naquela faixa campo/layout/versão e o holdout mostrar calibração aceitável. Guardar versão do calibrador, janela de dados, tamanho da amostra, Brier score e Expected Calibration Error.
6. Se a amostra/calibração não passar, voltar ao estado textual de evidência. Nunca transformar `0.94` heurístico em `94%` só mudando o rótulo.

Essa regra torna a métrica reprodutível: alvo, normalização, agrupamento, amostra, calibrador e critérios ficam versionados. Os números absolutos e limites podem ser ajustados por projeto, mas devem ser definidos antes de avaliar a nova versão.

### 4. Revisão e decisão humana

Mostrar a fonte ao lado dos campos, com evidência por campo. O revisor pode corrigir, manter, deixar pendente ou rejeitar. A aprovação só ocorre depois das validações obrigatórias e da decisão explícita do revisor. Campos obrigatórios sem evidência bloqueiam aprovação.

### 5. Capturar feedback fiel

Ao salvar uma revisão, comparar o valor persistido com o valor enviado e registrar **somente os campos que realmente mudaram**. Guardar, sob o mesmo controle de acesso do dado, os valores `before`/`after`, o ator, horário, decisão, documento e versão do ETL. Um envio sem alterações não cria um evento de correção. A rejeição deve registrar motivo/categoria sempre que disponível.

O evento de revisão é a trilha de auditoria e a fonte potencial de rótulos; não é, por si só, um conjunto de treinamento pronto. Separar correções de mudanças de negócio, migrações, defaults e reprocessamentos.

Na demonstração Invoice, o evento de correção mais antigo foi escrito antes da correção do rastreamento de `changed_fields`; essa lista histórica contém todos os campos enviados pelo formulário. Para esse evento, a alteração real deve ser recuperada comparando `event_payload.before` e `event_payload.after`. Eventos novos registram a lista precisa.

### 6. Converter correções em avaliação

Curar uma amostra rotulada com os valores aceitos pelos revisores e conservar a ligação com a versão da origem. Executar cada nova versão do ETL contra essa amostra e comparar por campo, não apenas por documento.

Manter exemplos de avaliação fora do código quando contiverem dados pessoais ou confidenciais. Para testes versionados, gerar fixtures sintéticas ou anonimizadas que preservem o layout e o tipo de erro, sem copiar nomes, identificadores, texto integral ou PDFs reais.

### 7. Corrigir a causa e testar regressões

Classificar discrepâncias (segmentação de prestador/tomador, OCR/layout, formato, normalização, campo ausente, alucinação, regra de negócio). Corrigir primeiro regras determinísticas e mapeamento de evidência; usar LLM local/remoto apenas onde houver benefício comprovado. Reexecutar o conjunto de aceitação e um conjunto separado de validação; não ajustar e avaliar na mesma amostra sem registrar essa limitação.

Não fazer fine-tuning automático após uma correção isolada. Um ou poucos rótulos servem para diagnosticar e criar testes direcionados; não justificam treinar/publicar um modelo.

## Métricas mínimas

Reportar por campo, tipo de documento, origem/layout e versão:

- **exact match** após normalização definida;
- **taxa de correção humana** e campos mais corrigidos;
- **taxa de evidência válida** (o trecho sustenta o valor);
- **cobertura/abstenção** (preenchido versus nulo/pendente);
- **taxa de aprovação e rejeição** e tempo de revisão.

Separar acerto, cobertura e evidência. Um modelo que preenche mais campos pode também inventar mais; cobertura sozinha não prova melhoria. Publicar métricas agregadas, nunca valores ou trechos de documentos em logs/dashboards sem autorização.

## Critério de promoção

Uma alteração de parser, prompt ou modelo só avança quando:

1. passa nos testes de formato e validação de negócio;
2. melhora ou mantém o exact match por campo no conjunto de aceitação;
3. não reduz a evidência/proveniência nem aumenta alucinações;
4. mantém a decisão humana para casos ambíguos e para todos os campos críticos;
5. tem versão registrada e plano de rollback.

“100%” pode ser critério para um conjunto fechado de aceite. Para dados reais novos, o estado padrão continua sendo revisão humana até existir política de aprovação automática aprovada, validada e monitorada.

## Segurança e privacidade

- Derivar tenant/projeto da identidade autenticada; aplicar RLS/ACL ao dado, eventos, amostras e relatórios.
- Não enviar documento, OCR ou correções a provedor externo sem autorização específica. Para testes com dados reais, usar infraestrutura local aprovada e não exibir valores no terminal, logs ou artefatos compartilhados.
- Não versionar documentos, texto integral, tokens nem rótulos reais. Restringir acesso ao conjunto de feedback pelo mesmo tenant e finalidade do dado original.
- Manter um caminho de exclusão/retificação que alcance o dado operacional e suas cópias de avaliação, sujeito às regras de retenção aplicáveis.

## Situação do Pulse Invoice

O MVP já implementa upload, rascunho `pending_review`, evidência por página, correção, aprovação/rejeição, exportação controlada por tenant e eventos com `before`/`after`. A API agora registra apenas campos efetivamente alterados; salvamentos sem mudanças não criam eventos. O registro de revisão atual pôde ser usado para uma comparação local do Llama 3.1 8B: ele coincidiu com 2 dos 5 campos corrigidos manualmente. O teste sintético foi perfeito, mas esse resultado real mostra por que o synthetic pass não basta.

O `field_confidence` atual do pipeline NFS-e ainda contém constantes heurísticas. A interface deve apresentá-las como sinal heurístico, nunca como probabilidade calibrada, até existir amostra rotulada suficiente e validação de calibração.

O pipeline de produção continua determinístico e a inferência Llama permanece experimental. Próximos passos para promover qualquer melhoria: montar um conjunto de aceitação redigido/anônimo com layouts variados, medir exact match/evidência por campo, corrigir os casos recorrentes e manter conferência obrigatória onde houver divergência.

## Checklist de adoção em outro projeto

- [ ] Definir campos, regras, evidência obrigatória e quem pode revisar/aprovar.
- [ ] Guardar versão do pipeline/modelo e origem/checksum de cada tentativa.
- [ ] Registrar `before`/`after` e apenas os campos realmente alterados.
- [ ] Validar e testar um salvamento sem alterações (não deve gerar evento de correção).
- [ ] Criar métricas por campo e uma amostra gold com controle de acesso.
- [ ] Separar dados reais de fixtures sintéticas/anonimizadas.
- [ ] Estabelecer critérios de promoção, revisão humana e rollback.
