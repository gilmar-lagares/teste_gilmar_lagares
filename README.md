# teste_gilmar_lagares# teste_gilmar_lagares# Teste Técnico - Intuitive Care

**Candidato:** [Seu Nome Completo]
**Vaga:** Estágio em Desenvolvimento / Engenharia de Dados

Este projeto é uma solução completa Fullstack e de Engenharia de Dados que realiza a extração (ETL), transformação, armazenamento e visualização de dados contábeis de operadoras de planos de saúde, obtidos do portal de dados abertos da ANS.

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
- Python 3.8 ou superior
- Navegador Web moderno

### Passo 1: Instalação de Dependências
Na raiz do projeto, instale as bibliotecas necessárias:
```bash
pip install -r requirements.txt
Passo 2: Executar o Pipeline de Dados (ETL)
Este script realizará o scraping, download, validação, enriquecimento e consolidação dos dados dos últimos 3 anos.

Bash
python src/etl.py
Aguarde a mensagem "SUCESSO! Pipeline concluído". Os arquivos gerados estarão na pasta data/.

Passo 3: Iniciar a API (Backend)
Inicie o servidor local FastAPI:

Bash
python src/api.py
A API estará disponível em: http://localhost:8000 Documentação automática (Swagger): http://localhost:8000/docs

Passo 4: Acessar o Dashboard (Frontend)
Com a API rodando, abra o arquivo web/index.html diretamente no seu navegador (dois cliques no arquivo ou arraste para o Chrome/Firefox).


⚖️ Decisões Técnicas e Trade-offs
Conforme solicitado, abaixo estão as justificativas para as decisões arquiteturais tomadas durante o desenvolvimento.

1. Engenharia de Dados (ETL)
1.1 Processamento em Memória vs. Incremental

Decisão: Processamento em Memória (pandas).

Justificativa: O volume de dados dos 3 últimos anos, embora relevante, cabe confortavelmente na memória RAM de máquinas modernas (>4GB). O uso do Pandas simplifica drasticamente a lógica de tratamento e agregações estatísticas (vetorização) comparado a um processamento em stream (chunk-by-chunk), atendendo ao princípio KISS (Keep It Simple).

1.2 Tratamento de Inconsistências (CNPJ e Datas)

Duplicidades: Utilizamos os dados do arquivo CADOP (Cadastro de Operadoras) atualizado como "Master Data". Ignoramos as Razões Sociais históricas nos arquivos contábeis e projetamos sempre o nome atual da empresa baseado no RegistroANS, garantindo consistência.

Datas: Devido à inconsistência de formatos dentro dos CSVs, abstraímos a data exata do evento e utilizamos os metadados da estrutura de diretórios da ANS (Ano/Trimestre) para o agrupamento temporal.

1.3 Estratégia de Join (Enriquecimento)

Decisão: Hash Map em Python (Dicionário).

Justificativa: Transformamos o CSV de Cadastro em um dicionário indexado pelo RegistroANS. Isso permite buscas com complexidade O(1) durante a iteração das despesas. É ordens de magnitude mais rápido do que carregar os dados em um banco temporário SQL apenas para fazer o join.

1.4 Validação de CNPJ

Decisão: Soft Validation (Flagging).

Justificativa: Criamos uma coluna CNPJ_Valido (booleana) em vez de descartar o registro. Dados financeiros, mesmo com erros cadastrais, devem ser auditáveis e não apenas deletados silenciosamente do pipeline.

2. Banco de Dados (SQL)
2.1 Normalização

Decisão: Opção B - Tabelas Normalizadas (operadoras e despesas).

Justificativa: Redução de redundância e economia de armazenamento. A Razão Social e UF se repetem milhares de vezes na tabela de fatos. Separar em uma tabela dimensão facilita a atualização cadastral (alterar o nome da empresa em apenas um lugar).

2.2 Tipos de Dados

Monetário: DECIMAL(15,2). O uso de FLOAT foi descartado para evitar erros de precisão em ponto flutuante, críticos em sistemas financeiros.

Datas: DATE. Suficiente para fechamentos trimestrais, sem necessidade da precisão de TIMESTAMP.

3. Backend (API)
3.1 Framework

Decisão: FastAPI.

Justificativa: Performance superior (Asynchronous) e, principalmente, geração automática de documentação (Swagger UI), o que facilita o teste e a integração com o frontend.

3.2 Paginação

Decisão: Offset-based (page e limit).

Justificativa: Para o volume de dados atual e o requisito de permitir ao usuário "pular" para páginas específicas na interface, o Offset é a solução mais simples e direta. Cursor-based seria excessivo para este caso de uso.

3.3 Cache vs. Real-time

Decisão: Cálculo Real-time.

Justificativa: Como os dados são estáticos (atualizados trimestralmente pelo ETL) e agregados previamente em um CSV otimizado (despesas_agregadas.csv), o tempo de resposta é negligenciável, não justificando a complexidade de um Redis/Memcached.

4. Frontend (Interface Web)
4.1 Busca e Filtro

Decisão: Busca no Servidor (Server-side).

Justificativa: Filtrar no cliente exigiria baixar todo o dataset para o navegador, o que causaria lentidão e alto consumo de dados. A busca via API retorna apenas o subset necessário.

4.2 Gerenciamento de Estado

Decisão: Vue 3 Composition API (ref e reactive).

Justificativa: A aplicação possui escopo limitado a uma "Single Page". Utilizar Vuex ou Pinia adicionaria "boilerplate" desnecessário. O estado local reativo é suficiente e mais legível.

4.3 Tratamento de Erros e UX

Estratégia: Mensagens claras para falha de conexão e estados de "Loading" desativando botões de paginação para evitar múltiplas requisições simultâneas.