# Gestor Neto Contabilidade — Pipelines de Dados

Automação para coletar processos, entregas e eventos de e-mail da API da Acessórias
contábil. O objetivo é popular `data/events.json` sem travar a execução mesmo na
primeira carga ou quando o token estiver inválido.

## Pré-requisitos
- Windows 10 ou superior (scripts principais em PowerShell/Batch)
- Python 3.10+ instalado e disponível no `PATH`
- Acesso à API da Acessórias com token válido
- Credenciais IMAP liberadas para leitura

Crie um arquivo `.env` na raiz com as variáveis abaixo:

```ini
ACESSORIAS_TOKEN=seu_token_aqui
IMAP_HOST=imap.seudominio.com
IMAP_USER=usuario@seudominio.com
IMAP_PASSWORD=senha_supersecreta
```

## Teste do token fora do projeto

```powershell
$TOKEN="COLE_SEU_TOKEN_AQUI"
$H=@{Authorization="Bearer $TOKEN"}
iwr -Headers $H "https://api.acessorias.com/processes/ListAll*/?Pagina=1" | select -Expand Content
iwr -Headers $H "https://api.acessorias.com/processes/ListAll/?Pagina=1"  | select -Expand Content
```

Se 401/403: gere novo token e atualize `.env`.

## Execução rápida no VS Code (PowerShell)
1. Abra o diretório do projeto (`File > Open Folder...`).
2. No terminal integrado do VS Code, selecione **PowerShell**.
3. Instale as dependências:
   ```powershell
   python -m pip install --upgrade pip
   python -m pip install python-dotenv requests
   ```
4. Garanta que o `.env` esteja preenchido e revise `scripts\config.json`.
5. Execute o pipeline completo:
   ```powershell
   .\run_all.bat
   ```
6. Ao concluir, abra `web\index.html` no navegador (ou use a aba **Preview** do VS Code)
   para visualizar rapidamente os dados consolidados.

### Saída esperada
- Os arquivos em `data\` são recriados a cada execução.
- `data\events.json` sempre existe; se estiver vazio, uma mensagem de aviso indica
  os itens a conferir (`.env` e filtros no `config.json`).
- Logs amigáveis são emitidos para cada etapa (API, entregas, empresas e IMAP).

## Estrutura dos scripts
- `scripts/fetch_api.py`: baixa processos (`ListAll*/` ou `ListAll/` conforme o filtro).
- `scripts/fetch_deliveries.py`: obtém entregas com intervalo configurável.
- `scripts/fetch_companies.py`: sincroniza o catálogo de empresas.
- `scripts/fetch_email_imap.py`: extrai e-mails recentes via IMAP.
- `scripts/build_events.py`: consolida a saída dos scripts anteriores em `events.json`.

Todos os scripts carregam o `.env` da raiz do projeto, validam variáveis críticas e
utilizam um logger compartilhado (`scripts/utils/logger.py`).

## Registro de mudanças relevante

### v4.0 - Modernização do Modelo de Custos

#### Removido
- ❌ Aba "Resultado_Regime_Sem_Peso"
- ❌ Aba "Resultado_Segmento_Sem_Peso"
- ❌ KPI "Resultado Total (Sem Peso)"
- ❌ Tab "Sem Peso" na interface
- ❌ Seção "Comparativo Sem Peso × Com Peso"

#### Adicionado
- ✅ Rateio consolidado: `peso_base × qtd_clientes`
- ✅ Aba "DRE_Simplificada"
- ✅ Campos em Resumo_Retirada: `retiradas_total`, `%_retiradas_sobre_receita`, `%_retiradas_sobre_resultado`
- ✅ Tratamento pró-labore como retirada (não custo)

#### Modificado
- 🔄 Abatimento advocacia não afeta pró-labores
- 🔄 Apropriação de custos sempre usa rateio consolidado
- 🔄 KPI renomeado: "Resultado Total (Com Peso)" → "Resultado Total"
