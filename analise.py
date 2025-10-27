#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema de Análise Financeira Padronizada
Versão: 4.0 - Modelo Consolidado por Volume
Mudanças principais:
- Visão única baseada em complexidade × volume
- Pró-labores tratados como retirada (fora de custos)
- Nova aba: DRE_Simplificada
- Abatimento advocacia não afeta pró-labores
Python: 3.10+
Autor: Você + IA
Data: 2025-10-26
"""

import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import warnings
warnings.filterwarnings('ignore', category=UserWarning)

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

# ============================================================================
# CONFIGURAÇÕES GLOBAIS
# ============================================================================

DIRETORIO_BASE = Path(r"C:\neto")
ARQUIVO_ENTRADA = DIRETORIO_BASE / "tabela resultados.xlsx"
ARQUIVO_SAIDA   = DIRETORIO_BASE / "analise_financeira_v3.xlsx"
ARQUIVO_LOG     = DIRETORIO_BASE / "analise_log.txt"

# Período acumulado (para cálculo das métricas mensais)
MESES_ACUMULADOS = 10

# Top N em rankings
TOP_N_CLIENTES  = 10
TOP_N_DESPESAS  = 10

# Pesos de complexidade por regime (visão única por peso)
PESOS_COMPLEXIDADE_REGIME: Dict[str, float] = {
    "Simples Nacional": 1.0,
    "Lucro Presumido": 1.2,
    "Lucro Real": 1.6,
    "PF": 0.6,
    "Imune/Isenta": 0.6,
    "Paralisada": 0.5,
    "Não informado": 1.0,
}

# Hiperparâmetros do esforço
ALFA_VOLUME = 0.7  # quanto o nº de empresas pesa (0.5~1.0 recomendado)
BETA_TICKET = 0.3  # quanto o ticket médio relativo pesa (0.0~0.5 recomendado)

# Departamentos e colaboradores (para fuzzy match)
COLABORADORES_DEPARTAMENTO = {
    "FISCAL": [
        "DENISE CORREA DE PAIVA",
        "FERNANDO BORGES CESAR",
        "CARLA DOS SANTOS SILVA MENDES",
    ],
    "PESSOAL": [
        "JOYCE GUIMARÃES MENDES CARVALHO",
        "ALDENEIDE CORREA DE PAIVA",
        "AMANDA DOS SANTOS OLIVEIRA",
        "GUILHERME MARRA CARDOSO",
    ],
    "CONTABILIDADE": [
        "RHAIANE MORAIS MATOS",
        "RHAIANE MORAES MATOS",
        "JOÃO VITOR GUIMARAES SILVA",
        "MATHEUS GONÇALVES GODOI LOBO",
        "MARCIO TERUO YAMAMOTO",
        "ANA LAURA GUIMARAES CARVALHO",
        "JHONATAN THYAGO COUTINHO DE SOUZA",
        "JONATHAN THYAGO COUTINHO DE SOUSA",
    ],
    "CADASTRO": [
        "MARIA CLARA SANTOS CAMARGO",
        "JULIANA DE OLIVEIRA MARQUES",
    ],
}
DEPARTAMENTOS  = ["CONTABILIDADE", "FISCAL", "PESSOAL", "CADASTRO"]
THRESHOLD_FUZZY = 90

# Rateio trabalhista
TIPOS_DIRETOS_RATEIO = ["SALÁRIO", "FÉRIAS"]
TIPOS_GENERICOS      = ["INSS", "FGTS", "IRRF", "VALE TRANSPORTE", "AJUDA DE CUSTO"]

# Agrupamentos de despesas
CATEGORIAS_DESPESAS = {
    "DESPESAS ADMINISTRATIVAS": ["DESPESAS ADMINISTRATIVAS", "ADMINISTRATIVA", "ADMINISTRATIVAS"],
    "DESPESAS TRABALHISTAS":    ["DESPESAS TRABALHISTAS", "TRABALHISTA", "TRABALHISTAS"],
    "DESPESAS TRIBUTÁRIAS":     ["DESPESAS TRIBUTÁRIAS", "DESPESAS TRIBUTARIAS", "DESPESAS TRIBUTARIAIS"],
    "DESPESAS FINANCEIRAS":     ["DESPESAS FINANCEIRAS", "FINANCEIRA", "FINANCEIRAS"],
    "DESPESAS BANCÁRIAS":       ["DESPESAS BANCÁRIAS", "DESPESAS BANCARIAS", "BANCARIA", "BANCARIAS"],
}

TIPOS_TRABALHISTAS = {
    "SALÁRIO":          ["SALÁRIO", "SALARIOS", "SALARIO"],
    "FÉRIAS":           ["FÉRIAS", "FERIAS"],
    "INSS":             ["INSS"],
    "FGTS":             ["FGTS"],
    "IRRF":             ["IRRF"],
    "VALE TRANSPORTE":  ["VALE TRANSPORTE", "VT"],
    "AJUDA DE CUSTO":   ["AJUDA DE CUSTO"],
    "OUTROS":           [],
}

PROLABORE_SOCIOS = [
    "PRO-LABORE - MARCO",
    "PRO-LABORE - MATEUS",
]

TICKET_GERAL_COLS = ["receita_mensal", "ticket_medio_mensal", "qtd_clientes"]
TICKET_REGIME_COLS = ["regime", "clientes", "receita", "ticketMedio"]
TICKET_REGIME_ATIV_COLS = ["regime", "atividade", "clientes", "receita", "ticketMedio"]
TICKET_CLIENTES_COLS = ["cliente", "regime", "atividade", "receita_mensal", "ticket_mensal"]
RESULTADO_REGIME_COLS = [
    "regime_base",
    "qtd_clientes",
    "receita_mensal",
    "custo_mensal",
    "resultado_mensal",
    "ticket_medio_mensal",
    "custo_medio_mensal",
    "resultado_medio_mensal",
]
RESULTADO_SEGMENTO_COLS = [
    "regime_base",
    "atividade",
    "qtd_clientes",
    "receita_mensal",
    "custo_mensal",
    "resultado_mensal",
    "ticket_medio_mensal",
    "custo_medio_mensal",
    "resultado_medio_mensal",
]

RESULTADO_REGIME_FULL_COLS = RESULTADO_REGIME_COLS + [
    "peso_complexidade",
    "peso_total",
    "participacao_peso",
    "participacao_esforco",
    "receita_regime",
]


def _empty_df(columns: List[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _prepare_regime_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(RESULTADO_REGIME_COLS)
    base = df.copy()
    for col in RESULTADO_REGIME_COLS:
        if col not in base.columns:
            base[col] = 0.0
    base = base[RESULTADO_REGIME_COLS]
    base['qtd_clientes'] = base['qtd_clientes'].fillna(0).astype(int)
    return base


def _prepare_segmento_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(RESULTADO_SEGMENTO_COLS)
    base = df.copy()
    rename_map = {
        'segmento': 'atividade',
        'receita_mensal_segmento': 'receita_mensal',
        'custo_mensal_segmento': 'custo_mensal',
    }
    for src, dst in rename_map.items():
        if src in base.columns:
            base[dst] = base[src]
        elif dst not in base.columns:
            base[dst] = 0.0
    for col in RESULTADO_SEGMENTO_COLS:
        if col not in base.columns:
            base[col] = 0.0
    base = base[RESULTADO_SEGMENTO_COLS]
    base['qtd_clientes'] = base['qtd_clientes'].fillna(0).astype(int)
    base['atividade'] = base['atividade'].fillna('')
    return base

# Cache para reuso de parsing
_SHEETS_INFO_CACHE: Optional[Dict[str, Optional[str]]] = None
_PLANILHA_UNICA_CACHE: Optional[Dict[str, pd.DataFrame]] = None

# ============================================================================
# SANITIZAÇÃO PARA EXCEL (evita fórmulas removidas)
# ============================================================================

FORMULA_PREFIXES = ('=', '+', '-', '@')

def _sanitize_excel_text(s):
    if s is None or pd.isna(s):
        return s
    if not isinstance(s, str):
        s = str(s)
    s = s.replace('\r', ' ').replace('\x0b', ' ').replace('\x0c', ' ')
    stripped = s.lstrip()
    if stripped and set(stripped.strip()) == {'='}:
        return '-' * 80
    if stripped.startswith(FORMULA_PREFIXES):
        return "'" + s
    return s

def _safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    clean = df.copy()
    for col in clean.columns:
        if pd.api.types.is_object_dtype(clean[col]) or pd.api.types.is_string_dtype(clean[col]):
            clean[col] = clean[col].apply(_sanitize_excel_text)
    return clean

def _safe_to_excel(df: pd.DataFrame, writer, sheet_name: str):
    _safe_df(df).to_excel(writer, sheet_name=sheet_name, index=False)

# ============================================================================
# UTILITÁRIOS
# ============================================================================

def normaliza_texto(texto: str, remover_acentos: bool = True, minuscula: bool = True) -> str:
    if not isinstance(texto, str):
        texto = str(texto) if pd.notna(texto) else ""
    texto = texto.strip()
    texto = re.sub(r'\s+', ' ', texto)
    if remover_acentos:
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    if minuscula:
        texto = texto.lower()
    return texto

def normaliza_texto_comparacao(texto: str) -> str:
    texto = normaliza_texto(texto, remover_acentos=True, minuscula=True)
    particulas = ['da', 'de', 'do', 'das', 'dos', 'e']
    palavras = [p for p in texto.split() if p not in particulas]
    return ' '.join(palavras)

def converte_para_numero(valor) -> float:
    if pd.isna(valor):
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        v = valor.strip()
        v = re.sub(r'[^\d,.\-]', '', v)
        if ',' in v and '.' in v:
            v = v.replace('.', '').replace(',', '.')
        elif ',' in v:
            v = v.replace(',', '.')
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

# ============================================================================
# DETECÇÕES ESPECIAIS
# ============================================================================

def is_rateio_advocacia(nome_cliente: str) -> bool:
    """
    Identifica a receita que deve virar abatimento em DESPESAS ADMINISTRATIVAS.
    Critérios: contém 'rateio' + 'despesa' + ('advocacia' ou 'escritorio')
    """
    if not isinstance(nome_cliente, str):
        return False
    t = normaliza_texto(nome_cliente, remover_acentos=True, minuscula=True)
    return ("rateio" in t and "despesa" in t and ("advocacia" in t or "escritorio" in t))

def is_prolabore_socio(item_nome: str) -> bool:
    if not isinstance(item_nome, str):
        return False
    t = normaliza_texto(item_nome, remover_acentos=True, minuscula=False).upper()
    return any(t.startswith(x) for x in PROLABORE_SOCIOS)

# ============================================================================
# PARSING / CLASSIFICAÇÃO
# ============================================================================

def parse_regime_e_segmento(regime_texto: str) -> Tuple[str, str]:
    if pd.isna(regime_texto) or not str(regime_texto).strip():
        return "Não informado", "N/A"
    regime_original = str(regime_texto).strip()
    regime_norm = normaliza_texto(regime_original, remover_acentos=True, minuscula=True)
    regime_base = "Não informado"
    if "simples" in regime_norm:
        regime_base = "Simples Nacional"
    elif "presumid" in regime_norm:
        regime_base = "Lucro Presumido"
    elif "real" in regime_norm:
        regime_base = "Lucro Real"
    elif regime_norm in ["pf", "pessoa fisica", "pessoa física"]:
        regime_base = "PF"
    elif "paralisad" in regime_norm:
        regime_base = "Paralisada"
    elif "imune" in regime_norm or "isent" in regime_norm:
        regime_base = "Imune/Isenta"
    segmento = "N/A"
    if " - " in regime_original:
        partes = regime_original.split(" - ", 1)
        if len(partes) > 1:
            seg_raw = partes[1].strip()
            segmento = normaliza_texto(seg_raw, remover_acentos=True, minuscula=False).upper()
    return regime_base, segmento

def classifica_tipo_receita(cliente_nome: str) -> str:
    nome_norm = normaliza_texto(cliente_nome, minuscula=True)
    if "certificado" in nome_norm or "certidao" in nome_norm:
        return "CERTIFICADO"
    if any(k in nome_norm for k in ["financeira", "juros", "rendimento"]):
        return "FINANCEIRA"
    if any(k in nome_norm for k in ["rateio", "reembolso", "repasse"]):
        return "OUTRAS"
    return "HONORÁRIOS"

def classifica_grupo_despesa(grupo_texto: str) -> str:
    if pd.isna(grupo_texto):
        return "OUTROS"
    grupo_norm = normaliza_texto(str(grupo_texto), remover_acentos=True, minuscula=True)
    for categoria_padrao, variantes in CATEGORIAS_DESPESAS.items():
        for variante in variantes:
            if normaliza_texto(variante, remover_acentos=True, minuscula=True) in grupo_norm:
                return categoria_padrao
    return "OUTROS"

def extrai_colaborador(item_nome: str) -> str:
    if pd.isna(item_nome) or " - " not in str(item_nome):
        return ""
    partes = str(item_nome).split(" - ", 1)
    if len(partes) < 2:
        return ""
    colaborador = partes[1].strip()
    for tipo_prefixos in TIPOS_TRABALHISTAS.values():
        for prefixo in tipo_prefixos:
            prefixo_norm = normaliza_texto(prefixo, remover_acentos=True, minuscula=False)
            colaborador = re.sub(rf'^{re.escape(prefixo_norm)}\s*-?\s*', '', colaborador, flags=re.IGNORECASE)
    return colaborador.strip()

def classifica_tipo_trabalhista(item_nome: str) -> str:
    if pd.isna(item_nome):
        return "OUTROS"
    item_norm = normaliza_texto(str(item_nome), remover_acentos=True, minuscula=True)
    for tipo, prefixos in TIPOS_TRABALHISTAS.items():
        if tipo == "OUTROS":
            continue
        for prefixo in prefixos:
            prefixo_norm = normaliza_texto(prefixo, remover_acentos=True, minuscula=True)
            if item_norm.startswith(prefixo_norm):
                return tipo
    return "OUTROS"

def classifica_departamento_fuzzy(colaborador: str) -> Tuple[str, int]:
    if not colaborador:
        return "SEM MATCH", 0
    colab_norm = normaliza_texto_comparacao(colaborador)
    melhor_dept = "SEM MATCH"
    melhor_score = 0
    melhor_nome = ""
    for dept, nomes in COLABORADORES_DEPARTAMENTO.items():
        for nome in nomes:
            nome_norm = normaliza_texto_comparacao(nome)
            score = fuzz.token_set_ratio(colab_norm, nome_norm)
            if score > melhor_score:
                melhor_score = score
                melhor_dept = dept
                melhor_nome = nome
            elif score == melhor_score and score >= THRESHOLD_FUZZY:
                tokens_atual  = set(colab_norm.split())
                tokens_nome   = set(nome_norm.split())
                tokens_melhor = set(normaliza_texto_comparacao(melhor_nome).split())
                if len(tokens_atual & tokens_nome) > len(tokens_atual & tokens_melhor):
                    melhor_dept = dept
                    melhor_nome = nome
                elif len(tokens_atual & tokens_nome) == len(tokens_atual & tokens_melhor):
                    if nome < melhor_nome:
                        melhor_dept = dept
                        melhor_nome = nome
    if melhor_score < THRESHOLD_FUZZY:
        return "SEM MATCH", melhor_score
    return melhor_dept, melhor_score

# ============================================================================
# CARREGAMENTO DOS DADOS
# ============================================================================

def _detectar_abas_excel(caminho: Path, log: List[str]) -> Dict[str, Optional[str]]:
    """Determina se o arquivo possui abas separadas ou layout combinado."""
    global _SHEETS_INFO_CACHE
    if _SHEETS_INFO_CACHE is not None:
        return _SHEETS_INFO_CACHE

    info: Dict[str, Optional[str]] = {
        'modo': None,
        'sheet_receitas': None,
        'sheet_despesas': None,
        'sheet_unica': None,
        'erro': None,
    }

    try:
        xls = pd.ExcelFile(caminho)
    except Exception as e:
        info['erro'] = str(e)
        log.append(f"✗ ERRO ao abrir '{caminho}': {e}")
        _SHEETS_INFO_CACHE = info
        return info

    sheets = xls.sheet_names
    log.append(f"✓ Abas encontradas: {sheets}")

    def _fuzzy_find(keywords: List[str]) -> Tuple[Optional[str], float]:
        melhor_nome = None
        melhor_score = 0.0
        for sheet in sheets:
            sheet_norm = sheet.lower()
            for alvo in keywords:
                score = fuzz.partial_ratio(sheet_norm, alvo)
                if score > melhor_score:
                    melhor_score = score
                    melhor_nome = sheet
        return melhor_nome, melhor_score

    sheet_rec, score_rec = _fuzzy_find(["receita", "receitas"])
    sheet_desp, score_desp = _fuzzy_find(["despesa", "despesas"])

    threshold = 70.0
    if (
        sheet_rec
        and sheet_desp
        and sheet_rec != sheet_desp
        and score_rec >= threshold
        and score_desp >= threshold
    ):
        info['modo'] = 'duas_abas'
        info['sheet_receitas'] = sheet_rec
        info['sheet_despesas'] = sheet_desp
        log.append(
            f"→ Estratégia de leitura: abas dedicadas (Receitas='{sheet_rec}', Despesas='{sheet_desp}')"
        )
    else:
        # fallback para aba única combinada
        sheet_unica: Optional[str] = None
        if len(sheets) == 1:
            sheet_unica = sheets[0]
        else:
            for candidato in [sheet_rec, sheet_desp] + sheets:
                if candidato:
                    sheet_unica = candidato
                    break
        if sheet_unica is None:
            info['erro'] = 'Nenhuma aba disponível para leitura.'
            log.append("✗ ERRO: Nenhuma aba encontrada no arquivo.")
        else:
            info['modo'] = 'aba_unica'
            info['sheet_unica'] = sheet_unica
            if sheet_rec and sheet_desp and sheet_rec == sheet_desp:
                log.append(
                    f"→ Estratégia de leitura: aba única '{sheet_unica}' (receitas+despesas na mesma guia)"
                )
            else:
                log.append(
                    f"→ Estratégia de leitura: aba única '{sheet_unica}' (layout combinado)"
                )

    _SHEETS_INFO_CACHE = info
    return info


def _coluna_por_indice(df: pd.DataFrame, indice: int) -> Optional[str]:
    if indice < len(df.columns):
        return df.columns[indice]
    return None


def _parse_planilha_unica(
    caminho: Path,
    sheet_name: str,
    log: List[str]
) -> Dict[str, pd.DataFrame]:
    """Parser específico para o layout combinado (Planilha única)."""
    global _PLANILHA_UNICA_CACHE
    if _PLANILHA_UNICA_CACHE is not None:
        return _PLANILHA_UNICA_CACHE

    try:
        df_raw = pd.read_excel(caminho, sheet_name=sheet_name, header=0)
    except Exception as e:
        log.append(f"✗ ERRO ao ler aba única '{sheet_name}': {e}")
        _PLANILHA_UNICA_CACHE = {
            'df_receitas': pd.DataFrame(),
            'df_despesas': pd.DataFrame(),
            'valor_rateio_adv': 0.0,
            'linhas_receitas_lidas': 0,
            'linhas_despesas_lidas': 0,
            'erro': str(e),
        }
        return _PLANILHA_UNICA_CACHE

    # ---------------- RECEITAS ----------------
    col_cliente = _coluna_por_indice(df_raw, 0)
    col_regime = _coluna_por_indice(df_raw, 1)
    col_receita = _coluna_por_indice(df_raw, 2)

    df_receitas_raw = df_raw[[c for c in [col_cliente, col_regime, col_receita] if c is not None]].copy()
    df_receitas_raw.columns = ['cliente', 'regime_original', 'receita'][: len(df_receitas_raw.columns)]
    df_receitas_raw = df_receitas_raw.dropna(how='all')

    df_receitas_norm = pd.DataFrame()
    if not df_receitas_raw.empty and {'cliente', 'regime_original', 'receita'}.issubset(df_receitas_raw.columns):
        df_receitas_norm['cliente'] = df_receitas_raw['cliente'].apply(
            lambda x: str(x).strip() if pd.notna(x) else ""
        )
        df_receitas_norm['regime_original'] = df_receitas_raw['regime_original'].apply(
            lambda x: str(x).strip() if pd.notna(x) else ""
        )
        df_receitas_norm['receita'] = df_receitas_raw['receita'].apply(converte_para_numero)
        df_receitas_norm['empresa_padronizada'] = df_receitas_norm['cliente'].apply(
            lambda x: normaliza_texto(x, remover_acentos=True, minuscula=False).upper()
        )
        df_receitas_norm[['regime_base', 'segmento']] = df_receitas_norm['regime_original'].apply(
            lambda x: pd.Series(parse_regime_e_segmento(x))
        )
        df_receitas_norm['tipo_receita'] = df_receitas_norm['cliente'].apply(classifica_tipo_receita)
        df_receitas_norm['is_rateio_adv'] = df_receitas_norm['cliente'].apply(is_rateio_advocacia)
    else:
        df_receitas_norm = pd.DataFrame(columns=[
            'cliente', 'regime_original', 'receita', 'empresa_padronizada',
            'regime_base', 'segmento', 'tipo_receita', 'is_rateio_adv'
        ])

    valor_rateio_adv = df_receitas_norm.loc[df_receitas_norm['is_rateio_adv'], 'receita'].sum()
    df_receitas_norm = df_receitas_norm[~df_receitas_norm['is_rateio_adv']].copy()
    df_receitas_norm = df_receitas_norm[df_receitas_norm['cliente'] != ""].copy()

    # ---------------- DESPESAS ----------------
    def _col_por_nome_ou_indice(prefixo: str, fallback_indice: int) -> Optional[str]:
        for col in df_raw.columns:
            if isinstance(col, str) and col.strip().lower().startswith(prefixo):
                return col
        return _coluna_por_indice(df_raw, fallback_indice)

    col_adm_item = _col_por_nome_ou_indice('unnamed: 6', 6)
    col_adm_valor = _col_por_nome_ou_indice('unnamed: 7', 7)
    col_trab_item = _col_por_nome_ou_indice('unnamed: 9', 9)
    col_trab_valor = _col_por_nome_ou_indice('unnamed: 10', 10)

    registros: List[pd.DataFrame] = []

    def _monta_bloco(col_item: Optional[str], col_valor: Optional[str], grupo: str) -> pd.DataFrame:
        if col_item is None or col_valor is None:
            return pd.DataFrame(columns=['grupo', 'item_nome', 'valor', 'id_linha'])
        bloco = df_raw[[col_item, col_valor]].copy()
        bloco.columns = ['item_nome', 'valor']
        bloco['item_nome_raw'] = bloco['item_nome']
        bloco['valor_raw'] = bloco['valor']
        bloco['item_nome'] = bloco['item_nome'].apply(
            lambda x: str(x).strip() if pd.notna(x) else ""
        )
        bloco['valor'] = bloco['valor'].apply(converte_para_numero)
        labels_excluir = {'VALOR', 'DESPESAS ADMINISTRATIVAS', 'DESPESAS TRABALHISTAS', 'TOTAL'}
        bloco = bloco[~bloco['item_nome'].str.upper().isin(labels_excluir)]
        bloco = bloco[~bloco['valor_raw'].apply(
            lambda x: str(x).strip().upper() if pd.notna(x) else ""
        ).isin(labels_excluir)]
        bloco = bloco[~((bloco['item_nome'] == "") & (bloco['valor'] == 0.0))]
        bloco['grupo'] = grupo
        bloco['id_linha'] = bloco.index + 2  # aproximado ao nº de linha no Excel
        return bloco[['grupo', 'item_nome', 'valor', 'id_linha']]

    registros.append(_monta_bloco(col_adm_item, col_adm_valor, 'DESPESAS ADMINISTRATIVAS'))
    registros.append(_monta_bloco(col_trab_item, col_trab_valor, 'DESPESAS TRABALHISTAS'))

    df_despesas_norm = pd.concat(registros, ignore_index=True) if registros else pd.DataFrame()
    if df_despesas_norm.empty:
        df_despesas_norm = pd.DataFrame(columns=['grupo', 'item_nome', 'valor', 'id_linha'])

    df_despesas_norm['tipo_trabalhista'] = ""
    df_despesas_norm['colaborador'] = ""
    df_despesas_norm['departamento_classificado'] = ""
    df_despesas_norm['fuzzy_score'] = 0.0

    mask_trab = df_despesas_norm['grupo'] == 'DESPESAS TRABALHISTAS'
    if mask_trab.any():
        df_despesas_norm.loc[mask_trab, 'tipo_trabalhista'] = df_despesas_norm.loc[mask_trab, 'item_nome'].apply(classifica_tipo_trabalhista)
        df_despesas_norm.loc[mask_trab, 'colaborador'] = df_despesas_norm.loc[mask_trab, 'item_nome'].apply(extrai_colaborador)
        classificacoes = df_despesas_norm.loc[mask_trab, 'colaborador'].apply(
            lambda x: classifica_departamento_fuzzy(x) if x else ("SEM MATCH", 0)
        )
        df_despesas_norm.loc[mask_trab, 'departamento_classificado'] = [c[0] for c in classificacoes]
        df_despesas_norm.loc[mask_trab, 'fuzzy_score'] = np.array([c[1] for c in classificacoes], dtype=float)

    _PLANILHA_UNICA_CACHE = {
        'df_receitas': df_receitas_norm,
        'df_despesas': df_despesas_norm,
        'valor_rateio_adv': float(valor_rateio_adv),
        'linhas_receitas_lidas': int(len(df_receitas_raw)),
        'linhas_despesas_lidas': int(len(df_despesas_norm)),
        'erro': None,
    }
    return _PLANILHA_UNICA_CACHE

def carrega_receitas(caminho: Path, log: List[str]) -> Tuple[pd.DataFrame, float]:
    """
    Lê RECEITAS e retorna (df_receitas_validas, valor_rateio_advocacia),
    removendo do DF as linhas de 'rateio advocacia', para que elas não entrem como receita.
    """
    log.append("\n" + "-"*80)
    log.append("PROCESSANDO RECEITAS")
    log.append("-"*80)
    info = _detectar_abas_excel(caminho, log)
    if info.get('erro') and info['erro']:
        log.append("✗ ERRO: Não foi possível detectar as abas de receitas.")
        return pd.DataFrame(), 0.0

    modo = info.get('modo')
    if modo == 'aba_unica':
        cache = _parse_planilha_unica(caminho, info.get('sheet_unica', ''), log)
        if cache.get('erro'):
            log.append("✗ ERRO: Falha ao interpretar aba única de receitas.")
            return pd.DataFrame(), 0.0
        df_norm = cache['df_receitas'].copy()
        valor_rateio_adv = cache['valor_rateio_adv']
        log.append(f"✓ Linhas lidas (aba única): {cache['linhas_receitas_lidas']}")
    else:
        sheet_receitas = info.get('sheet_receitas') or 'RECEITAS'
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_receitas)
            log.append(f"✓ Linhas lidas ({sheet_receitas}): {len(df)}")
        except Exception as e:
            log.append(f"✗ ERRO ao ler RECEITAS ('{sheet_receitas}'): {e}")
            return pd.DataFrame(), 0.0

        df.columns = [normaliza_texto(str(c), remover_acentos=True, minuscula=False).upper() for c in df.columns]

        col_cliente = col_regime = col_receita = None
        for col in df.columns:
            col_norm = normaliza_texto(col, remover_acentos=True, minuscula=True)
            if "honorario" in col_norm and "contab" in col_norm:
                col_cliente = col
            elif "regime" in col_norm and "empresa" in col_norm:
                col_regime = col
            elif "receita" in col_norm:
                col_receita = col

        if not all([col_cliente, col_regime, col_receita]):
            log.append(f"✗ ERRO: Colunas esperadas não encontradas em RECEITAS. Disponíveis: {df.columns.tolist()}")
            return pd.DataFrame(), 0.0

        df_norm = pd.DataFrame()
        df_norm['cliente'] = df[col_cliente].apply(lambda x: str(x).strip() if pd.notna(x) else "")
        df_norm['regime_original'] = df[col_regime].apply(lambda x: str(x).strip() if pd.notna(x) else "")
        df_norm['receita'] = df[col_receita].apply(converte_para_numero)
        df_norm['empresa_padronizada'] = df_norm['cliente'].apply(
            lambda x: normaliza_texto(x, remover_acentos=True, minuscula=False).upper()
        )
        df_norm[['regime_base', 'segmento']] = df_norm['regime_original'].apply(
            lambda x: pd.Series(parse_regime_e_segmento(x))
        )
        df_norm['tipo_receita'] = df_norm['cliente'].apply(classifica_tipo_receita)
        df_norm['is_rateio_adv'] = df_norm['cliente'].apply(is_rateio_advocacia)
        valor_rateio_adv = df_norm.loc[df_norm['is_rateio_adv'], 'receita'].sum()
        df_norm = df_norm[~df_norm['is_rateio_adv']].copy()

    if 'is_rateio_adv' in df_norm.columns:
        df_norm = df_norm.drop(columns=['is_rateio_adv'])

    df_norm = df_norm[df_norm['cliente'] != ""].copy()

    log.append(f"✓ Receitas válidas (já excluído rateio-advocacia): {len(df_norm)}")
    log.append(f"✓ Receita total (válida): R$ {df_norm['receita'].sum():,.2f}")
    log.append(f"  • Receita CORE: R$ {df_norm[df_norm['tipo_receita']=='HONORÁRIOS']['receita'].sum():,.2f}")
    log.append(f"  • Receita ACESSÓRIA: R$ {df_norm[df_norm['tipo_receita']!='HONORÁRIOS']['receita'].sum():,.2f}")
    log.append(f"  • Abatimento rateio-advocacia a aplicar em ADM (excluindo pró-labores): R$ {valor_rateio_adv:,.2f}")
    return df_norm, float(valor_rateio_adv)

def carrega_despesas(caminho: Path, log: List[str]) -> pd.DataFrame:
    log.append("\n" + "-"*80)
    log.append("PROCESSANDO DESPESAS")
    log.append("-"*80)
    info = _detectar_abas_excel(caminho, log)
    if info.get('erro') and info['erro']:
        log.append("✗ ERRO: Não foi possível detectar as abas de despesas.")
        return pd.DataFrame()

    modo = info.get('modo')
    if modo == 'aba_unica':
        cache = _parse_planilha_unica(caminho, info.get('sheet_unica', ''), log)
        if cache.get('erro'):
            log.append("✗ ERRO: Falha ao interpretar aba única de despesas.")
            return pd.DataFrame()
        df_norm = cache['df_despesas'].copy()
        log.append(f"✓ Linhas lidas (aba única): {cache['linhas_despesas_lidas']}")
    else:
        sheet_despesas = info.get('sheet_despesas') or 'DESPESAS'
        try:
            df = pd.read_excel(caminho, sheet_name=sheet_despesas, header=None)
            log.append(f"✓ Linhas lidas ({sheet_despesas}): {len(df)}")
        except Exception as e:
            log.append(f"✗ ERRO ao ler DESPESAS ('{sheet_despesas}'): {e}")
            return pd.DataFrame()

        registros = []
        for idx in range(1, len(df)):
            grupo_raw = df.iloc[idx, 0] if len(df.columns) > 0 else None
            nome_raw = df.iloc[idx, 1] if len(df.columns) > 1 else None
            valor_raw = df.iloc[idx, 2] if len(df.columns) > 2 else None

            if pd.isna(grupo_raw) and pd.isna(nome_raw) and pd.isna(valor_raw):
                continue

            grupo = classifica_grupo_despesa(grupo_raw)
            nome = str(nome_raw).strip() if pd.notna(nome_raw) else ""
            valor = converte_para_numero(valor_raw)

            if nome and valor != 0:
                registros.append({'grupo': grupo, 'item_nome': nome, 'valor': valor, 'id_linha': idx + 1})

        df_norm = pd.DataFrame(registros)
        if df_norm.empty:
            log.append("⚠ AVISO: Nenhuma despesa válida")
            return df_norm

        df_norm['tipo_trabalhista'] = ""
        df_norm['colaborador'] = ""
        df_norm['departamento_classificado'] = ""
        df_norm['fuzzy_score'] = 0.0  # dtype float para evitar FutureWarning

        mask_trab = df_norm['grupo'] == "DESPESAS TRABALHISTAS"
        if mask_trab.any():
            df_norm.loc[mask_trab, 'tipo_trabalhista'] = df_norm.loc[mask_trab, 'item_nome'].apply(classifica_tipo_trabalhista)
            df_norm.loc[mask_trab, 'colaborador'] = df_norm.loc[mask_trab, 'item_nome'].apply(extrai_colaborador)

            classificacoes = df_norm.loc[mask_trab, 'colaborador'].apply(
                lambda x: classifica_departamento_fuzzy(x) if x else ("SEM MATCH", 0)
            )
            df_norm.loc[mask_trab, 'departamento_classificado'] = [c[0] for c in classificacoes]
            df_norm.loc[mask_trab, 'fuzzy_score'] = np.array([c[1] for c in classificacoes], dtype=float)

    log.append(f"✓ Total de despesas normalizadas: {len(df_norm)}")
    for grp in sorted(df_norm['grupo'].unique()):
        total = df_norm[df_norm['grupo'] == grp]['valor'].sum()
        log.append(f"  • {grp}: R$ {total:,.2f}")
    return df_norm

def redistribui_abatimento_admin_sem_afetar_prolabores(
    df_despesas: pd.DataFrame,
    abatimento_adm: float,
    log: List[str]
) -> pd.DataFrame:
    """
    Reduz proporcionalmente APENAS as despesas administrativas que NÃO são os dois pró-labores.
    Se o abatimento exceder o total elegível, aplica o máximo possível e lança o excedente como
    linha 'ABATIMENTO EXCEDENTE – ADM' (não toca pró-labores).
    """
    if df_despesas.empty or abatimento_adm == 0:
        return df_despesas.copy()

    df = df_despesas.copy()
    is_admin = df['grupo'] == 'DESPESAS ADMINISTRATIVAS'
    is_prolab = df['item_nome'].apply(is_prolabore_socio)

    base_elegivel = df.loc[is_admin & ~is_prolab, 'valor'].sum()
    abat = abs(float(abatimento_adm))  # sempre positivo para facilitar

    if base_elegivel <= 0:
        # Não há base para reduzir: adiciona linha negativa separada
        df = pd.concat([df, pd.DataFrame([{
            'grupo': 'DESPESAS ADMINISTRATIVAS',
            'item_nome': 'ABATIMENTO EXCEDENTE – ADM',
            'valor': -abat,
            'id_linha': 0,
            'tipo_trabalhista': '',
            'colaborador': '',
            'departamento_classificado': '',
            'fuzzy_score': 0.0
        }])], ignore_index=True)
        log.append(f"⚠ Abatimento ADM sem base elegível; registrado como linha separada: R$ {abat:,.2f}")
        return df

    # Reduz proporcionalmente as linhas elegíveis
    fator = max(0.0, (base_elegivel - abat) / base_elegivel)
    df.loc[is_admin & ~is_prolab, 'valor'] = df.loc[is_admin & ~is_prolab, 'valor'] * fator

    # Se houve abatimento maior que a base elegível (fator==0 e ainda sobra), lança excedente
    valor_aplicado = base_elegivel - base_elegivel * fator
    excedente = abat - valor_aplicado
    if excedente > 1e-6:
        df = pd.concat([df, pd.DataFrame([{
            'grupo': 'DESPESAS ADMINISTRATIVAS',
            'item_nome': 'ABATIMENTO EXCEDENTE – ADM',
            'valor': -excedente,
            'id_linha': 0,
            'tipo_trabalhista': '',
            'colaborador': '',
            'departamento_classificado': '',
            'fuzzy_score': 0.0
        }])], ignore_index=True)
        log.append(f"⚠ Abatimento ADM excedente lançado à parte: R$ {excedente:,.2f}")
    else:
        log.append(f"✓ Abatimento ADM aplicado proporcionalmente (sem afetar pró-labores): R$ {abat:,.2f}")

    return df

# ============================================================================
# RATEIO PROPORCIONAL (mantido - massa salarial direta)
# ============================================================================

def calcula_base_rateio(df_despesas: pd.DataFrame, log: List[str]) -> Dict[str, float]:
    log.append("\n" + "-"*80)
    log.append("CALCULANDO BASE DE RATEIO (Massa Salarial Direta)")
    log.append("-"*80)

    mask_trab = df_despesas['grupo'] == "DESPESAS TRABALHISTAS"
    df_trab = df_despesas[mask_trab].copy()

    mask_direto = (
        df_trab['tipo_trabalhista'].isin(TIPOS_DIRETOS_RATEIO) &
        (df_trab['departamento_classificado'] != "SEM MATCH") &
        (df_trab['colaborador'] != "")
    )
    df_direto = df_trab[mask_direto].copy()

    massa_por_dept: Dict[str, float] = {}
    total_massa = 0.0
    for dept in DEPARTAMENTOS:
        massa = df_direto[df_direto['departamento_classificado'] == dept]['valor'].sum()
        massa_por_dept[dept] = massa
        total_massa += massa

    pesos: Dict[str, float] = {}
    if total_massa > 0:
        for dept in DEPARTAMENTOS:
            pesos[dept] = massa_por_dept[dept] / total_massa
    else:
        log.append("⚠ AVISO: Massa salarial direta = 0. Usando fallback 25% cada.")
        for dept in DEPARTAMENTOS:
            pesos[dept] = 0.25

    for dept in DEPARTAMENTOS:
        log.append(f"  • {dept}: R$ {massa_por_dept.get(dept, 0):,.2f}  -> {pesos[dept]*100:.2f}%")
    log.append(f"  • TOTAL MASSA: R$ {total_massa:,.2f}")
    return pesos

def aplica_rateio_proporcional(df_despesas: pd.DataFrame, pesos: Dict[str, float], log: List[str]) -> pd.DataFrame:
    log.append("\n" + "-"*80)
    log.append("APLICANDO RATEIO PROPORCIONAL (genéricos/sem match)")
    log.append("-"*80)

    mask_trab = df_despesas['grupo'] == "DESPESAS TRABALHISTAS"
    df_trab   = df_despesas[mask_trab].copy()
    df_outros = df_despesas[~mask_trab].copy()

    if df_trab.empty:
        df_despesas['departamento_final'] = df_despesas['departamento_classificado']
        log.append("⚠ Nenhuma despesa trabalhista")
        return df_despesas

    df_trab['precisa_rateio'] = (
        (df_trab['departamento_classificado'] == "SEM MATCH") |
        (df_trab['tipo_trabalhista'].isin(TIPOS_GENERICOS)) |
        ((df_trab['tipo_trabalhista'] == "FÉRIAS") & (df_trab['colaborador'] == ""))
    )

    registros_finais = []
    total_rateado = 0.0
    qtd_itens_rateados = 0

    for _, row in df_trab.iterrows():
        if row['precisa_rateio']:
            total_rateado += row['valor']
            qtd_itens_rateados += 1
            for dept in DEPARTAMENTOS:
                r = row.copy()
                r['departamento_final'] = dept
                r['valor'] = row['valor'] * pesos[dept]
                registros_finais.append(r)
        else:
            r = row.copy()
            r['departamento_final'] = row['departamento_classificado']
            registros_finais.append(r)

    df_trab_final = pd.DataFrame(registros_finais)
    df_outros['departamento_final'] = ""
    df_final = pd.concat([df_outros, df_trab_final], ignore_index=True)

    log.append(f"✓ Itens rateados: {qtd_itens_rateados}")
    log.append(f"✓ Valor total rateado: R$ {total_rateado:,.2f}")
    for dept in DEPARTAMENTOS:
        valor_dept = df_trab_final[df_trab_final['departamento_final'] == dept]['valor'].sum()
        log.append(f"  • {dept}: R$ {valor_dept:,.2f}")
    return df_final

# ============================================================================
# TICKET MÉDIO + TOP DESPESAS
# ============================================================================

def gera_ticket_medio(df_receitas: pd.DataFrame):
    df_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS'].copy()
    meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1

    if df_core.empty:
        vazio_geral = _empty_df(TICKET_GERAL_COLS)
        vazio_reg = _empty_df(TICKET_REGIME_COLS)
        vazio_reg_ativ = _empty_df(TICKET_REGIME_ATIV_COLS)
        vazio_cli = _empty_df(TICKET_CLIENTES_COLS + ["rank"])
        return vazio_geral, vazio_reg, vazio_reg_ativ, vazio_cli

    df_clientes = (
        df_core.groupby(['cliente', 'regime_base', 'segmento'], dropna=False)['receita']
               .sum().reset_index()
    )

    qtd_clientes = len(df_clientes)
    receita_acumulada = df_clientes['receita'].sum()
    receita_mensal = receita_acumulada / meses if meses else receita_acumulada
    ticket_medio_mensal = (receita_mensal / qtd_clientes) if qtd_clientes > 0 else 0.0

    ticket_geral = pd.DataFrame([{
        "receita_mensal": float(receita_mensal),
        "ticket_medio_mensal": float(ticket_medio_mensal),
        "qtd_clientes": int(qtd_clientes),
    }], columns=TICKET_GERAL_COLS)

    por_regime = (
        df_clientes.groupby('regime_base', dropna=False)['receita']
                   .agg(['sum', 'count']).reset_index()
    )
    por_regime.columns = ['regime', 'receita_acumulada', 'clientes']
    por_regime['receita'] = por_regime['receita_acumulada'] / meses if meses else por_regime['receita_acumulada']
    por_regime['ticketMedio'] = np.where(
        por_regime['clientes'] > 0,
        por_regime['receita'] / por_regime['clientes'],
        0.0,
    )
    por_regime = por_regime[TICKET_REGIME_COLS]
    por_regime['clientes'] = por_regime['clientes'].astype(int)

    por_reg_ativ = (
        df_clientes.groupby(['regime_base', 'segmento'], dropna=False)['receita']
                   .agg(['sum', 'count']).reset_index()
    )
    por_reg_ativ.columns = ['regime', 'atividade', 'receita_acumulada', 'clientes']
    por_reg_ativ['receita'] = por_reg_ativ['receita_acumulada'] / meses if meses else por_reg_ativ['receita_acumulada']
    por_reg_ativ['ticketMedio'] = np.where(
        por_reg_ativ['clientes'] > 0,
        por_reg_ativ['receita'] / por_reg_ativ['clientes'],
        0.0,
    )
    por_reg_ativ = por_reg_ativ[TICKET_REGIME_ATIV_COLS]
    por_reg_ativ['clientes'] = por_reg_ativ['clientes'].astype(int)

    por_cliente = df_clientes.copy()
    por_cliente['receita_mensal'] = por_cliente['receita'] / meses if meses else por_cliente['receita']
    por_cliente['ticket_mensal'] = por_cliente['receita_mensal']
    por_cliente = por_cliente.rename(columns={
        'cliente': 'cliente',
        'regime_base': 'regime',
        'segmento': 'atividade',
    })
    por_cliente = por_cliente[['cliente', 'regime', 'atividade', 'receita_mensal', 'ticket_mensal']]
    por_cliente = por_cliente.sort_values('ticket_mensal', ascending=False).reset_index(drop=True)
    por_cliente['rank'] = por_cliente.index + 1

    return (
        ticket_geral[TICKET_GERAL_COLS],
        por_regime[TICKET_REGIME_COLS],
        por_reg_ativ[TICKET_REGIME_ATIV_COLS],
        por_cliente[TICKET_CLIENTES_COLS + ['rank']],
    )

# ============================================================================
# ANÁLISE DE MARGEM DE CONTRIBUIÇÃO
# ============================================================================

def analise_margem_contribuicao(
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Calcula margem de contribuição por regime, segmento e cliente.
    Retorna: (df_margem_regime, df_margem_segmento, df_margem_cliente)
    """
    df_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS'].copy()
    if df_core.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    custo_total = df_despesas_rateado['valor'].sum() if not df_despesas_rateado.empty else 0.0
    receita_total = df_core['receita'].sum()

    # ========== MARGEM POR REGIME ==========
    rec_regime = df_core.groupby('regime_base', dropna=False).agg({
        'receita': 'sum',
        'cliente': 'nunique'
    }).reset_index()
    rec_regime.columns = ['regime_base', 'receita_total', 'qtd_clientes']

    rec_regime['receita_mensal'] = rec_regime['receita_total'] / meses
    rec_regime['perc_receita'] = np.where(
        receita_total > 0, rec_regime['receita_total'] / receita_total * 100, 0.0
    )

    # Custo proporcional à receita
    rec_regime['custo_alocado'] = rec_regime['perc_receita'] / 100 * custo_total
    rec_regime['custo_mensal'] = rec_regime['custo_alocado'] / meses

    # Margem de contribuição
    rec_regime['margem_contribuicao'] = rec_regime['receita_total'] - rec_regime['custo_alocado']
    rec_regime['margem_contribuicao_mensal'] = rec_regime['margem_contribuicao'] / meses
    rec_regime['margem_percentual'] = np.where(
        rec_regime['receita_total'] > 0,
        rec_regime['margem_contribuicao'] / rec_regime['receita_total'] * 100,
        0.0
    )

    # Ticket e custo médio
    rec_regime['ticket_medio'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['receita_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    rec_regime['custo_medio'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['custo_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    rec_regime['margem_media_cliente'] = rec_regime['ticket_medio'] - rec_regime['custo_medio']

    df_margem_regime = rec_regime.sort_values('margem_contribuicao', ascending=False).reset_index(drop=True)

    # ========== MARGEM POR SEGMENTO ==========
    rec_seg = df_core.groupby(['regime_base', 'segmento'], dropna=False).agg({
        'receita': 'sum',
        'cliente': 'nunique'
    }).reset_index()
    rec_seg.columns = ['regime_base', 'segmento', 'receita_total', 'qtd_clientes']

    # Custo proporcional dentro do regime
    rec_seg = rec_seg.merge(
        rec_regime[['regime_base', 'custo_alocado']],
        on='regime_base',
        how='left',
        suffixes=('', '_regime')
    )

    receita_por_regime = df_core.groupby('regime_base')['receita'].sum().to_dict()
    rec_seg['prop_no_regime'] = rec_seg.apply(
        lambda r: r['receita_total'] / receita_por_regime.get(r['regime_base'], 1)
        if receita_por_regime.get(r['regime_base'], 0) > 0 else 0.0,
        axis=1
    )
    rec_seg['custo_alocado_segmento'] = rec_seg['prop_no_regime'] * rec_seg['custo_alocado']

    rec_seg['receita_mensal'] = rec_seg['receita_total'] / meses
    rec_seg['custo_mensal'] = rec_seg['custo_alocado_segmento'] / meses
    rec_seg['margem_contribuicao'] = rec_seg['receita_total'] - rec_seg['custo_alocado_segmento']
    rec_seg['margem_contribuicao_mensal'] = rec_seg['margem_contribuicao'] / meses
    rec_seg['margem_percentual'] = np.where(
        rec_seg['receita_total'] > 0,
        rec_seg['margem_contribuicao'] / rec_seg['receita_total'] * 100,
        0.0
    )

    df_margem_segmento = rec_seg[[
        'regime_base', 'segmento', 'qtd_clientes', 'receita_total', 'receita_mensal',
        'custo_alocado_segmento', 'custo_mensal', 'margem_contribuicao',
        'margem_contribuicao_mensal', 'margem_percentual'
    ]].sort_values(['regime_base', 'margem_contribuicao'], ascending=[True, False]).reset_index(drop=True)

    # ========== MARGEM POR CLIENTE ==========
    rec_cliente = df_core.groupby(['cliente', 'regime_base', 'segmento'], dropna=False).agg({
        'receita': 'sum'
    }).reset_index()

    rec_cliente['receita_mensal'] = rec_cliente['receita'] / meses

    rec_cliente = rec_cliente.merge(
        rec_regime[['regime_base', 'custo_alocado', 'receita_total']],
        on='regime_base',
        how='left',
        suffixes=('', '_regime')
    )

    rec_cliente['prop_receita'] = np.where(
        rec_cliente['receita_total'] > 0,
        rec_cliente['receita'] / rec_cliente['receita_total'],
        0.0
    )
    rec_cliente['custo_alocado_cliente'] = rec_cliente['prop_receita'] * rec_cliente['custo_alocado']
    rec_cliente['custo_mensal'] = rec_cliente['custo_alocado_cliente'] / meses

    rec_cliente['margem_contribuicao'] = rec_cliente['receita'] - rec_cliente['custo_alocado_cliente']
    rec_cliente['margem_contribuicao_mensal'] = rec_cliente['margem_contribuicao'] / meses
    rec_cliente['margem_percentual'] = np.where(
        rec_cliente['receita'] > 0,
        rec_cliente['margem_contribuicao'] / rec_cliente['receita'] * 100,
        0.0
    )

    df_margem_cliente = rec_cliente[[
        'cliente', 'regime_base', 'segmento', 'receita', 'receita_mensal',
        'custo_alocado_cliente', 'custo_mensal', 'margem_contribuicao',
        'margem_contribuicao_mensal', 'margem_percentual'
    ]].sort_values('margem_contribuicao', ascending=False).reset_index(drop=True)

    return df_margem_regime, df_margem_segmento, df_margem_cliente


def analise_ponto_equilibrio(df_margem_regime: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ponto de equilíbrio por regime (quantos clientes precisaria para cobrir custos).
    """
    if df_margem_regime.empty:
        return pd.DataFrame()

    df_pe = df_margem_regime.copy()

    df_pe['clientes_break_even'] = np.where(
        df_pe['ticket_medio'] > 0,
        np.ceil(df_pe['custo_mensal'] / df_pe['ticket_medio']),
        np.inf
    )

    df_pe['clientes_faltantes'] = df_pe['clientes_break_even'] - df_pe['qtd_clientes']
    df_pe['ja_esta_equilibrado'] = df_pe['clientes_faltantes'] <= 0

    return df_pe[[
        'regime_base', 'qtd_clientes', 'ticket_medio', 'custo_medio',
        'margem_media_cliente', 'clientes_break_even', 'clientes_faltantes',
        'ja_esta_equilibrado'
    ]]

# ============================================================================
# ANÁLISE DE RENTABILIDADE
# ============================================================================

def analise_rentabilidade(
    df_margem_regime: pd.DataFrame,
    df_margem_cliente: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Gera análises de ROI, índice de eficiência e ranking de rentabilidade.
    Retorna: (df_roi_regime, df_clientes_deficitarios, df_ranking_rentabilidade)
    """

    # ========== ROI E EFICIÊNCIA POR REGIME ==========
    df_roi = df_margem_regime.copy()

    if df_roi.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_roi['roi_percentual'] = np.where(
        df_roi['custo_alocado'] > 0,
        (df_roi['margem_contribuicao'] / df_roi['custo_alocado']) * 100,
        0.0
    )

    df_roi['indice_eficiencia'] = np.where(
        df_roi['custo_alocado'] > 0,
        df_roi['receita_total'] / df_roi['custo_alocado'],
        0.0
    )

    df_roi['classificacao_rentabilidade'] = pd.cut(
        df_roi['margem_percentual'],
        bins=[-np.inf, 0, 10, 25, 50, np.inf],
        labels=['DEFICITÁRIO', 'BAIXA', 'MÉDIA', 'BOA', 'EXCELENTE']
    )

    df_roi_final = df_roi[[
        'regime_base', 'qtd_clientes', 'receita_total', 'custo_alocado',
        'margem_contribuicao', 'margem_percentual', 'roi_percentual',
        'indice_eficiencia', 'classificacao_rentabilidade'
    ]].sort_values('roi_percentual', ascending=False).reset_index(drop=True)

    # ========== CLIENTES DEFICITÁRIOS ==========
    df_deficitarios = df_margem_cliente[df_margem_cliente['margem_contribuicao'] < 0].copy()
    df_deficitarios['prejuizo_mensal'] = -df_deficitarios['margem_contribuicao_mensal']
    df_deficitarios = df_deficitarios.sort_values('prejuizo_mensal', ascending=False).reset_index(drop=True)

    # ========== RANKING DE RENTABILIDADE ==========
    df_ranking = df_margem_cliente.sort_values('margem_contribuicao', ascending=False).reset_index(drop=True).copy()
    df_ranking['rank_geral'] = range(1, len(df_ranking) + 1)

    top_10_melhores = df_ranking.head(10).copy()
    top_10_melhores['classificacao'] = 'TOP 10 MELHORES'

    top_10_piores = df_ranking.tail(10).sort_values('margem_contribuicao', ascending=True).copy()
    top_10_piores['classificacao'] = 'TOP 10 PIORES'
    if not top_10_piores.empty:
        start_rank = len(df_ranking) - len(top_10_piores) + 1
        top_10_piores['rank_geral'] = range(start_rank, len(df_ranking) + 1)

    df_ranking_final = pd.concat([top_10_melhores, top_10_piores], ignore_index=True)

    return df_roi_final, df_deficitarios, df_ranking_final

# ============================================================================
# ANÁLISE AMPLIADA DE RETIRADA DOS SÓCIOS
# ============================================================================

def analise_retirada_socios_completa(
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    total_retiradas: float,
    df_margem_regime: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Análise completa de retiradas: % sobre receita, comparativo por regime, sustentabilidade.
    Retorna: (df_resumo_retirada, df_comparativo_regime)
    """
    receita_total = df_receitas['receita'].sum()
    receita_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS']['receita'].sum()
    despesas_totais = df_despesas_rateado['valor'].sum()
    resultado_liquido = receita_total - despesas_totais

    perc_sobre_receita_bruta = (total_retiradas / receita_total * 100) if receita_total > 0 else 0.0
    perc_sobre_receita_core = (total_retiradas / receita_core * 100) if receita_core > 0 else 0.0
    perc_sobre_resultado = (total_retiradas / resultado_liquido * 100) if resultado_liquido != 0 else np.nan

    despesas_mensais = despesas_totais / meses if meses > 0 else despesas_totais
    meses_sustentabilidade = (total_retiradas / despesas_mensais) if despesas_mensais > 0 else 0.0

    df_resumo = pd.DataFrame([{
        'receita_total': receita_total,
        'receita_core_honorarios': receita_core,
        'despesas_totais': despesas_totais,
        'resultado_liquido': resultado_liquido,
        'retiradas_prolabore_socios': total_retiradas,
        'perc_retirada_sobre_receita_bruta': perc_sobre_receita_bruta,
        'perc_retirada_sobre_receita_core': perc_sobre_receita_core,
        'perc_retirada_sobre_resultado_liquido': perc_sobre_resultado,
        'meses_sustentabilidade_retirada': meses_sustentabilidade,
    }])

    if not df_margem_regime.empty:
        df_comp = df_margem_regime.copy()

        margem_positiva_total = df_comp[df_comp['margem_contribuicao'] > 0]['margem_contribuicao'].sum()

        df_comp['retirada_proporcional'] = np.where(
            df_comp['margem_contribuicao'] > 0,
            (df_comp['margem_contribuicao'] / margem_positiva_total * total_retiradas)
            if margem_positiva_total > 0 else 0.0,
            0.0
        )

        df_comp['lucro_liquido_pos_retirada'] = df_comp['margem_contribuicao'] - df_comp['retirada_proporcional']
        df_comp['perc_retirada_sobre_margem'] = np.where(
            df_comp['margem_contribuicao'] > 0,
            df_comp['retirada_proporcional'] / df_comp['margem_contribuicao'] * 100,
            0.0
        )

        df_comp['contribui_para_lucro'] = df_comp['lucro_liquido_pos_retirada'] > 0

        df_comparativo = df_comp[[
            'regime_base', 'margem_contribuicao', 'retirada_proporcional',
            'lucro_liquido_pos_retirada', 'perc_retirada_sobre_margem', 'contribui_para_lucro'
        ]].sort_values('lucro_liquido_pos_retirada', ascending=False).reset_index(drop=True)
    else:
        df_comparativo = pd.DataFrame()

    return df_resumo, df_comparativo

# ============================================================================
# ANÁLISE DE ABSORÇÃO DE CUSTOS
# ============================================================================

def analise_absorcao_custos(df_margem_regime: pd.DataFrame) -> pd.DataFrame:
    """
    Índice de absorção de custos: identifica regimes subsidiados.
    """
    if df_margem_regime.empty:
        return pd.DataFrame()

    df_abs = df_margem_regime.copy()

    df_abs['indice_absorcao_percentual'] = np.where(
        df_abs['receita_total'] > 0,
        df_abs['custo_alocado'] / df_abs['receita_total'] * 100,
        0.0
    )

    df_abs['classificacao_absorcao'] = pd.cut(
        df_abs['indice_absorcao_percentual'],
        bins=[0, 50, 75, 90, 100, np.inf],
        labels=['EFICIENTE (<50%)', 'BOM (50-75%)', 'MODERADO (75-90%)', 'ALTO (90-100%)', 'SUBSIDIADO (>100%)']
    )

    df_abs['eh_subsidiado'] = df_abs['indice_absorcao_percentual'] > 100

    df_abs['deficit_a_cobrir'] = np.where(
        df_abs['eh_subsidiado'],
        df_abs['custo_alocado'] - df_abs['receita_total'],
        0.0
    )

    return df_abs[[
        'regime_base', 'receita_total', 'custo_alocado', 'margem_contribuicao',
        'indice_absorcao_percentual', 'classificacao_absorcao', 'eh_subsidiado',
        'deficit_a_cobrir'
    ]].sort_values('indice_absorcao_percentual', ascending=False).reset_index(drop=True)

# ============================================================================
# ANÁLISE DE CONCENTRAÇÃO E RISCO
# ============================================================================

def analise_concentracao_risco(
    df_margem_cliente: pd.DataFrame,
    df_receitas: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Curva ABC, índice de concentração e análise de risco de perda.
    Retorna: (df_curva_abc, df_concentracao, df_diversificacao_regime)
    """
    df_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS'].copy()
    receita_total = df_core['receita'].sum()

    df_abc = df_margem_cliente[['cliente', 'receita', 'regime_base']].copy()
    if df_abc.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_abc = df_abc.sort_values('receita', ascending=False).reset_index(drop=True)
    df_abc['rank'] = range(1, len(df_abc) + 1)
    df_abc['receita_acumulada'] = df_abc['receita'].cumsum()

    if receita_total > 0:
        df_abc['perc_receita_individual'] = df_abc['receita'] / receita_total * 100
        df_abc['perc_acumulado'] = df_abc['receita_acumulada'] / receita_total * 100
    else:
        df_abc['perc_receita_individual'] = 0.0
        df_abc['perc_acumulado'] = 0.0

    df_abc['classe_abc'] = pd.cut(
        df_abc['perc_acumulado'],
        bins=[0, 80, 95, 100],
        labels=['A (até 80%)', 'B (80-95%)', 'C (95-100%)'],
        include_lowest=True
    )

    top_3 = df_abc.head(3)['receita'].sum()
    top_5 = df_abc.head(5)['receita'].sum()
    top_10 = df_abc.head(10)['receita'].sum()

    df_concentracao = pd.DataFrame([{
        'top_3_clientes_receita': top_3,
        'top_3_perc_receita_total': (top_3 / receita_total * 100) if receita_total > 0 else 0.0,
        'top_5_clientes_receita': top_5,
        'top_5_perc_receita_total': (top_5 / receita_total * 100) if receita_total > 0 else 0.0,
        'top_10_clientes_receita': top_10,
        'top_10_perc_receita_total': (top_10 / receita_total * 100) if receita_total > 0 else 0.0,
        'risco_concentracao': 'ALTO' if receita_total > 0 and (top_3 / receita_total * 100) > 50
        else ('MÉDIO' if receita_total > 0 and (top_3 / receita_total * 100) > 30 else 'BAIXO')
    }])

    diversif = df_core.groupby('regime_base', dropna=False)['receita'].sum().reset_index()
    if receita_total > 0:
        diversif['perc_receita_total'] = diversif['receita'] / receita_total * 100
    else:
        diversif['perc_receita_total'] = 0.0
    diversif['indice_dependencia'] = pd.cut(
        diversif['perc_receita_total'],
        bins=[0, 20, 40, 60, 100],
        labels=['BAIXA', 'MÉDIA', 'ALTA', 'CRÍTICA'],
        include_lowest=True
    )
    diversif = diversif.sort_values('perc_receita_total', ascending=False).reset_index(drop=True)

    return df_abc, df_concentracao, diversif

# ============================================================================
# ANÁLISE DE EFICIÊNCIA OPERACIONAL
# ============================================================================

def analise_eficiencia_operacional(
    df_despesas_rateado: pd.DataFrame,
    df_receitas: pd.DataFrame,
    df_margem_regime: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Análise de custo por departamento e ticket médio ponderado por complexidade.
    Retorna: (df_custo_departamento, df_ticket_ponderado)
    """

    mask_trab = df_despesas_rateado['grupo'] == 'DESPESAS TRABALHISTAS'
    df_trab = df_despesas_rateado[mask_trab & (df_despesas_rateado['departamento_final'] != "")].copy()

    if not df_trab.empty:
        df_dept = df_trab.groupby('departamento_final')['valor'].sum().reset_index()
        df_dept.columns = ['departamento', 'custo_total']
        df_dept['custo_mensal'] = df_dept['custo_total'] / meses

        total_trab = df_dept['custo_total'].sum()
        df_dept['perc_custo_trabalhista'] = np.where(total_trab > 0, df_dept['custo_total'] / total_trab * 100, 0.0)

        receita_total = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS']['receita'].sum()
        df_dept['receita_por_real_gasto'] = np.where(
            df_dept['custo_total'] > 0,
            receita_total / df_dept['custo_total'],
            np.nan
        )

        df_dept = df_dept.sort_values('custo_total', ascending=False).reset_index(drop=True)
    else:
        df_dept = pd.DataFrame()

    if not df_margem_regime.empty:
        df_pond = df_margem_regime.copy()

        df_pond['peso_complexidade'] = df_pond['regime_base'].map(PESOS_COMPLEXIDADE_REGIME).fillna(1.0)

        df_pond['ticket_ponderado'] = df_pond['ticket_medio'] * df_pond['peso_complexidade']
        df_pond['contribuicao_ponderada'] = df_pond['qtd_clientes'] * df_pond['ticket_ponderado']

        total_contrib_pond = df_pond['contribuicao_ponderada'].sum()
        df_pond['perc_contribuicao_ponderada'] = np.where(
            total_contrib_pond > 0,
            df_pond['contribuicao_ponderada'] / total_contrib_pond * 100,
            0.0
        )

        df_ticket_pond = df_pond[[
            'regime_base', 'qtd_clientes', 'peso_complexidade', 'ticket_medio',
            'ticket_ponderado', 'contribuicao_ponderada', 'perc_contribuicao_ponderada'
        ]].sort_values('ticket_ponderado', ascending=False).reset_index(drop=True)
    else:
        df_ticket_pond = pd.DataFrame()

    return df_dept, df_ticket_pond

# ============================================================================
# ANÁLISE DE CENÁRIOS
# ============================================================================

def analise_cenarios(
    df_margem_regime: pd.DataFrame,
    df_margem_cliente: pd.DataFrame,
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    total_retiradas: float
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Simula cenários: otimista, pessimista, reajuste, ideal para retirada.
    Retorna: (df_otimista, df_pessimista, df_reajuste, df_ideal_retirada)
    """

    receita_atual = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS']['receita'].sum()
    custo_atual = df_despesas_rateado['valor'].sum()
    resultado_atual = receita_atual - custo_atual

    df_otim = df_margem_regime[df_margem_regime['margem_contribuicao'] > 0].copy()
    if not df_otim.empty:
        df_otim['qtd_clientes_novo'] = np.ceil(df_otim['qtd_clientes'] * 1.2).astype(int)
        df_otim['clientes_adicionais'] = df_otim['qtd_clientes_novo'] - df_otim['qtd_clientes']
        df_otim['receita_adicional'] = df_otim['clientes_adicionais'] * df_otim['ticket_medio']

        df_otim['custo_adicional'] = df_otim['receita_adicional'] * 0.5
        df_otim['margem_adicional'] = df_otim['receita_adicional'] - df_otim['custo_adicional']

        df_otim['receita_total_novo'] = df_otim['receita_total'] + df_otim['receita_adicional']
        df_otim['custo_total_novo'] = df_otim['custo_alocado'] + df_otim['custo_adicional']
        df_otim['margem_total_novo'] = df_otim['receita_total_novo'] - df_otim['custo_total_novo']

        receita_otim_total = df_otim['receita_total_novo'].sum()
        custo_otim_total = df_otim['custo_total_novo'].sum()
        resultado_otim = receita_otim_total - custo_otim_total
    else:
        receita_otim_total = receita_atual
        custo_otim_total = custo_atual
        resultado_otim = resultado_atual

    df_cenario_otim = pd.DataFrame([{
        'cenario': 'OTIMISTA (+20% clientes rentáveis)',
        'receita_atual': receita_atual,
        'receita_novo': receita_otim_total,
        'receita_adicional': receita_otim_total - receita_atual,
        'custo_atual': custo_atual,
        'custo_novo': custo_otim_total,
        'custo_adicional': custo_otim_total - custo_atual,
        'resultado_atual': resultado_atual,
        'resultado_novo': resultado_otim,
        'resultado_adicional': resultado_otim - resultado_atual,
        'margem_percentual_atual': (resultado_atual / receita_atual * 100) if receita_atual > 0 else 0.0,
        'margem_percentual_novo': (resultado_otim / receita_otim_total * 100) if receita_otim_total > 0 else 0.0,
        'retirada_atual': total_retiradas,
        'perc_retirada_resultado_novo': (total_retiradas / resultado_otim * 100) if resultado_otim > 0 else np.nan
    }])

    top_3_clientes = df_margem_cliente.head(3)
    receita_perdida = top_3_clientes['receita'].sum()
    custo_economizado = top_3_clientes['custo_alocado_cliente'].sum()

    receita_pess = receita_atual - receita_perdida
    custo_pess = custo_atual - custo_economizado
    resultado_pess = receita_pess - custo_pess

    df_cenario_pess = pd.DataFrame([{
        'cenario': 'PESSIMISTA (Perda TOP 3 clientes)',
        'receita_atual': receita_atual,
        'receita_novo': receita_pess,
        'receita_perdida': receita_perdida,
        'custo_atual': custo_atual,
        'custo_novo': custo_pess,
        'custo_economizado': custo_economizado,
        'resultado_atual': resultado_atual,
        'resultado_novo': resultado_pess,
        'resultado_perdido': resultado_atual - resultado_pess,
        'margem_percentual_atual': (resultado_atual / receita_atual * 100) if receita_atual > 0 else 0.0,
        'margem_percentual_novo': (resultado_pess / receita_pess * 100) if receita_pess > 0 else 0.0,
        'retirada_atual': total_retiradas,
        'perc_retirada_resultado_novo': (total_retiradas / resultado_pess * 100) if resultado_pess > 0 else np.nan,
        'viabilidade': 'VIÁVEL' if resultado_pess > total_retiradas else 'INVIÁVEL'
    }])

    receita_reaj = receita_atual * 1.15
    custo_reaj = custo_atual * 1.05
    resultado_reaj = receita_reaj - custo_reaj

    df_cenario_reaj = pd.DataFrame([{
        'cenario': 'REAJUSTE (+15% tickets, +5% custos)',
        'receita_atual': receita_atual,
        'receita_novo': receita_reaj,
        'receita_adicional': receita_reaj - receita_atual,
        'perc_aumento_receita': 15.0,
        'custo_atual': custo_atual,
        'custo_novo': custo_reaj,
        'custo_adicional': custo_reaj - custo_atual,
        'perc_aumento_custo': 5.0,
        'resultado_atual': resultado_atual,
        'resultado_novo': resultado_reaj,
        'resultado_adicional': resultado_reaj - resultado_atual,
        'margem_percentual_atual': (resultado_atual / receita_atual * 100) if receita_atual > 0 else 0.0,
        'margem_percentual_novo': (resultado_reaj / receita_reaj * 100) if receita_reaj > 0 else 0.0,
        'retirada_atual': total_retiradas,
        'perc_retirada_resultado_novo': (total_retiradas / resultado_reaj * 100) if resultado_reaj > 0 else np.nan
    }])

    perc_retirada_ideal = 30.0
    retirada_ideal = resultado_atual * (perc_retirada_ideal / 100)

    if perc_retirada_ideal > 0:
        resultado_minimo_necessario = total_retiradas / (perc_retirada_ideal / 100)
    else:
        resultado_minimo_necessario = resultado_atual
    receita_necessaria = resultado_minimo_necessario + custo_atual
    aumento_receita_necessario = receita_necessaria - receita_atual
    perc_aumento_necessario = (aumento_receita_necessario / receita_atual * 100) if receita_atual > 0 else 0.0

    df_ideal_detalhe = df_margem_regime[df_margem_regime['margem_contribuicao'] > 0].copy()
    total_receita_rentaveis = df_ideal_detalhe['receita_total'].sum()
    if total_receita_rentaveis > 0 and aumento_receita_necessario > 0:
        df_ideal_detalhe['prop_receita'] = df_ideal_detalhe['receita_total'] / total_receita_rentaveis
        df_ideal_detalhe['aumento_receita_regime'] = df_ideal_detalhe['prop_receita'] * aumento_receita_necessario
        df_ideal_detalhe['receita_total_necessaria'] = df_ideal_detalhe['receita_total'] + df_ideal_detalhe['aumento_receita_regime']
        df_ideal_detalhe['aumento_percentual_regime'] = np.where(
            df_ideal_detalhe['receita_total'] > 0,
            df_ideal_detalhe['aumento_receita_regime'] / df_ideal_detalhe['receita_total'] * 100,
            0.0
        )
        df_ideal_detalhe['novos_clientes_necessarios'] = np.where(
            df_ideal_detalhe['ticket_medio'] > 0,
            np.ceil(df_ideal_detalhe['aumento_receita_regime'] / df_ideal_detalhe['ticket_medio']).astype(int),
            0
        )
    else:
        df_ideal_detalhe['prop_receita'] = 0.0
        df_ideal_detalhe['aumento_receita_regime'] = 0.0
        df_ideal_detalhe['receita_total_necessaria'] = df_ideal_detalhe['receita_total']
        df_ideal_detalhe['aumento_percentual_regime'] = 0.0
        df_ideal_detalhe['novos_clientes_necessarios'] = 0

    df_cenario_ideal = pd.DataFrame([{
        'cenario': 'IDEAL (Manter retirada em 30% resultado)',
        'retirada_atual': total_retiradas,
        'perc_retirada_atual_sobre_resultado': (total_retiradas / resultado_atual * 100) if resultado_atual > 0 else np.nan,
        'perc_retirada_ideal': perc_retirada_ideal,
        'retirada_ideal_atual': retirada_ideal,
        'gap_retirada': total_retiradas - retirada_ideal,
        'resultado_minimo_necessario': resultado_minimo_necessario,
        'receita_atual': receita_atual,
        'receita_necessaria': receita_necessaria,
        'aumento_receita_necessario': aumento_receita_necessario,
        'perc_aumento_necessario': perc_aumento_necessario,
        'custo_atual': custo_atual,
        'resultado_novo': resultado_minimo_necessario,
        'margem_percentual_nova': (resultado_minimo_necessario / receita_necessaria * 100) if receita_necessaria > 0 else 0.0
    }])

    df_cenario_ideal_completo = pd.concat([df_cenario_ideal, df_ideal_detalhe], axis=1, sort=False)

    return df_cenario_otim, df_cenario_pess, df_cenario_reaj, df_cenario_ideal_completo

def gera_top_despesas(df_despesas: pd.DataFrame, top_n: int = 10):
    if df_despesas.empty:
        vazio = pd.DataFrame([{
            "rank": 0, "grupo": "", "item_nome": "Sem dados", "valor": 0.0,
            "perc_total_despesas": 0.0, "perc_no_grupo": 0.0
        }])
        return vazio, vazio

    total_desp = df_despesas['valor'].sum()
    base = (
        df_despesas.groupby(['grupo', 'item_nome'], dropna=False)['valor']
                   .sum().reset_index()
    )
    base['perc_total_despesas'] = np.where(total_desp > 0, base['valor'] / total_desp * 100, 0.0)
    total_por_grupo = base.groupby('grupo', dropna=False)['valor'].transform('sum')
    base['perc_no_grupo'] = np.where(total_por_grupo > 0, base['valor'] / total_por_grupo * 100, 0.0)

    base = base.sort_values('valor', ascending=False).reset_index(drop=True)
    base['rank'] = range(1, len(base) + 1)
    top_geral = base.head(top_n).copy()
    top_geral = top_geral[['rank', 'grupo', 'item_nome', 'valor', 'perc_total_despesas', 'perc_no_grupo']]

    admin = base[base['grupo'] == 'DESPESAS ADMINISTRATIVAS'].copy()
    admin = admin.head(top_n).reset_index(drop=True)
    admin['rank'] = range(1, len(admin) + 1)
    top_admin = admin[['rank', 'item_nome', 'valor', 'perc_no_grupo']]
    return top_geral, top_admin

# ============================================================================
# VISÃO ÚNICA DE CUSTO POR ABSORÇÃO (PESO × VOLUME)
# ============================================================================

def _clientes_por_regime(df_core: pd.DataFrame) -> pd.DataFrame:
    return (
        df_core.groupby('regime_base', dropna=False)
               .agg(qtd_clientes=('cliente', 'nunique'))
               .reset_index()
    )

def _clientes_por_regime_segmento(df_core: pd.DataFrame) -> pd.DataFrame:
    return (
        df_core.groupby(['regime_base', 'segmento'], dropna=False)
               .agg(qtd_clientes=('cliente', 'nunique'))
               .reset_index()
    )

def _participacoes(df_core: pd.DataFrame, meses: int):
    """Retorna receitas e participações por regime e por (regime, segmento)."""
    receita_total = df_core['receita'].sum()
    rec_regime = (
        df_core.groupby('regime_base', dropna=False)['receita']
               .sum().reset_index().rename(columns={'receita': 'receita_regime'})
    )
    rec_regime['receita_mensal'] = rec_regime['receita_regime'] / meses
    rec_regime['perc_receita_total'] = np.where(
        receita_total > 0, rec_regime['receita_regime'] / receita_total, 0.0
    )

    rec_seg = (
        df_core.groupby(['regime_base', 'segmento'], dropna=False)['receita']
               .sum().reset_index().rename(columns={'receita': 'receita_segmento'})
    )
    rec_seg = rec_seg.merge(rec_regime[['regime_base', 'receita_regime']], on='regime_base', how='left', validate='many_to_one')
    rec_seg['receita_mensal_segmento'] = rec_seg['receita_segmento'] / meses
    rec_seg['prop_no_regime'] = np.where(
        rec_seg['receita_regime'] > 0, rec_seg['receita_segmento'] / rec_seg['receita_regime'], 0.0
    )

    return receita_total, rec_regime, rec_seg

def _aplica_visao_com_peso(
    df_core: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    meses: int,
    pesos_regime: Optional[Dict[str, float]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Custo por absorção proporcional ao peso do regime (complexidade × volume)."""
    if df_despesas_rateado.empty:
        custos_adm_liquidos = total_trabalhistas = total_tributarias = total_financeiras = total_bancarias = 0.0
    else:
        grupos = df_despesas_rateado.groupby('grupo')['valor'].sum()
        custos_adm_liquidos = float(grupos.get('DESPESAS ADMINISTRATIVAS', 0.0))
        total_trabalhistas = float(grupos.get('DESPESAS TRABALHISTAS', 0.0))
        total_tributarias = float(grupos.get('DESPESAS TRIBUTÁRIAS', 0.0))
        total_financeiras = float(grupos.get('DESPESAS FINANCEIRAS', 0.0))
        total_bancarias = float(grupos.get('DESPESAS BANCÁRIAS', 0.0))

    custos_alocaveis = (
        custos_adm_liquidos
        + total_trabalhistas
        + total_tributarias
        + total_financeiras
        + total_bancarias
    )

    _, rec_regime, rec_seg = _participacoes(df_core, meses)

    if pesos_regime is None:
        pesos_regime = PESOS_COMPLEXIDADE_REGIME

    clientes_regime = _clientes_por_regime(df_core)  # qtd_clientes por regime
    rec_regime = rec_regime.merge(clientes_regime, on='regime_base', how='left')
    rec_regime['qtd_clientes'] = rec_regime['qtd_clientes'].fillna(0).astype(int)

    rec_regime['ticket_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['receita_mensal'] / rec_regime['qtd_clientes'],
        0.0,
    )

    rec_regime['peso_complexidade'] = rec_regime['regime_base'].map(pesos_regime).fillna(1.0)
    rec_regime['peso_total'] = rec_regime['peso_complexidade'] * rec_regime['qtd_clientes'].clip(lower=0)

    soma_pesos_totais = rec_regime['peso_total'].sum()
    rec_regime['participacao_peso'] = np.where(
        soma_pesos_totais > 0,
        rec_regime['peso_total'] / soma_pesos_totais,
        0.0,
    )
    rec_regime['participacao_esforco'] = rec_regime['participacao_peso']

    base_custo_mensal = custos_alocaveis / meses if meses else custos_alocaveis
    rec_regime['custo_mensal'] = rec_regime['participacao_peso'] * base_custo_mensal

    rec_regime['custo_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['custo_mensal'] / rec_regime['qtd_clientes'],
        0.0,
    )
    rec_regime['resultado_mensal'] = rec_regime['receita_mensal'] - rec_regime['custo_mensal']
    rec_regime['resultado_medio_mensal'] = rec_regime['ticket_medio_mensal'] - rec_regime['custo_medio_mensal']

    df_regime = rec_regime[[
        'regime_base',
        'qtd_clientes',
        'peso_complexidade',
        'peso_total',
        'participacao_peso',
        'participacao_esforco',
        'receita_regime',
        'receita_mensal',
        'ticket_medio_mensal',
        'custo_mensal',
        'custo_medio_mensal',
        'resultado_mensal',
        'resultado_medio_mensal',
    ]].copy()
    df_regime = df_regime.sort_values('receita_regime', ascending=False).reset_index(drop=True)

    # por (regime, segmento) — distribuição interna proporcional ao peso do grupo
    rec_seg = rec_seg.merge(
        df_regime[['regime_base', 'custo_mensal', 'peso_complexidade']],
        on='regime_base',
        how='left',
        suffixes=('', '_regime')
    )

    clientes_seg = _clientes_por_regime_segmento(df_core)
    rec_seg = rec_seg.merge(clientes_seg, on=['regime_base', 'segmento'], how='left')
    rec_seg['qtd_clientes'] = rec_seg['qtd_clientes'].fillna(0).astype(int)

    rec_seg['peso_grupo'] = rec_seg['peso_complexidade'] * rec_seg['qtd_clientes']
    soma_pesos_regime = rec_seg.groupby('regime_base')['peso_grupo'].transform(lambda x: x.sum() if x.sum() > 0 else 0.0)
    rec_seg['participacao_no_regime'] = np.where(
        soma_pesos_regime > 0,
        rec_seg['peso_grupo'] / soma_pesos_regime,
        0.0,
    )
    rec_seg['custo_mensal_segmento'] = rec_seg['participacao_no_regime'] * rec_seg['custo_mensal']

    rec_seg['ticket_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['receita_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0,
    )
    rec_seg['custo_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['custo_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0,
    )
    rec_seg['resultado_mensal'] = rec_seg['receita_mensal_segmento'] - rec_seg['custo_mensal_segmento']
    rec_seg['resultado_medio_mensal'] = rec_seg['ticket_medio_mensal'] - rec_seg['custo_medio_mensal']

    df_segmento = rec_seg[[
        'regime_base',
        'segmento',
        'qtd_clientes',
        'receita_segmento',
        'receita_mensal_segmento',
        'custo_mensal_segmento',
        'ticket_medio_mensal',
        'custo_medio_mensal',
        'resultado_mensal',
        'resultado_medio_mensal',
    ]].copy()
    df_segmento = df_segmento.sort_values(['regime_base', 'receita_segmento'], ascending=[True, False]).reset_index(drop=True)

    return df_regime, df_segmento

def custo_absorcao_por_peso(
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    pesos_regime: Optional[Dict[str, float]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Gera os DataFrames de resultado por regime e segmento na visão única por peso."""
    meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1
    df_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS'].copy()
    if df_core.empty:
        vazio_reg = _empty_df(RESULTADO_REGIME_FULL_COLS)
        vazio_seg = _empty_df([
            'regime_base',
            'segmento',
            'qtd_clientes',
            'receita_segmento',
            'receita_mensal_segmento',
            'custo_mensal_segmento',
            'ticket_medio_mensal',
            'custo_medio_mensal',
            'resultado_mensal',
            'resultado_medio_mensal',
        ])
        return vazio_reg, vazio_seg

    reg, seg = _aplica_visao_com_peso(df_core, df_despesas_rateado, meses, pesos_regime=pesos_regime)
    return reg, seg

# ============================================================================
# CENÁRIO "SEM PRÓ-LABORE" + % RETIRADA
# ============================================================================

def remover_prolabores(df_despesas: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """Remove apenas PRO-LABORE - MARCO e PRO-LABORE - MATEUS; retorna (df_sem_prolab, total_retiradas)."""
    if df_despesas.empty:
        return df_despesas.copy(), 0.0
    mask = df_despesas['item_nome'].apply(is_prolabore_socio)
    valor_retiradas = df_despesas.loc[mask, 'valor'].sum()
    return df_despesas.loc[~mask].copy(), float(valor_retiradas)

def percentual_retirada_sobre_resultado(
    df_receitas_validas: pd.DataFrame,
    df_desp_sem_prolab: pd.DataFrame,
    total_retiradas: float
) -> Tuple[float, float, float]:
    """Retorna (resultado_liquido_sem_prolab, total_retiradas, percentual)."""
    receitas_totais_validas = df_receitas_validas['receita'].sum()
    despesas_sem_prolab_total = df_desp_sem_prolab['valor'].sum()
    resultado = receitas_totais_validas - despesas_sem_prolab_total
    perc = (total_retiradas / resultado * 100.0) if resultado != 0 else np.nan
    return float(resultado), float(total_retiradas), float(perc)


def gera_dre_simplificada(
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    total_retiradas: float,
    abatimento_adv: float
) -> pd.DataFrame:
    """Gera DRE simplificada com margem líquida."""

    meses = MESES_ACUMULADOS if MESES_ACUMULADOS else 1
    if df_receitas.empty or meses == 0:
        return _empty_df(["linha", "valor_mensal", "perc_receita"])

    receita_core = df_receitas['receita'].sum()
    receita_mensal = receita_core / meses if meses else receita_core

    if df_despesas_rateado.empty:
        custos_alocados = 0.0
    else:
        custos_alocados = df_despesas_rateado['valor'].sum() / meses if meses else df_despesas_rateado['valor'].sum()

    margem_liquida = receita_mensal - custos_alocados
    retirada_mensal = total_retiradas / meses if meses else total_retiradas
    resultado_ajustado = margem_liquida - retirada_mensal

    linhas = [
        {
            'linha': 'Receita (CORE)',
            'valor_mensal': receita_mensal,
            'perc_receita': 1.0,
        },
        {
            'linha': '(–) Custos Alocados',
            'valor_mensal': -custos_alocados,
            'perc_receita': (custos_alocados / receita_mensal) if receita_mensal else 0.0,
        },
        {
            'linha': '= Margem de Contribuição Líquida',
            'valor_mensal': margem_liquida,
            'perc_receita': (margem_liquida / receita_mensal) if receita_mensal else 0.0,
        },
        {
            'linha': '(–) Retiradas - Pró-labore',
            'valor_mensal': -retirada_mensal,
            'perc_receita': (retirada_mensal / receita_mensal) if receita_mensal else 0.0,
        },
        {
            'linha': '= Resultado Líquido Ajustado',
            'valor_mensal': resultado_ajustado,
            'perc_receita': (resultado_ajustado / receita_mensal) if receita_mensal else 0.0,
        },
    ]

    dre = pd.DataFrame(linhas, columns=["linha", "valor_mensal", "perc_receita"])
    return dre

# ============================================================================
# RELATÓRIO COMPLETO - VERSÃO EXPANDIDA
# ============================================================================

def gera_relatorio_completo():
    log: List[str] = []
    try:
        global _SHEETS_INFO_CACHE, _PLANILHA_UNICA_CACHE
        _SHEETS_INFO_CACHE = None
        _PLANILHA_UNICA_CACHE = None

        DIRETORIO_BASE.mkdir(parents=True, exist_ok=True)

        # 1) Receitas (com remoção da receita de rateio-advocacia)
        df_receitas, abatimento_adm = carrega_receitas(ARQUIVO_ENTRADA, log)

        # 2) Despesas (sem abatimento aplicado ainda)
        df_despesas = carrega_despesas(ARQUIVO_ENTRADA, log)

        if df_receitas.empty:
            log.append("✗ ERRO: Não há dados válidos de RECEITAS. Verifique a planilha de origem.")
            return log
        if df_despesas.empty:
            log.append("✗ ERRO: Não há dados válidos de DESPESAS. Verifique a planilha de origem.")
            return log

        # 2a) Aplica abatimento de advocacia nas ADM sem afetar pró-labores
        total_admin_bruto = df_despesas[df_despesas['grupo'] == 'DESPESAS ADMINISTRATIVAS']['valor'].sum()
        df_despesas_abatidas = redistribui_abatimento_admin_sem_afetar_prolabores(
            df_despesas, abatimento_adm, log
        )
        total_admin_pos = df_despesas_abatidas[df_despesas_abatidas['grupo'] == 'DESPESAS ADMINISTRATIVAS']['valor'].sum()
        log.append(
            f"✓ Rateio advocacia abatido: R$ {abs(abatimento_adm):,.2f} | ADM antes: R$ {total_admin_bruto:,.2f} | ADM após: R$ {total_admin_pos:,.2f}"
        )

        # ---------- CENÁRIO GERAL ----------
        # 3) Rateio trabalhista (mantido)
        df_desp_rateado_geral = pd.DataFrame()
        if not df_despesas_abatidas.empty:
            pesos = calcula_base_rateio(df_despesas_abatidas, log)
            df_desp_rateado_geral = aplica_rateio_proporcional(df_despesas_abatidas, pesos, log)

        # 4) Tickets e Top despesas (geral)
        df_ticket_geral = _empty_df(TICKET_GERAL_COLS)
        df_ticket_regime = _empty_df(TICKET_REGIME_COLS)
        df_ticket_reg_ativ = _empty_df(TICKET_REGIME_ATIV_COLS)
        df_ticket_clientes = _empty_df(TICKET_CLIENTES_COLS + ['rank'])
        if not df_receitas.empty:
            df_ticket_geral, df_ticket_regime, df_ticket_reg_ativ, df_ticket_clientes = gera_ticket_medio(df_receitas)

        df_top_desp, df_top_desp_admin = gera_top_despesas(df_despesas_abatidas, TOP_N_DESPESAS) if not df_despesas_abatidas.empty else (pd.DataFrame(), pd.DataFrame())

        # 5) Visão de custo por absorção e resultados (peso único)
        (df_regime_peso,
         df_seg_peso) = custo_absorcao_por_peso(df_receitas, df_desp_rateado_geral, pesos_regime=PESOS_COMPLEXIDADE_REGIME) if not df_receitas.empty else (
            _empty_df(RESULTADO_REGIME_FULL_COLS),
            _empty_df([
                'regime_base',
                'segmento',
                'qtd_clientes',
                'receita_segmento',
                'receita_mensal_segmento',
                'custo_mensal_segmento',
                'ticket_medio_mensal',
                'custo_medio_mensal',
                'resultado_mensal',
                'resultado_medio_mensal',
            ])
        )

        # ---------- CENÁRIO SEM PRÓ-LABORE ----------
        df_desp_sem_prolab, total_retiradas = remover_prolabores(df_despesas_abatidas)
        df_desp_rateado_sem_prolab = pd.DataFrame()
        if not df_desp_sem_prolab.empty:
            pesos2 = calcula_base_rateio(df_desp_sem_prolab, log)
            df_desp_rateado_sem_prolab = aplica_rateio_proporcional(df_desp_sem_prolab, pesos2, log)

        # Visão única (sem pró-labore)
        (df_regime_peso__noPL,
         df_seg_peso__noPL) = custo_absorcao_por_peso(df_receitas, df_desp_rateado_sem_prolab, pesos_regime=PESOS_COMPLEXIDADE_REGIME) if not df_receitas.empty else (pd.DataFrame(), pd.DataFrame())

        # % retirada sobre resultado líquido (receitas válidas - despesas sem pró-labore)
        resultado_liq_sem_pl, total_retiradas_val, perc_retirada = percentual_retirada_sobre_resultado(
            df_receitas_validas=df_receitas,
            df_desp_sem_prolab=df_desp_sem_prolab,
            total_retiradas=total_retiradas
        )

        receita_total = df_receitas['receita'].sum()
        resumo_retirada = pd.DataFrame([{
            "receitas_validas": receita_total,
            "despesas_sem_prolabore": df_desp_sem_prolab['valor'].sum(),
            "resultado_liquido_sem_prolabore": resultado_liq_sem_pl,
            "retiradas_prolabore_socios": total_retiradas_val,
            "percentual_retirada_sobre_resultado_%": perc_retirada,
            "retiradas_total": total_retiradas_val,
            "perc_retiradas_sobre_receita": (total_retiradas_val / receita_total * 100) if receita_total else 0.0,
            "perc_retiradas_sobre_resultado_liquido_pre_retirada": perc_retirada,
            "abatimento_rateio_advocacia_aplicado": -abs(abatimento_adm),
            "ALFA_VOLUME": ALFA_VOLUME,
            "BETA_TICKET": BETA_TICKET
        }])

        df_dre = gera_dre_simplificada(df_receitas, df_desp_rateado_geral, total_retiradas_val, abatimento_adm)

        # ========== NOVAS ANÁLISES ==========
        log.append("\n" + "="*80)
        log.append("GERANDO NOVAS ANÁLISES")
        log.append("="*80)

        meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1

        # 6) MARGEM DE CONTRIBUIÇÃO
        log.append("\n→ Calculando Margem de Contribuição...")
        df_margem_regime, df_margem_segmento, df_margem_cliente = analise_margem_contribuicao(
            df_receitas, df_desp_rateado_geral, meses
        )

        df_ponto_equilibrio = analise_ponto_equilibrio(df_margem_regime) if not df_margem_regime.empty else pd.DataFrame()
        log.append(f"  ✓ Margem por Regime: {len(df_margem_regime)} registros")
        log.append(f"  ✓ Margem por Segmento: {len(df_margem_segmento)} registros")
        log.append(f"  ✓ Margem por Cliente: {len(df_margem_cliente)} registros")

        # 7) RENTABILIDADE
        log.append("\n→ Calculando Análise de Rentabilidade...")
        df_roi_regime, df_clientes_deficitarios, df_ranking_rentabilidade = analise_rentabilidade(
            df_margem_regime, df_margem_cliente
        )
        log.append(f"  ✓ ROI por Regime: {len(df_roi_regime)} registros")
        log.append(f"  ✓ Clientes Deficitários: {len(df_clientes_deficitarios)} registros")
        log.append(f"  ✓ Ranking Rentabilidade: {len(df_ranking_rentabilidade)} registros")

        # 8) RETIRADA SÓCIOS COMPLETA
        log.append("\n→ Calculando Análise Completa de Retirada dos Sócios...")
        df_resumo_retirada_completo, df_comparativo_retirada_regime = analise_retirada_socios_completa(
            df_receitas, df_desp_rateado_geral, total_retiradas, df_margem_regime, meses
        )
        log.append(f"  ✓ Resumo Retirada: {len(df_resumo_retirada_completo)} registros")
        log.append(f"  ✓ Comparativo por Regime: {len(df_comparativo_retirada_regime)} registros")

        # 9) ABSORÇÃO DE CUSTOS
        log.append("\n→ Calculando Análise de Absorção de Custos...")
        df_absorcao = analise_absorcao_custos(df_margem_regime)
        log.append(f"  ✓ Absorção de Custos: {len(df_absorcao)} registros")

        # 10) CONCENTRAÇÃO E RISCO
        log.append("\n→ Calculando Análise de Concentração e Risco...")
        df_curva_abc, df_concentracao, df_diversificacao = analise_concentracao_risco(
            df_margem_cliente, df_receitas
        )
        log.append(f"  ✓ Curva ABC: {len(df_curva_abc)} registros")
        log.append(f"  ✓ Índice Concentração: {len(df_concentracao)} registros")
        log.append(f"  ✓ Diversificação Regime: {len(df_diversificacao)} registros")

        # 11) EFICIÊNCIA OPERACIONAL
        log.append("\n→ Calculando Análise de Eficiência Operacional...")
        df_custo_dept, df_ticket_ponderado = analise_eficiencia_operacional(
            df_desp_rateado_geral, df_receitas, df_margem_regime, meses
        )
        log.append(f"  ✓ Custo por Departamento: {len(df_custo_dept)} registros")
        log.append(f"  ✓ Ticket Ponderado: {len(df_ticket_ponderado)} registros")

        # 12) CENÁRIOS
        log.append("\n→ Calculando Análise de Cenários...")
        df_cenario_otim, df_cenario_pess, df_cenario_reaj, df_cenario_ideal = analise_cenarios(
            df_margem_regime, df_margem_cliente, df_receitas, df_desp_rateado_geral, total_retiradas
        )
        log.append(f"  ✓ Cenário Otimista: {len(df_cenario_otim)} registros")
        log.append(f"  ✓ Cenário Pessimista: {len(df_cenario_pess)} registros")
        log.append(f"  ✓ Cenário Reajuste: {len(df_cenario_reaj)} registros")
        log.append(f"  ✓ Cenário Ideal: {len(df_cenario_ideal)} registros")

        # ---------- GRAVAÇÃO ----------
        log.append("\n" + "="*80)
        log.append("GRAVANDO ARQUIVO EXCEL")
        log.append("="*80)

        checks_validacao = valida_resultado(df_despesas_abatidas, abatimento_adm, df_regime_peso)
        if checks_validacao:
            log.append("\nVALIDAÇÕES DO MODELO")
            for chk in checks_validacao:
                log.append(f"  • {chk}")

        df_regime_peso_excel = _prepare_regime_excel(df_regime_peso)
        df_seg_peso_excel = _prepare_segmento_excel(df_seg_peso)
        df_regime_peso_no_pl_excel = _prepare_regime_excel(df_regime_peso__noPL)
        df_seg_peso_no_pl_excel = _prepare_segmento_excel(df_seg_peso__noPL)

        total_abas = 0
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl') as writer:
            # Bases originais
            if not df_receitas.empty:
                _safe_to_excel(df_receitas, writer, 'Receitas_Base')
            if not df_despesas.empty:
                _safe_to_excel(df_despesas, writer, 'Despesas_Base_Original')
            if not df_despesas_abatidas.empty:
                _safe_to_excel(df_despesas_abatidas, writer, 'Despesas_Base_Abatida')
            if not df_desp_rateado_geral.empty:
                _safe_to_excel(df_desp_rateado_geral, writer, 'Despesas_Rateadas_Geral')
            if not df_desp_sem_prolab.empty:
                _safe_to_excel(df_desp_sem_prolab, writer, 'Despesas_Sem_Prolabore')
            if not df_desp_rateado_sem_prolab.empty:
                _safe_to_excel(df_desp_rateado_sem_prolab, writer, 'Despesas_Rateadas_Sem_PL')

            # Tickets / Clientes
            _safe_to_excel(df_ticket_geral, writer, 'Ticket_Geral')
            _safe_to_excel(df_ticket_regime, writer, 'Ticket_Regime')
            _safe_to_excel(df_ticket_reg_ativ, writer, 'Ticket_Regime_Atividade')
            _safe_to_excel(df_ticket_clientes, writer, 'Ticket_Clientes')

            # Top despesas (geral)
            if not df_top_desp.empty:
                _safe_to_excel(df_top_desp, writer, 'Top_Despesas')
            if not df_top_desp_admin.empty:
                _safe_to_excel(df_top_desp_admin, writer, 'Top_Despesas_Adm')

            # Resultado por regime/segmento - CENÁRIO GERAL
            _safe_to_excel(df_regime_peso_excel, writer, 'Resultado_Regime_Com_Peso')
            _safe_to_excel(df_seg_peso_excel, writer, 'Resultado_Segmento_Com_Peso')

            # Resultado por regime/segmento - CENÁRIO SEM PRÓ-LABORE
            _safe_to_excel(df_regime_peso_no_pl_excel, writer, 'Res_Regime_Com_Peso_Sem_PL')
            _safe_to_excel(df_seg_peso_no_pl_excel, writer, 'Res_Segmento_Com_Peso_Sem_PL')

            # Resumo de retirada (original)
            _safe_to_excel(resumo_retirada, writer, 'Resumo_Retirada')
            _safe_to_excel(df_dre, writer, 'DRE_Simplificada')

            # ========== NOVAS ABAS ==========

            # MARGEM DE CONTRIBUIÇÃO
            if not df_margem_regime.empty:
                _safe_to_excel(df_margem_regime, writer, 'Margem_Contribuicao_Regime')
            if not df_margem_segmento.empty:
                _safe_to_excel(df_margem_segmento, writer, 'Margem_Contribuicao_Segmento')
            if not df_margem_cliente.empty:
                _safe_to_excel(df_margem_cliente, writer, 'Margem_Contribuicao_Cliente')
            if not df_ponto_equilibrio.empty:
                _safe_to_excel(df_ponto_equilibrio, writer, 'Ponto_Equilibrio_Regime')

            # RENTABILIDADE
            if not df_roi_regime.empty:
                _safe_to_excel(df_roi_regime, writer, 'ROI_Rentabilidade_Regime')
            if not df_clientes_deficitarios.empty:
                _safe_to_excel(df_clientes_deficitarios, writer, 'Clientes_Deficitarios')
            if not df_ranking_rentabilidade.empty:
                _safe_to_excel(df_ranking_rentabilidade, writer, 'Ranking_Rentabilidade')

            # RETIRADA SÓCIOS
            if not df_resumo_retirada_completo.empty:
                _safe_to_excel(df_resumo_retirada_completo, writer, 'Retirada_Completa')
            if not df_comparativo_retirada_regime.empty:
                _safe_to_excel(df_comparativo_retirada_regime, writer, 'Retirada_Por_Regime')

            # ABSORÇÃO
            if not df_absorcao.empty:
                _safe_to_excel(df_absorcao, writer, 'Absorcao_Custos_Regime')

            # CONCENTRAÇÃO E RISCO
            if not df_curva_abc.empty:
                _safe_to_excel(df_curva_abc, writer, 'Curva_ABC_Clientes')
            if not df_concentracao.empty:
                _safe_to_excel(df_concentracao, writer, 'Indice_Concentracao')
            if not df_diversificacao.empty:
                _safe_to_excel(df_diversificacao, writer, 'Diversificacao_Regime')

            # EFICIÊNCIA
            if not df_custo_dept.empty:
                _safe_to_excel(df_custo_dept, writer, 'Custo_Por_Departamento')
            if not df_ticket_ponderado.empty:
                _safe_to_excel(df_ticket_ponderado, writer, 'Ticket_Ponderado_Complexidade')

            # CENÁRIOS
            if not df_cenario_otim.empty:
                _safe_to_excel(df_cenario_otim, writer, 'Cenario_Otimista')
            if not df_cenario_pess.empty:
                _safe_to_excel(df_cenario_pess, writer, 'Cenario_Pessimista')
            if not df_cenario_reaj.empty:
                _safe_to_excel(df_cenario_reaj, writer, 'Cenario_Reajuste')
            if not df_cenario_ideal.empty:
                _safe_to_excel(df_cenario_ideal, writer, 'Cenario_Ideal_Retirada')

            # Log como aba
            _safe_to_excel(pd.DataFrame({"log": log}), writer, 'Log')

            try:
                total_abas = len(writer.book.sheetnames)
            except Exception:
                total_abas = 0

        # Log em arquivo texto
        try:
            with open(ARQUIVO_LOG, "w", encoding="utf-8") as f:
                f.write("\n".join(_sanitize_excel_text(l) for l in log))
        except Exception as e:
            log.append(f"⚠ AVISO: Falha ao gravar arquivo de log: {e}")

        log.append("\n✓ Relatório gerado com sucesso:")
        log.append(f"  • Arquivo: {ARQUIVO_SAIDA}")
        if total_abas:
            log.append(f"  • Total de abas: {total_abas}")
        else:
            log.append("  • Total de abas: não disponível")
        return log

    except Exception as e:
        import traceback
        log.append(f"✗ ERRO FATAL: {e}")
        log.append(traceback.format_exc())
        return log

# ============================================================================
# MAIN
# ============================================================================

def valida_resultado(
    df_despesas_abatidas: pd.DataFrame,
    abatimento_adm: float,
    df_regime_peso: pd.DataFrame,
) -> List[str]:
    checks: List[str] = []

    if df_despesas_abatidas is not None and not df_despesas_abatidas.empty:
        prolabores = df_despesas_abatidas[
            df_despesas_abatidas['item_nome'].apply(is_prolabore_socio)
        ]['valor'].sum()
        checks.append(f"Pró-labores identificados: R$ {prolabores:,.2f}")
    else:
        checks.append("Pró-labores identificados: R$ 0,00")

    checks.append(f"Abatimento advocacia: R$ {abatimento_adm:,.2f}")

    if df_regime_peso is not None and not df_regime_peso.empty:
        soma_participacoes = df_regime_peso['participacao_esforco'].sum()
        checks.append(f"Soma participações: {soma_participacoes:.4f} (deve ser ~1.0)")

        for _, row in df_regime_peso.iterrows():
            diff = abs(row['resultado_mensal'] - (row['receita_mensal'] - row['custo_mensal']))
            if diff > 0.01:
                checks.append(f"⚠️ Divergência em {row['regime_base']}: R$ {diff:,.2f}")
    else:
        checks.append("Soma participações: 0.0000 (deve ser ~1.0)")

    return checks


if __name__ == "__main__":
    logs = gera_relatorio_completo()
    print("\n".join(logs))
