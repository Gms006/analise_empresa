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
    "PF": 0.0,
    "Imune/Isenta": 1.2,
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

CLASSIFICACAO_RECEITA_MAP = {
    "HONORARIO": "HONORARIO",
    "HONORARIOS": "HONORARIO",
    "RECEITA HONORARIO": "HONORARIO",
    "RECEITA HONORARIOS": "HONORARIO",
    "RECEITA COM CERTIFICADO": "CERTIFICADO",
    "RECEITA CERTIFICADO": "CERTIFICADO",
    "RECEITA DE CERTIFICADO DIGITAL": "CERTIFICADO",
    "CERTIFICADO": "CERTIFICADO",
    "CERTIFICADOS": "CERTIFICADO",
    "RECEITA FINANCEIRA": "FINANCEIRA",
    "RECEITAS FINANCEIRAS": "FINANCEIRA",
    "FINANCEIRA": "FINANCEIRA",
}

TIPO_DESPESA_MAP = {
    "DESPESA ADMINISTRATIVA": "DESPESA ADMINISTRATIVA",
    "DESPESAS ADMINISTRATIVAS": "DESPESA ADMINISTRATIVA",
    "DESPESA TRABALHISTA": "DESPESA TRABALHISTA",
    "DESPESAS TRABALHISTAS": "DESPESA TRABALHISTA",
    "DESPESA TRIBUTARIA": "DESPESA TRIBUTÁRIA",
    "DESPESAS TRIBUTARIAS": "DESPESA TRIBUTÁRIA",
    "DESPESA TRIBUTÁRIA": "DESPESA TRIBUTÁRIA",
    "DESPESA FINANCEIRA": "DESPESA FINANCEIRA",
    "DESPESAS FINANCEIRAS": "DESPESA FINANCEIRA",
    "DESPESA BANCARIA": "DESPESA BANCÁRIA",
    "DESPESAS BANCARIAS": "DESPESA BANCÁRIA",
    "DESPESA BANCÁRIA": "DESPESA BANCÁRIA",
    "RETIRADA": "RETIRADA",
}

GRUPO_LEGADO_MAP = {
    "DESPESA ADMINISTRATIVA": "DESPESAS ADMINISTRATIVAS",
    "DESPESA TRABALHISTA": "DESPESAS TRABALHISTAS",
    "DESPESA TRIBUTÁRIA": "DESPESAS TRIBUTÁRIAS",
    "DESPESA FINANCEIRA": "DESPESAS FINANCEIRAS",
    "DESPESA BANCÁRIA": "DESPESAS BANCÁRIAS",
    "RETIRADA": "RETIRADA",
}


def normaliza_classificacao_receita(valor: str) -> str:
    if pd.isna(valor):
        return "OUTRAS"
    bruto = str(valor).strip()
    chave = normaliza_texto(bruto, remover_acentos=True, minuscula=False).upper()
    if chave in CLASSIFICACAO_RECEITA_MAP:
        return CLASSIFICACAO_RECEITA_MAP[chave]
    chave_minuscula = normaliza_texto(bruto, remover_acentos=True, minuscula=True)
    chave_minuscula = chave_minuscula.replace("  ", " ").strip()
    for original, destino in CLASSIFICACAO_RECEITA_MAP.items():
        if normaliza_texto(original, remover_acentos=True, minuscula=True) == chave_minuscula:
            return destino
    if "FINANCEIRA" in chave:
        return "FINANCEIRA"
    if "CERTIFIC" in chave:
        return "CERTIFICADO"
    if "HONOR" in chave:
        return "HONORARIO"
    return chave


def normaliza_tipo_despesa(valor: str) -> str:
    if pd.isna(valor):
        return "OUTRAS"
    bruto = str(valor).strip()
    chave = normaliza_texto(bruto, remover_acentos=True, minuscula=False).upper()
    if chave in TIPO_DESPESA_MAP:
        return TIPO_DESPESA_MAP[chave]
    chave_minuscula = normaliza_texto(bruto, remover_acentos=True, minuscula=True)
    chave_minuscula = chave_minuscula.replace("  ", " ").strip()
    for original, destino in TIPO_DESPESA_MAP.items():
        if normaliza_texto(original, remover_acentos=True, minuscula=True) == chave_minuscula:
            return destino
    if "TRAB" in chave:
        return "DESPESA TRABALHISTA"
    if "ADM" in chave or "ADMIN" in chave:
        return "DESPESA ADMINISTRATIVA"
    if "TRIB" in chave:
        return "DESPESA TRIBUTÁRIA"
    if "BANC" in chave:
        return "DESPESA BANCÁRIA"
    if "FINAN" in chave:
        return "DESPESA FINANCEIRA"
    if "RETIR" in chave:
        return "RETIRADA"
    return chave

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

def carrega_receitas_v2(caminho: Path, log: List[str]) -> pd.DataFrame:
    log.append("\n" + "-" * 80)
    log.append("PROCESSANDO RECEITAS (v2)")
    log.append("-" * 80)

    sheet_receitas = 'RECEITAS'
    try:
        df_raw = pd.read_excel(caminho, sheet_name=sheet_receitas)
        log.append(f"✓ Linhas lidas ({sheet_receitas}): {len(df_raw)}")
    except Exception as exc:
        log.append(f"✗ ERRO ao ler RECEITAS ('{sheet_receitas}'): {exc}")
        return pd.DataFrame()

    if df_raw.empty:
        log.append("⚠ AVISO: Aba RECEITAS vazia")
        return pd.DataFrame()

    col_class = col_desc = col_regime = col_valor = None
    for col in df_raw.columns:
        col_norm = normaliza_texto(str(col), remover_acentos=True, minuscula=True)
        if col_norm.startswith('receita') and col_class is None:
            col_class = col
        elif 'descricao' in col_norm or 'cliente' in col_norm:
            col_desc = col
        elif 'regime' in col_norm:
            col_regime = col
        elif 'valor' in col_norm or 'receita' in col_norm:
            col_valor = col

    if not all([col_class, col_desc, col_valor]):
        log.append(f"✗ ERRO: Estrutura inesperada em RECEITAS. Colunas: {df_raw.columns.tolist()}")
        return pd.DataFrame()

    dados = []
    for idx, row in df_raw.iterrows():
        classificacao_bruta = row[col_class]
        descricao = row[col_desc] if col_desc is not None else ''
        regime = row[col_regime] if col_regime is not None else ''
        valor = converte_para_numero(row[col_valor])

        if (pd.isna(classificacao_bruta) or str(classificacao_bruta).strip() == "") and valor == 0:
            continue

        classificacao = normaliza_classificacao_receita(classificacao_bruta)
        cliente = str(descricao).strip()

        if not cliente and classificacao not in {"FINANCEIRA", "CERTIFICADO"}:
            continue

        dados.append({
            'classificacao_receita': classificacao,
            'classificacao_receita_original': str(classificacao_bruta).strip(),
            'cliente': cliente,
            'regime_original': str(regime).strip() if pd.notna(regime) else '',
            'receita': valor,
        })

    df_norm = pd.DataFrame(dados)
    if df_norm.empty:
        log.append("⚠ AVISO: Nenhuma receita válida encontrada")
        return df_norm

    df_norm[['regime_base', 'segmento']] = df_norm['regime_original'].apply(
        lambda texto: pd.Series(parse_regime_e_segmento(texto))
    )

    tipo_receita_map = {
        'HONORARIO': 'HONORÁRIOS',
        'CERTIFICADO': 'CERTIFICADO',
        'FINANCEIRA': 'FINANCEIRA',
    }
    df_norm['tipo_receita'] = df_norm['classificacao_receita'].map(
        lambda x: tipo_receita_map.get(str(x).upper(), str(x).upper())
    )

    df_norm = df_norm[['cliente', 'regime_original', 'regime_base', 'segmento', 'receita', 'classificacao_receita', 'classificacao_receita_original', 'tipo_receita']]

    df_norm['cliente'] = df_norm['cliente'].fillna('').astype(str)
    df_norm['receita'] = df_norm['receita'].astype(float)

    log.append(f"✓ Receitas válidas: {len(df_norm)}")
    log.append(f"✓ Receita total lida: R$ {df_norm['receita'].sum():,.2f}")
    return df_norm


def separa_receitas_por_classificacao(df_receitas: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if df_receitas.empty:
        return {'HONORARIO': pd.DataFrame(), 'CERTIFICADO': pd.DataFrame(), 'FINANCEIRA': pd.DataFrame()}

    resultado: Dict[str, pd.DataFrame] = {}
    for classificacao, grupo in df_receitas.groupby('classificacao_receita', dropna=False):
        resultado[str(classificacao)] = grupo.copy()
    for chave in ['HONORARIO', 'CERTIFICADO', 'FINANCEIRA']:
        resultado.setdefault(chave, pd.DataFrame(columns=df_receitas.columns))
    return resultado


def carrega_despesas_v2(caminho: Path, log: List[str]) -> pd.DataFrame:
    log.append("\n" + "-" * 80)
    log.append("PROCESSANDO DESPESAS (v2)")
    log.append("-" * 80)

    sheet_despesas = 'DESPESAS'
    try:
        df_raw = pd.read_excel(caminho, sheet_name=sheet_despesas, header=None)
        log.append(f"✓ Linhas lidas ({sheet_despesas}): {len(df_raw)}")
    except Exception as exc:
        log.append(f"✗ ERRO ao ler DESPESAS ('{sheet_despesas}'): {exc}")
        return pd.DataFrame()

    if df_raw.empty or len(df_raw.columns) < 3:
        log.append("⚠ AVISO: Estrutura inesperada em DESPESAS")
        return pd.DataFrame()

    registros = []
    for idx in range(1, len(df_raw)):
        tipo_bruto = df_raw.iloc[idx, 0]
        nome_bruto = df_raw.iloc[idx, 1]
        valor_bruto = df_raw.iloc[idx, 2]

        if pd.isna(tipo_bruto) and pd.isna(nome_bruto) and pd.isna(valor_bruto):
            continue

        item_nome = str(nome_bruto).strip() if pd.notna(nome_bruto) else ''
        valor = converte_para_numero(valor_bruto)

        tipo_normalizado = normaliza_tipo_despesa(tipo_bruto)
        tipo_bruto_txt = str(tipo_bruto).strip().upper() if pd.notna(tipo_bruto) else ''
        is_prolabore = 'PRO-LABORE' in item_nome.upper()

        if 'RETIRADA' in tipo_bruto_txt or is_prolabore:
            tipo = 'DESPESA ADMINISTRATIVA'
        elif 'REDUTORA' in item_nome.upper():
            tipo = 'DESPESA ADMINISTRATIVA'
            valor = -abs(valor)
        else:
            tipo = tipo_normalizado

        if item_nome == '' and valor == 0:
            continue

        registros.append({
            'tipo_despesa': tipo,
            'tipo_despesa_original': str(tipo_bruto).strip() if pd.notna(tipo_bruto) else '',
            'item_nome': item_nome,
            'valor': valor,
            'id_linha': idx + 1,
            'is_prolabore': bool(is_prolabore),
        })

    df_norm = pd.DataFrame(registros)
    if df_norm.empty:
        log.append("⚠ AVISO: Nenhuma despesa válida encontrada")
        return df_norm

    df_norm['tipo_trabalhista'] = ""
    df_norm['colaborador'] = ""
    df_norm['departamento_classificado'] = ""
    df_norm['departamento_final'] = ""
    df_norm['fuzzy_score'] = 0.0

    mask_trab = df_norm['tipo_despesa'] == 'DESPESA TRABALHISTA'
    if mask_trab.any():
        df_norm.loc[mask_trab, 'tipo_trabalhista'] = df_norm.loc[mask_trab, 'item_nome'].apply(classifica_tipo_trabalhista)
        df_norm.loc[mask_trab, 'colaborador'] = df_norm.loc[mask_trab, 'item_nome'].apply(extrai_colaborador)
        classificacoes = df_norm.loc[mask_trab, 'colaborador'].apply(
            lambda x: classifica_departamento_fuzzy(x) if x else ("SEM MATCH", 0)
        )
        df_norm.loc[mask_trab, 'departamento_classificado'] = [c[0] for c in classificacoes]
        df_norm.loc[mask_trab, 'fuzzy_score'] = np.array([c[1] for c in classificacoes], dtype=float)

    df_norm['grupo'] = df_norm['tipo_despesa'].map(lambda x: GRUPO_LEGADO_MAP.get(x, str(x).upper()))

    log.append(f"✓ Despesas válidas: {len(df_norm)}")
    for tipo, subtotal in sorted(df_norm.groupby('tipo_despesa')['valor'].sum().items()):
        log.append(f"  • {tipo}: R$ {subtotal:,.2f}")

    return df_norm


def separa_despesas_por_tipo(df_despesas: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if df_despesas.empty:
        return {}
    resultado: Dict[str, pd.DataFrame] = {}
    for tipo, bloco in df_despesas.groupby('tipo_despesa', dropna=False):
        resultado[str(tipo)] = bloco.copy()
    return resultado


def aplica_redutora_administrativa(df_operacionais: pd.DataFrame, valor_redutora: float) -> pd.DataFrame:
    if df_operacionais.empty or valor_redutora == 0:
        return df_operacionais

    df = df_operacionais.copy()
    mask_admin = df['tipo_despesa'] == 'DESPESA ADMINISTRATIVA'
    mask_redutora = mask_admin & df['item_nome'].str.contains('REDUTORA', case=False, na=False)

    if not mask_redutora.any():
        return df

    base_admin = df.loc[mask_admin & ~mask_redutora, 'valor'].sum()
    abatimento = float(valor_redutora)

    df = df.loc[~mask_redutora].copy()

    if base_admin <= 0:
        return df

    fator = (base_admin + abatimento) / base_admin
    fator = max(fator, 0.0)

    df.loc[df['tipo_despesa'] == 'DESPESA ADMINISTRATIVA', 'valor'] *= fator

    return df

# ============================================================================
# RATEIO PROPORCIONAL (mantido - massa salarial direta)
# ============================================================================

def calcula_base_rateio(df_despesas: pd.DataFrame, log: List[str]) -> Dict[str, float]:
    log.append("\n" + "-"*80)
    log.append("CALCULANDO BASE DE RATEIO (Massa Salarial Direta)")
    log.append("-"*80)

    mask_trab = df_despesas['tipo_despesa'] == "DESPESA TRABALHISTA"
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

    mask_trab = df_despesas['tipo_despesa'] == "DESPESA TRABALHISTA"
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

def gera_ticket_medio(df_honorarios: pd.DataFrame):
    df_core = df_honorarios.copy()
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
    df_honorarios: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Calcula margem de contribuição por regime, segmento e cliente.
    Retorna: (df_margem_regime, df_margem_segmento, df_margem_cliente)
    """
    df_core = df_honorarios.copy()
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
# ANÁLISE DO IMPACTO DOS PRÓ-LABORES
# ============================================================================

def analise_impacto_retiradas(
    df_receitas: pd.DataFrame,
    df_despesas_operacionais: pd.DataFrame,
    meses: int
) -> pd.DataFrame:
    """Analisa o impacto dos pró-labores no resultado e margens da empresa."""

    receita_total = float(df_receitas['receita'].sum())
    receita_core = float(df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS']['receita'].sum())

    despesas_totais = float(df_despesas_operacionais['valor'].sum())

    if 'is_prolabore' in df_despesas_operacionais.columns:
        mask_prolab = df_despesas_operacionais['is_prolabore'].fillna(False)
    else:
        mask_prolab = pd.Series([False] * len(df_despesas_operacionais), index=df_despesas_operacionais.index)

    if df_despesas_operacionais.empty:
        prolabores_total = 0.0
    else:
        prolabores_total = float(df_despesas_operacionais.loc[mask_prolab, 'valor'].sum())
    despesas_sem_prolabore = despesas_totais - prolabores_total

    resultado_bruto = receita_total - despesas_totais
    resultado_sem_prolabore = receita_total - despesas_sem_prolabore

    perc_prolabore_sobre_receita_total = (prolabores_total / receita_total * 100) if receita_total else 0.0
    perc_prolabore_sobre_receita_core = (prolabores_total / receita_core * 100) if receita_core else 0.0
    perc_prolabore_sobre_despesas = (prolabores_total / despesas_totais * 100) if despesas_totais else 0.0
    perc_prolabore_sobre_resultado_sem_pl = (
        (prolabores_total / resultado_sem_prolabore * 100)
        if resultado_sem_prolabore
        else np.nan
    )

    margem_com_prolabore = (resultado_bruto / receita_total * 100) if receita_total else 0.0
    margem_sem_prolabore = (resultado_sem_prolabore / receita_total * 100) if receita_total else 0.0
    impacto_margem = margem_sem_prolabore - margem_com_prolabore

    return pd.DataFrame([{
        'receita_total': receita_total,
        'receita_core_honorarios': receita_core,
        'despesas_totais': despesas_totais,
        'despesas_sem_prolabore': despesas_sem_prolabore,
        'prolabores_total': prolabores_total,
        'resultado_bruto_com_prolabore': resultado_bruto,
        'resultado_hipotetico_sem_prolabore': resultado_sem_prolabore,
        'perc_prolabore_sobre_receita_total': perc_prolabore_sobre_receita_total,
        'perc_prolabore_sobre_receita_core': perc_prolabore_sobre_receita_core,
        'perc_prolabore_sobre_despesas_totais': perc_prolabore_sobre_despesas,
        'perc_prolabore_sobre_resultado_sem_pl': perc_prolabore_sobre_resultado_sem_pl,
        'margem_operacional_com_prolabore': margem_com_prolabore,
        'margem_operacional_sem_prolabore': margem_sem_prolabore,
        'impacto_margem_percentual': impacto_margem,
        'meses_analisados': meses,
        'prolabores_mensal_medio': (prolabores_total / meses) if meses else prolabores_total,
    }])

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
    df_honorarios: pd.DataFrame,
    df_margem_regime: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Análise de custo por departamento e ticket médio ponderado por complexidade.
    Retorna: (df_custo_departamento, df_ticket_ponderado)
    """

    mask_trab = df_despesas_rateado['tipo_despesa'] == 'DESPESA TRABALHISTA'
    df_trab = df_despesas_rateado[mask_trab & (df_despesas_rateado['departamento_final'] != "")].copy()

    if not df_trab.empty:
        df_dept = df_trab.groupby('departamento_final')['valor'].sum().reset_index()
        df_dept.columns = ['departamento', 'custo_total']
        df_dept['custo_mensal'] = df_dept['custo_total'] / meses

        total_trab = df_dept['custo_total'].sum()
        df_dept['perc_custo_trabalhista'] = np.where(total_trab > 0, df_dept['custo_total'] / total_trab * 100, 0.0)

        receita_total = df_honorarios['receita'].sum()
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
    """
    Retorna o ranking geral de despesas excluindo pró-labores para evitar distorções.
    """
    if df_despesas.empty:
        return pd.DataFrame(columns=['rank', 'tipo_despesa', 'item_nome', 'valor', 'perc_total_despesas', 'perc_no_tipo'])

    if 'is_prolabore' in df_despesas.columns:
        mask_prolab = df_despesas['is_prolabore'].fillna(False)
    else:
        mask_prolab = pd.Series([False] * len(df_despesas), index=df_despesas.index)

    base_desp = df_despesas.loc[~mask_prolab].copy()

    if base_desp.empty:
        return pd.DataFrame(columns=['rank', 'tipo_despesa', 'item_nome', 'valor', 'perc_total_despesas', 'perc_no_tipo'])

    total_desp = base_desp['valor'].sum()
    base = (
        base_desp.groupby(['tipo_despesa', 'item_nome'], dropna=False)['valor']
                 .sum().reset_index()
    )
    base['perc_total_despesas'] = np.where(total_desp > 0, base['valor'] / total_desp * 100, 0.0)
    total_por_tipo = base.groupby('tipo_despesa', dropna=False)['valor'].transform('sum')
    base['perc_no_tipo'] = np.where(total_por_tipo > 0, base['valor'] / total_por_tipo * 100, 0.0)

    base = base.sort_values('valor', ascending=False).reset_index(drop=True)
    base['rank'] = range(1, len(base) + 1)
    top_geral = base.head(top_n).copy()
    return top_geral[['rank', 'tipo_despesa', 'item_nome', 'valor', 'perc_total_despesas', 'perc_no_tipo']]

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
        grupos = df_despesas_rateado.groupby('tipo_despesa')['valor'].sum()
        custos_adm_liquidos = float(grupos.get('DESPESA ADMINISTRATIVA', 0.0))
        total_trabalhistas = float(grupos.get('DESPESA TRABALHISTA', 0.0))
        total_tributarias = float(grupos.get('DESPESA TRIBUTÁRIA', 0.0))
        total_financeiras = float(grupos.get('DESPESA FINANCEIRA', 0.0))
        total_bancarias = float(grupos.get('DESPESA BANCÁRIA', 0.0))

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
    df_honorarios: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    pesos_regime: Optional[Dict[str, float]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Gera os DataFrames de resultado por regime e segmento na visão única por peso."""
    meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1
    df_core = df_honorarios.copy()
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

def gerar_dre(
    receita_honorarios: float,
    receita_certificados: float,
    receita_financeira: float,
    df_despesas_operacionais: pd.DataFrame,
    valor_redutora: float
) -> pd.DataFrame:
    """Gera a DRE simplificada com pró-labores integrados às despesas administrativas."""

    receita_honorarios = float(receita_honorarios or 0.0)
    receita_certificados = float(receita_certificados or 0.0)
    receita_financeira = float(receita_financeira or 0.0)

    receita_operacional_bruta = receita_honorarios + receita_certificados
    receita_total = receita_operacional_bruta + receita_financeira

    if df_despesas_operacionais.empty:
        totais = {
            'DESPESA ADMINISTRATIVA': 0.0,
            'DESPESA TRABALHISTA': 0.0,
            'DESPESA TRIBUTÁRIA': 0.0,
            'DESPESA FINANCEIRA': 0.0,
            'DESPESA BANCÁRIA': 0.0,
        }
    else:
        totais = df_despesas_operacionais.groupby('tipo_despesa')['valor'].sum().to_dict()

    despesas_adm = float(totais.get('DESPESA ADMINISTRATIVA', 0.0))
    despesas_trab = float(totais.get('DESPESA TRABALHISTA', 0.0))
    despesas_trib = float(totais.get('DESPESA TRIBUTÁRIA', 0.0))
    despesas_fin = float(totais.get('DESPESA FINANCEIRA', 0.0))
    despesas_banc = float(totais.get('DESPESA BANCÁRIA', 0.0))

    total_despesas_operacionais = despesas_adm + despesas_trab + despesas_trib + despesas_fin + despesas_banc
    resultado_operacional = receita_operacional_bruta - total_despesas_operacionais
    resultado_liquido = resultado_operacional

    prolabores_total = 0.0
    detalhes_prolabore: List[str] = []
    if not df_despesas_operacionais.empty:
        if 'is_prolabore' in df_despesas_operacionais.columns:
            mask_prolab = df_despesas_operacionais['is_prolabore'].fillna(False)
        else:
            mask_prolab = pd.Series([False] * len(df_despesas_operacionais), index=df_despesas_operacionais.index)

        if mask_prolab.any():
            df_prolab = df_despesas_operacionais.loc[mask_prolab].copy()
            prolabores_total = float(df_prolab['valor'].sum())
            for _, row in df_prolab.iterrows():
                nome = str(row.get('item_nome', '')).strip()
                valor = float(row.get('valor', 0.0))
                if nome:
                    detalhes_prolabore.append(f"• {nome.upper()}  R$ {valor:,.2f}")

    def perc_receita(valor: float) -> float:
        return (valor / receita_total * 100.0) if receita_total else 0.0

    def perc_resultado(valor: float) -> float:
        return (valor / resultado_operacional * 100.0) if resultado_operacional else 0.0

    linhas: List[Dict[str, object]] = []

    linhas.append({
        'bloco': '1. RECEITA BRUTA',
        'descricao': 'Receita com Honorários Contábeis',
        'valor': receita_honorarios,
        'percentual_receita_total': perc_receita(receita_honorarios),
        'percentual_resultado_operacional': perc_resultado(receita_honorarios),
        'observacao': ''
    })
    linhas.append({
        'bloco': '1. RECEITA BRUTA',
        'descricao': 'Receita com Certificados Digitais',
        'valor': receita_certificados,
        'percentual_receita_total': perc_receita(receita_certificados),
        'percentual_resultado_operacional': perc_resultado(receita_certificados),
        'observacao': ''
    })
    linhas.append({
        'bloco': '1. RECEITA BRUTA',
        'descricao': '(=) RECEITA OPERACIONAL BRUTA',
        'valor': receita_operacional_bruta,
        'percentual_receita_total': perc_receita(receita_operacional_bruta),
        'percentual_resultado_operacional': perc_resultado(receita_operacional_bruta),
        'observacao': ''
    })

    linhas.append({
        'bloco': '2. RECEITAS FINANCEIRAS',
        'descricao': 'Juros, Rendimentos e Receitas Financeiras',
        'valor': receita_financeira,
        'percentual_receita_total': perc_receita(receita_financeira),
        'percentual_resultado_operacional': perc_resultado(receita_financeira),
        'observacao': ''
    })
    linhas.append({
        'bloco': '2. RECEITAS FINANCEIRAS',
        'descricao': '(=) RECEITA TOTAL',
        'valor': receita_total,
        'percentual_receita_total': perc_receita(receita_total),
        'percentual_resultado_operacional': perc_resultado(receita_total),
        'observacao': ''
    })

    observacao_partes: List[str] = []
    if valor_redutora:
        observacao_partes.append(f"Já líquidas da redutora advocacia: R$ {abs(valor_redutora):,.2f}")
    if prolabores_total:
        observacao_partes.append(f"Inclui pró-labores: R$ {prolabores_total:,.2f}")
    observacao_admin = " | ".join(observacao_partes)

    linhas.extend([
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(-) Despesas Administrativas',
            'valor': -despesas_adm,
            'percentual_receita_total': -perc_receita(despesas_adm),
            'percentual_resultado_operacional': perc_resultado(-despesas_adm),
            'observacao': observacao_admin,
        },
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(-) Despesas Trabalhistas Rateadas',
            'valor': -despesas_trab,
            'percentual_receita_total': -perc_receita(despesas_trab),
            'percentual_resultado_operacional': perc_resultado(-despesas_trab),
            'observacao': ''
        },
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(-) Despesas Tributárias',
            'valor': -despesas_trib,
            'percentual_receita_total': -perc_receita(despesas_trib),
            'percentual_resultado_operacional': perc_resultado(-despesas_trib),
            'observacao': ''
        },
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(-) Despesas Financeiras',
            'valor': -despesas_fin,
            'percentual_receita_total': -perc_receita(despesas_fin),
            'percentual_resultado_operacional': perc_resultado(-despesas_fin),
            'observacao': ''
        },
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(-) Despesas Bancárias',
            'valor': -despesas_banc,
            'percentual_receita_total': -perc_receita(despesas_banc),
            'percentual_resultado_operacional': perc_resultado(-despesas_banc),
            'observacao': ''
        },
        {
            'bloco': '3. DESPESAS OPERACIONAIS',
            'descricao': '(=) RESULTADO OPERACIONAL',
            'valor': resultado_operacional,
            'percentual_receita_total': perc_receita(resultado_operacional),
            'percentual_resultado_operacional': 100.0,
            'observacao': ''
        },
    ])

    linhas.append({
        'bloco': '4. RESULTADO FINAL',
        'descricao': '(=) RESULTADO OPERACIONAL / LÍQUIDO',
        'valor': resultado_liquido,
        'percentual_receita_total': perc_receita(resultado_liquido),
        'percentual_resultado_operacional': perc_resultado(resultado_liquido),
        'observacao': ''
    })

    linhas.append({
        'bloco': 'DISTRIBUIÇÃO INTERNA',
        'descricao': 'Pró-labores dos Sócios (já incluídos em ADM)',
        'valor': prolabores_total,
        'percentual_receita_total': perc_receita(prolabores_total),
        'percentual_resultado_operacional': perc_resultado(prolabores_total),
        'observacao': "\n".join(detalhes_prolabore)
    })

    margem_operacional = perc_receita(resultado_operacional)
    perc_prolab_receita = perc_receita(prolabores_total)
    perc_prolab_resultado = perc_resultado(prolabores_total)

    linhas.extend([
        {
            'bloco': 'INDICADORES',
            'descricao': 'Margem Operacional (%)',
            'valor': margem_operacional,
            'percentual_receita_total': margem_operacional,
            'percentual_resultado_operacional': margem_operacional,
            'observacao': ''
        },
        {
            'bloco': 'INDICADORES',
            'descricao': '% Pró-labores sobre Receita Total',
            'valor': perc_prolab_receita,
            'percentual_receita_total': perc_prolab_receita,
            'percentual_resultado_operacional': perc_prolab_receita,
            'observacao': ''
        },
        {
            'bloco': 'INDICADORES',
            'descricao': '% Pró-labores sobre Resultado Operacional',
            'valor': perc_prolab_resultado,
            'percentual_receita_total': perc_prolab_resultado,
            'percentual_resultado_operacional': perc_prolab_resultado,
            'observacao': ''
        },
    ])

    return pd.DataFrame(linhas)[[
        'bloco',
        'descricao',
        'valor',
        'percentual_receita_total',
        'percentual_resultado_operacional',
        'observacao',
    ]]

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

        df_receitas = carrega_receitas_v2(ARQUIVO_ENTRADA, log)
        if df_receitas.empty:
            log.append("✗ ERRO: Não há dados válidos de RECEITAS. Verifique a planilha de origem.")
            return log

        receitas_class = separa_receitas_por_classificacao(df_receitas)
        empty_receitas = df_receitas.head(0).copy()
        df_honorarios = receitas_class.get('HONORARIO', empty_receitas.copy())
        df_certificados = receitas_class.get('CERTIFICADO', empty_receitas.copy())
        df_financeiras = receitas_class.get('FINANCEIRA', empty_receitas.copy())

        log.append(f"✓ Receitas HONORÁRIO (base de cálculos): R$ {df_honorarios['receita'].sum():,.2f}")
        log.append(f"✓ Receitas CERTIFICADOS (apenas DRE): R$ {df_certificados['receita'].sum():,.2f}")
        log.append(f"✓ Receitas FINANCEIRAS (apenas DRE): R$ {df_financeiras['receita'].sum():,.2f}")

        df_despesas = carrega_despesas_v2(ARQUIVO_ENTRADA, log)
        if df_despesas.empty:
            log.append("✗ ERRO: Não há dados válidos de DESPESAS. Verifique a planilha de origem.")
            return log

        df_despesas_operacionais_bruto = df_despesas.copy()
        df_despesas_operacionais = df_despesas_operacionais_bruto.copy()

        mask_redutora = (
            (df_despesas_operacionais['tipo_despesa'] == 'DESPESA ADMINISTRATIVA') &
            (df_despesas_operacionais['item_nome'].str.contains('REDUTORA', case=False, na=False))
        )
        valor_redutora = df_despesas_operacionais.loc[mask_redutora, 'valor'].sum()
        df_despesas_operacionais = aplica_redutora_administrativa(df_despesas_operacionais, valor_redutora)

        total_desp_oper = float(df_despesas_operacionais['valor'].sum())
        if 'is_prolabore' in df_despesas_operacionais.columns:
            mask_prolab_log = df_despesas_operacionais['is_prolabore'].fillna(False)
            total_prolabores_log = float(df_despesas_operacionais.loc[mask_prolab_log, 'valor'].sum())
        else:
            total_prolabores_log = 0.0

        log.append(f"✓ Despesas OPERACIONAIS (para rateio): R$ {total_desp_oper:,.2f}")
        log.append(f"✓ Pró-labores incluídos em Despesas ADM: R$ {total_prolabores_log:,.2f}")
        log.append(f"✓ Redutora ADVOCACIA aplicada: R$ {valor_redutora:,.2f}")

        df_desp_rateado_geral = pd.DataFrame()
        pesos = {dept: 0.0 for dept in DEPARTAMENTOS}
        if not df_despesas_operacionais.empty:
            pesos = calcula_base_rateio(df_despesas_operacionais, log)
            df_desp_rateado_geral = aplica_rateio_proporcional(df_despesas_operacionais, pesos, log)

        if 'is_prolabore' in df_despesas_operacionais.columns:
            mask_prolab_geral = df_despesas_operacionais['is_prolabore'].fillna(False)
            total_retiradas = float(df_despesas_operacionais.loc[mask_prolab_geral, 'valor'].sum())
        else:
            total_retiradas = 0.0

        df_ticket_geral, df_ticket_regime, df_ticket_reg_ativ, df_ticket_clientes = gera_ticket_medio(df_honorarios)

        df_top_desp = gera_top_despesas(df_despesas_operacionais, TOP_N_DESPESAS) if not df_despesas_operacionais.empty else pd.DataFrame()

        df_regime_peso, df_seg_peso = custo_absorcao_por_peso(df_honorarios, df_desp_rateado_geral, pesos_regime=PESOS_COMPLEXIDADE_REGIME) if not df_honorarios.empty else (
            _empty_df(RESULTADO_REGIME_FULL_COLS),
            _empty_df([
                'regime_base', 'segmento', 'qtd_clientes', 'receita_segmento', 'receita_mensal_segmento',
                'custo_mensal_segmento', 'ticket_medio_mensal', 'custo_medio_mensal',
                'resultado_mensal', 'resultado_medio_mensal'
            ])
        )

        df_regime_peso__noPL, df_seg_peso__noPL = df_regime_peso.copy(), df_seg_peso.copy()

        meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1
        df_impacto_retiradas = analise_impacto_retiradas(
            df_receitas,
            df_despesas_operacionais,
            meses
        )
        df_margem_regime, df_margem_segmento, df_margem_cliente = analise_margem_contribuicao(
            df_honorarios, df_desp_rateado_geral, meses
        )

        df_ponto_equilibrio = analise_ponto_equilibrio(df_margem_regime) if not df_margem_regime.empty else pd.DataFrame()
        df_roi_regime, df_clientes_deficitarios, df_ranking_rentabilidade = analise_rentabilidade(
            df_margem_regime, df_margem_cliente
        )

        df_resumo_retirada_completo, df_comparativo_retirada_regime = analise_retirada_socios_completa(
            df_receitas, df_desp_rateado_geral, total_retiradas, df_margem_regime, meses
        )

        df_absorcao = analise_absorcao_custos(df_margem_regime)
        df_curva_abc, df_concentracao, df_diversificacao = analise_concentracao_risco(df_margem_cliente, df_receitas)
        df_custo_dept, df_ticket_ponderado = analise_eficiencia_operacional(df_desp_rateado_geral, df_honorarios, df_margem_regime, meses)
        df_cenario_otim, df_cenario_pess, df_cenario_reaj, df_cenario_ideal = analise_cenarios(
            df_margem_regime, df_margem_cliente, df_receitas, df_desp_rateado_geral, total_retiradas
        )

        dre = gerar_dre(
            receita_honorarios=df_honorarios['receita'].sum(),
            receita_certificados=df_certificados['receita'].sum(),
            receita_financeira=df_financeiras['receita'].sum(),
            df_despesas_operacionais=df_desp_rateado_geral,
            valor_redutora=valor_redutora
        )

        df_regime_peso_excel = _prepare_regime_excel(df_regime_peso)
        df_seg_peso_excel = _prepare_segmento_excel(df_seg_peso)
        df_regime_peso_no_pl_excel = _prepare_regime_excel(df_regime_peso__noPL)
        df_seg_peso_no_pl_excel = _prepare_segmento_excel(df_seg_peso__noPL)

        abas_para_salvar: List[Tuple[str, pd.DataFrame]] = [
            ('Receitas_Base', df_receitas),
            ('Receitas_Honorarios', df_honorarios),
            ('Receitas_Certificados', df_certificados),
            ('Receitas_Financeiras', df_financeiras),
            ('Despesas_Base_Original', df_despesas),
            ('Despesas_Operacionais', df_despesas_operacionais),
            ('Despesas_Rateadas_Geral', df_desp_rateado_geral),
            ('Ticket_Geral', df_ticket_geral),
            ('Ticket_Regime', df_ticket_regime),
            ('Ticket_Regime_Atividade', df_ticket_reg_ativ),
            ('Ticket_Clientes', df_ticket_clientes),
            ('Impacto_Retiradas_Socios', df_impacto_retiradas),
            ('Top_Despesas', df_top_desp),
            ('Resultado_Regime_Com_Peso', df_regime_peso_excel),
            ('Resultado_Segmento_Com_Peso', df_seg_peso_excel),
            ('Res_Regime_Com_Peso_Sem_PL', df_regime_peso_no_pl_excel),
            ('Res_Segmento_Com_Peso_Sem_PL', df_seg_peso_no_pl_excel),
            ('DRE_Simplificada', dre),
            ('Margem_Contribuicao_Regime', df_margem_regime),
            ('Margem_Contribuicao_Segmento', df_margem_segmento),
            ('Margem_Contribuicao_Cliente', df_margem_cliente),
            ('Ponto_Equilibrio_Regime', df_ponto_equilibrio),
            ('ROI_Rentabilidade_Regime', df_roi_regime),
            ('Clientes_Deficitarios', df_clientes_deficitarios),
            ('Ranking_Rentabilidade', df_ranking_rentabilidade),
            ('Retirada_Completa', df_resumo_retirada_completo),
            ('Retirada_Por_Regime', df_comparativo_retirada_regime),
            ('Absorcao_Custos_Regime', df_absorcao),
            ('Curva_ABC_Clientes', df_curva_abc),
            ('Indice_Concentracao', df_concentracao),
            ('Diversificacao_Regime', df_diversificacao),
            ('Custo_Por_Departamento', df_custo_dept),
            ('Ticket_Ponderado_Complexidade', df_ticket_ponderado),
            ('Cenario_Otimista', df_cenario_otim),
            ('Cenario_Pessimista', df_cenario_pess),
            ('Cenario_Reajuste', df_cenario_reaj),
            ('Cenario_Ideal_Retirada', df_cenario_ideal),
        ]

        nomes_abas = [nome for nome, _ in abas_para_salvar] + ['Log']
        total_abas = len(nomes_abas)
        log.append(f"✓ Abas preparadas para exportação ({total_abas} incluindo Log): {', '.join(nomes_abas)}")

        with pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl') as writer:
            for nome, df_sheet in abas_para_salvar:
                _safe_to_excel(df_sheet, writer, nome)

            log.append(f"✓ Arquivo Excel salvo em: {str(ARQUIVO_SAIDA.resolve())}")
            log.append(f"✓ Total de abas geradas: {total_abas}")

            try:
                ARQUIVO_LOG.parent.mkdir(parents=True, exist_ok=True)
                sucesso_log_msg = f"✓ Log exportado para: {ARQUIVO_LOG.resolve()}"
                linhas_log = [_sanitize_excel_text(item) for item in (log + [sucesso_log_msg])]
                conteudo_log = "\n".join(linhas_log)
                ARQUIVO_LOG.write_text(conteudo_log, encoding='utf-8')
                log.append(sucesso_log_msg)
            except Exception as exc:
                log.append(f"⚠ Não foi possível gravar log externo: {exc}")

            _safe_to_excel(pd.DataFrame({"log": log}), writer, 'Log')

        valida = valida_resultado(
            df_receitas,
            df_honorarios,
            df_certificados,
            df_financeiras,
            df_despesas,
            df_despesas_operacionais_bruto,
            df_despesas_operacionais,
            df_desp_rateado_geral,
            valor_redutora
        )
        if valida:
            log.append("\nVALIDAÇÕES DO MODELO")
            for chk in valida:
                log.append(f"  • {chk}")

    except Exception as e:
        log.append(f"✗ ERRO inesperado: {e}")

    return log


# ============================================================================
# MAIN
# ============================================================================

def valida_resultado(
    df_receitas: pd.DataFrame,
    df_honorarios: pd.DataFrame,
    df_certificados: pd.DataFrame,
    df_financeiras: pd.DataFrame,
    df_despesas: pd.DataFrame,
    df_despesas_operacionais_bruto: pd.DataFrame,
    df_despesas_operacionais: pd.DataFrame,
    df_despesas_rateadas: pd.DataFrame,
    valor_redutora: float,
) -> List[str]:
    checks: List[str] = []

    total_h = df_honorarios['receita'].sum()
    total_c = df_certificados['receita'].sum()
    total_f = df_financeiras['receita'].sum()
    total_receitas = df_receitas['receita'].sum()
    delta_receitas = abs((total_h + total_c + total_f) - total_receitas)
    if delta_receitas < 0.01:
        checks.append("Receitas por classificação conferem com o total.")
    else:
        checks.append(f"⚠️ Divergência em receitas: R$ {delta_receitas:,.2f}")

    total_oper = df_despesas_operacionais['valor'].sum()
    total_desp = df_despesas['valor'].sum()
    delta_desp = abs(total_oper - total_desp)
    if delta_desp < 0.01:
        checks.append("Despesas operacionais conferem com o total lido.")
    else:
        checks.append(f"⚠️ Divergência em despesas operacionais: R$ {delta_desp:,.2f}")

    valor_adm_bruto = df_despesas_operacionais_bruto[
        df_despesas_operacionais_bruto['tipo_despesa'] == 'DESPESA ADMINISTRATIVA'
    ]['valor'].sum()
    valor_adm_liquido = df_despesas_operacionais[
        df_despesas_operacionais['tipo_despesa'] == 'DESPESA ADMINISTRATIVA'
    ]['valor'].sum()
    if valor_redutora:
        if valor_adm_liquido <= valor_adm_bruto:
            checks.append("Redutora de advocacia aplicada às despesas administrativas.")
        else:
            checks.append("⚠️ Redutora de advocacia não reduziu as despesas administrativas.")
    else:
        checks.append("Nenhuma redutora de advocacia identificada.")

    if df_despesas_rateadas.empty:
        checks.append("⚠️ Despesas rateadas indisponíveis para validar pró-labores.")
    elif 'is_prolabore' in df_despesas_rateadas.columns:
        checks.append("Pró-labores integrados às despesas rateadas.")
    else:
        checks.append("⚠️ Coluna 'is_prolabore' ausente nas despesas rateadas.")

    honorario_unicos = set(df_honorarios['classificacao_receita'].unique())
    if honorario_unicos == {'HONORARIO'} or honorario_unicos == {'HONORARIO', ''}:
        checks.append("Análises baseadas apenas em HONORÁRIO.")
    else:
        checks.append(f"⚠️ Tipos inesperados nas receitas base: {honorario_unicos}")

    return checks


if __name__ == "__main__":
    logs = gera_relatorio_completo()
    print("\n".join(logs))
