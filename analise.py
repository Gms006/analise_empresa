#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema de Análise Financeira Padronizada
Versão: 3.4 - Esforço (peso+volume+ticket) + abatimento ADM sem afetar pró-labores
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

# Pesos de complexidade por regime (visão B - COM PESO)
PESOS_COMPLEXIDADE_REGIME: Dict[str, float] = {
    "Lucro Real": 1.6,
    "Lucro Presumido": 1.2,
    "Simples Nacional": 1.0,
    "PF": 0.6,
    "Imune/Isenta": 0.6,
    "Paralisada": 0.5,
    "Não informado": 1.0,  # fallback
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

def carrega_receitas(caminho: Path, log: List[str]) -> Tuple[pd.DataFrame, float]:
    """
    Lê RECEITAS e retorna (df_receitas_validas, valor_rateio_advocacia),
    removendo do DF as linhas de 'rateio advocacia', para que elas não entrem como receita.
    """
    log.append("\n" + "-"*80)
    log.append("PROCESSANDO RECEITAS")
    log.append("-"*80)
    try:
        df = pd.read_excel(caminho, sheet_name="RECEITAS")
        log.append(f"✓ Linhas lidas: {len(df)}")
    except Exception as e:
        log.append(f"✗ ERRO ao ler RECEITAS: {e}")
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
    df_norm['cliente']          = df[col_cliente].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df_norm['regime_original']  = df[col_regime].apply(lambda x: str(x).strip() if pd.notna(x) else "")
    df_norm['receita']          = df[col_receita].apply(converte_para_numero)
    df_norm['empresa_padronizada'] = df_norm['cliente'].apply(
        lambda x: normaliza_texto(x, remover_acentos=True, minuscula=False).upper()
    )
    df_norm[['regime_base', 'segmento']] = df_norm['regime_original'].apply(
        lambda x: pd.Series(parse_regime_e_segmento(x))
    )

    # Identifica e separa as receitas de rateio-advocacia (abatimento)
    df_norm['is_rateio_adv'] = df_norm['cliente'].apply(is_rateio_advocacia)
    valor_rateio_adv = df_norm.loc[df_norm['is_rateio_adv'], 'receita'].sum()

    # Remove essas linhas do dataset de receitas (não serão receita)
    df_norm = df_norm[~df_norm['is_rateio_adv']].copy()

    # Classificação do restante
    df_norm['tipo_receita'] = df_norm['cliente'].apply(classifica_tipo_receita)
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
    try:
        df = pd.read_excel(caminho, sheet_name="DESPESAS", header=None)
        log.append(f"✓ Linhas lidas: {len(df)}")
    except Exception as e:
        log.append(f"✗ ERRO ao ler DESPESAS: {e}")
        return pd.DataFrame()

    registros = []
    for idx in range(1, len(df)):
        grupo_raw = df.iloc[idx, 0] if len(df.columns) > 0 else None
        nome_raw  = df.iloc[idx, 1] if len(df.columns) > 1 else None
        valor_raw = df.iloc[idx, 2] if len(df.columns) > 2 else None

        if pd.isna(grupo_raw) and pd.isna(nome_raw) and pd.isna(valor_raw):
            continue

        grupo = classifica_grupo_despesa(grupo_raw)
        nome  = str(nome_raw).strip() if pd.notna(nome_raw) else ""
        valor = converte_para_numero(valor_raw)

        if nome and valor != 0:
            registros.append({'grupo': grupo, 'item_nome': nome, 'valor': valor, 'id_linha': idx + 1})

    df_norm = pd.DataFrame(registros)
    if df_norm.empty:
        log.append("⚠ AVISO: Nenhuma despesa válida")
        return df_norm

    # Enriquecimento para trabalhistas
    mask_trab = df_norm['grupo'] == "DESPESAS TRABALHISTAS"
    df_norm['tipo_trabalhista'] = ""
    df_norm['colaborador'] = ""
    df_norm['departamento_classificado'] = ""
    df_norm['fuzzy_score'] = 0.0  # dtype float para evitar FutureWarning

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

    df_clientes = (
        df_core.groupby(['cliente', 'regime_base', 'segmento'], dropna=False)['receita']
               .sum().reset_index()
    )

    # Geral
    qtd_clientes = len(df_clientes)
    receita_acumulada = df_clientes['receita'].sum()
    receita_mensal = receita_acumulada / meses
    ticket_medio_mensal = (receita_mensal / qtd_clientes) if qtd_clientes > 0 else 0.0
    ticket_mediano_mensal = (df_clientes['receita'] / meses).median() if qtd_clientes > 0 else 0.0
    ticket_geral = pd.DataFrame([{
        "qtd_clientes": qtd_clientes,
        "receita_acumulada": receita_acumulada,
        "receita_mensal": receita_mensal,
        "ticket_medio_mensal": ticket_medio_mensal,
        "ticket_mediano_mensal": ticket_mediano_mensal,
    }])

    # Por regime
    por_regime = (
        df_clientes.groupby('regime_base', dropna=False)['receita']
                   .agg(['sum', 'count']).reset_index()
    )
    por_regime.columns = ['regime_base', 'receita_acumulada', 'qtd_clientes']
    por_regime['receita_mensal'] = por_regime['receita_acumulada'] / meses
    por_regime['ticket_medio_mensal'] = np.where(
        por_regime['qtd_clientes'] > 0,
        por_regime['receita_mensal'] / por_regime['qtd_clientes'],
        0.0
    )
    med_reg = (
        df_clientes.assign(rec_mensal=df_clientes['receita'] / meses)
                   .groupby('regime_base', dropna=False)['rec_mensal']
                   .median().reset_index(name='ticket_mediano_mensal')
    )
    por_regime = por_regime.merge(med_reg, on='regime_base', how='left')

    # Por regime + atividade
    por_reg_ativ = (
        df_clientes.groupby(['regime_base', 'segmento'], dropna=False)['receita']
                   .agg(['sum', 'count']).reset_index()
    )
    por_reg_ativ.columns = ['regime_base', 'segmento', 'receita_acumulada', 'qtd_clientes']
    por_reg_ativ['receita_mensal'] = por_reg_ativ['receita_acumulada'] / meses
    por_reg_ativ['ticket_medio_mensal'] = np.where(
        por_reg_ativ['qtd_clientes'] > 0,
        por_reg_ativ['receita_mensal'] / por_reg_ativ['qtd_clientes'],
        0.0
    )
    med_reg_ativ = (
        df_clientes.assign(rec_mensal=df_clientes['receita'] / meses)
                   .groupby(['regime_base', 'segmento'], dropna=False)['rec_mensal']
                   .median().reset_index(name='ticket_mediano_mensal')
    )
    por_reg_ativ = por_reg_ativ.merge(med_reg_ativ, on=['regime_base', 'segmento'], how='left')

    # Por cliente
    por_cliente = df_clientes.copy()
    por_cliente['ticket_mensal'] = por_cliente['receita'] / meses
    por_cliente = por_cliente.sort_values('ticket_mensal', ascending=False).reset_index(drop=True)
    por_cliente['rank'] = range(1, len(por_cliente) + 1)

    return ticket_geral, por_regime, por_reg_ativ, por_cliente

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
# DUAS VISÕES DE CUSTO POR ABSORÇÃO E RESULTADO (SEM PESO / COM PESO)
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

def _aplica_visao_sem_peso(
    df_core: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    meses: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Custo por absorção proporcional à receita por regime; depois por segmento dentro do regime."""
    custo_total = df_despesas_rateado['valor'].sum() if not df_despesas_rateado.empty else 0.0
    _, rec_regime, rec_seg = _participacoes(df_core, meses)

    # por regime
    rec_regime['custo_mensal'] = rec_regime['perc_receita_total'] * (custo_total / meses if meses else custo_total)

    clientes_regime = _clientes_por_regime(df_core)
    rec_regime = rec_regime.merge(clientes_regime, on='regime_base', how='left')
    rec_regime['qtd_clientes'] = rec_regime['qtd_clientes'].fillna(0).astype(int)

    rec_regime['ticket_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['receita_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    rec_regime['custo_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['custo_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    rec_regime['resultado_mensal'] = rec_regime['receita_mensal'] - rec_regime['custo_mensal']
    rec_regime['resultado_medio_mensal'] = rec_regime['ticket_medio_mensal'] - rec_regime['custo_medio_mensal']

    df_regime = rec_regime[['regime_base', 'qtd_clientes', 'receita_regime', 'receita_mensal',
                            'perc_receita_total', 'custo_mensal',
                            'ticket_medio_mensal', 'custo_medio_mensal',
                            'resultado_mensal', 'resultado_medio_mensal']].copy()
    df_regime = df_regime.sort_values('receita_regime', ascending=False).reset_index(drop=True)

    # por (regime, segmento)
    rec_seg = rec_seg.merge(df_regime[['regime_base', 'custo_mensal']], on='regime_base', how='left', suffixes=('', '_regime'))
    rec_seg['custo_mensal_segmento'] = rec_seg['prop_no_regime'] * rec_seg['custo_mensal']

    clientes_seg = _clientes_por_regime_segmento(df_core)
    rec_seg = rec_seg.merge(clientes_seg, on=['regime_base', 'segmento'], how='left')
    rec_seg['qtd_clientes'] = rec_seg['qtd_clientes'].fillna(0).astype(int)

    rec_seg['ticket_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['receita_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0
    )
    rec_seg['custo_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['custo_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0
    )
    rec_seg['resultado_mensal'] = rec_seg['receita_mensal_segmento'] - rec_seg['custo_mensal_segmento']
    rec_seg['resultado_medio_mensal'] = rec_seg['ticket_medio_mensal'] - rec_seg['custo_medio_mensal']

    df_segmento = rec_seg[['regime_base', 'segmento', 'qtd_clientes',
                           'receita_segmento', 'receita_mensal_segmento',
                           'custo_mensal_segmento',
                           'ticket_medio_mensal', 'custo_medio_mensal',
                           'resultado_mensal', 'resultado_medio_mensal']].copy()
    df_segmento = df_segmento.sort_values(['regime_base', 'receita_segmento'], ascending=[True, False]).reset_index(drop=True)

    return df_regime, df_segmento

def _aplica_visao_com_peso(
    df_core: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    meses: int,
    pesos_regime: Optional[Dict[str, float]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Custo por absorção proporcional à participação de ESFORÇO do regime:
      Esforço_g = w_g * (n_g ** ALFA_VOLUME) * ((ticket_g / ticket_geral) ** BETA_TICKET)
    onde n_g é o nº de clientes do regime e ticket_g é o ticket médio mensal do regime.
    """
    custo_total = df_despesas_rateado['valor'].sum() if not df_despesas_rateado.empty else 0.0
    _, rec_regime, rec_seg = _participacoes(df_core, meses)

    if pesos_regime is None:
        pesos_regime = PESOS_COMPLEXIDADE_REGIME

    clientes_regime = _clientes_por_regime(df_core)  # qtd_clientes por regime
    rec_regime = rec_regime.merge(clientes_regime, on='regime_base', how='left')
    rec_regime['qtd_clientes'] = rec_regime['qtd_clientes'].fillna(0).astype(int)

    # Tickets por regime e ticket geral
    total_clientes = rec_regime['qtd_clientes'].sum()
    rec_regime['ticket_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['receita_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    receita_mensal_total = rec_regime['receita_mensal'].sum()
    ticket_geral = (receita_mensal_total / total_clientes) if total_clientes > 0 else 0.0

    # Esforço
    rec_regime['peso'] = rec_regime['regime_base'].map(pesos_regime).fillna(1.0)
    ng = rec_regime['qtd_clientes'].clip(lower=1)  # evita zero
    t_ratio = np.where(ticket_geral > 0, rec_regime['ticket_medio_mensal'] / ticket_geral, 1.0)
    rec_regime['esforco'] = rec_regime['peso'] * (ng ** ALFA_VOLUME) * (t_ratio ** BETA_TICKET)

    soma_esforco = rec_regime['esforco'].sum()
    rec_regime['participacao_esforco'] = np.where(soma_esforco > 0, rec_regime['esforco'] / soma_esforco, 0.0)
    rec_regime['custo_mensal'] = rec_regime['participacao_esforco'] * (custo_total / meses if meses else custo_total)

    rec_regime['custo_medio_mensal'] = np.where(
        rec_regime['qtd_clientes'] > 0,
        rec_regime['custo_mensal'] / rec_regime['qtd_clientes'],
        0.0
    )
    rec_regime['resultado_mensal'] = rec_regime['receita_mensal'] - rec_regime['custo_mensal']
    rec_regime['resultado_medio_mensal'] = rec_regime['ticket_medio_mensal'] - rec_regime['custo_medio_mensal']

    df_regime = rec_regime[['regime_base', 'qtd_clientes', 'peso',
                            'ticket_medio_mensal', 'participacao_esforco',
                            'receita_regime', 'receita_mensal',
                            'custo_mensal',
                            'custo_medio_mensal',
                            'resultado_mensal', 'resultado_medio_mensal']].copy()
    df_regime = df_regime.sort_values('receita_regime', ascending=False).reset_index(drop=True)

    # por (regime, segmento) — distribuição interna proporcional à receita no regime
    rec_seg = rec_seg.merge(df_regime[['regime_base', 'custo_mensal']], on='regime_base', how='left', suffixes=('', '_regime'))
    rec_seg['custo_mensal_segmento'] = rec_seg['prop_no_regime'] * rec_seg['custo_mensal']

    clientes_seg = _clientes_por_regime_segmento(df_core)
    rec_seg = rec_seg.merge(clientes_seg, on=['regime_base', 'segmento'], how='left')
    rec_seg['qtd_clientes'] = rec_seg['qtd_clientes'].fillna(0).astype(int)

    rec_seg['ticket_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['receita_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0
    )
    rec_seg['custo_medio_mensal'] = np.where(
        rec_seg['qtd_clientes'] > 0,
        rec_seg['custo_mensal_segmento'] / rec_seg['qtd_clientes'],
        0.0
    )
    rec_seg['resultado_mensal'] = rec_seg['receita_mensal_segmento'] - rec_seg['custo_mensal_segmento']
    rec_seg['resultado_medio_mensal'] = rec_seg['ticket_medio_mensal'] - rec_seg['custo_medio_mensal']

    df_segmento = rec_seg[['regime_base', 'segmento', 'qtd_clientes',
                           'receita_segmento', 'receita_mensal_segmento',
                           'custo_mensal_segmento',
                           'ticket_medio_mensal', 'custo_medio_mensal',
                           'resultado_mensal', 'resultado_medio_mensal']].copy()
    df_segmento = df_segmento.sort_values(['regime_base', 'receita_segmento'], ascending=[True, False]).reset_index(drop=True)

    return df_regime, df_segmento

def custo_absorcao_duas_visoes(
    df_receitas: pd.DataFrame,
    df_despesas_rateado: pd.DataFrame,
    pesos_regime: Optional[Dict[str, float]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Gera quatro DataFrames:
       - Regime_Sem_Peso, Segmento_Sem_Peso, Regime_Com_Peso, Segmento_Com_Peso."""
    meses = MESES_ACUMULADOS if MESES_ACUMULADOS > 0 else 1
    df_core = df_receitas[df_receitas['tipo_receita'] == 'HONORÁRIOS'].copy()
    if df_core.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    reg_a, seg_a = _aplica_visao_sem_peso(df_core, df_despesas_rateado, meses)
    reg_b, seg_b = _aplica_visao_com_peso(df_core, df_despesas_rateado, meses, pesos_regime=pesos_regime)
    return reg_a, seg_a, reg_b, seg_b

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

# ============================================================================
# RELATÓRIO COMPLETO
# ============================================================================

def gera_relatorio_completo():
    log: List[str] = []
    try:
        DIRETORIO_BASE.mkdir(parents=True, exist_ok=True)

        # 1) Receitas (com remoção da receita de rateio-advocacia)
        df_receitas, abatimento_adm = carrega_receitas(ARQUIVO_ENTRADA, log)

        # 2) Despesas (sem abatimento aplicado ainda)
        df_despesas = carrega_despesas(ARQUIVO_ENTRADA, log)

        if df_receitas.empty and df_despesas.empty:
            log.append("✗ ERRO: Não há dados válidos em RECEITAS e DESPESAS.")
            return log

        # 2a) Aplica abatimento de advocacia nas ADM sem afetar pró-labores
        df_despesas_abatidas = redistribui_abatimento_admin_sem_afetar_prolabores(
            df_despesas, abatimento_adm, log
        )

        # ---------- CENÁRIO GERAL ----------
        # 3) Rateio trabalhista (mantido)
        df_desp_rateado_geral = pd.DataFrame()
        if not df_despesas_abatidas.empty:
            pesos = calcula_base_rateio(df_despesas_abatidas, log)
            df_desp_rateado_geral = aplica_rateio_proporcional(df_despesas_abatidas, pesos, log)

        # 4) Tickets e Top despesas (geral)
        df_ticket_geral = df_ticket_regime = df_ticket_reg_ativ = df_ticket_clientes = pd.DataFrame()
        if not df_receitas.empty:
            df_ticket_geral, df_ticket_regime, df_ticket_reg_ativ, df_ticket_clientes = gera_ticket_medio(df_receitas)

        df_top_desp, df_top_desp_admin = gera_top_despesas(df_despesas_abatidas, TOP_N_DESPESAS) if not df_despesas_abatidas.empty else (pd.DataFrame(), pd.DataFrame())

        # 5) Duas visões de custo por absorção e resultados (geral)
        (df_regime_sem_peso,
         df_seg_sem_peso,
         df_regime_com_peso,
         df_seg_com_peso) = custo_absorcao_duas_visoes(df_receitas, df_desp_rateado_geral, pesos_regime=PESOS_COMPLEXIDADE_REGIME) if not df_receitas.empty else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        # ---------- CENÁRIO SEM PRÓ-LABORE ----------
        df_desp_sem_prolab, total_retiradas = remover_prolabores(df_despesas_abatidas)
        df_desp_rateado_sem_prolab = pd.DataFrame()
        if not df_desp_sem_prolab.empty:
            pesos2 = calcula_base_rateio(df_desp_sem_prolab, log)
            df_desp_rateado_sem_prolab = aplica_rateio_proporcional(df_desp_sem_prolab, pesos2, log)

        # Duas visões (sem pró-labore)
        (df_regime_sem_peso__noPL,
         df_seg_sem_peso__noPL,
         df_regime_com_peso__noPL,
         df_seg_com_peso__noPL) = custo_absorcao_duas_visoes(df_receitas, df_desp_rateado_sem_prolab, pesos_regime=PESOS_COMPLEXIDADE_REGIME) if not df_receitas.empty else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        # % retirada sobre resultado líquido (receitas válidas - despesas sem pró-labore)
        resultado_liq_sem_pl, total_retiradas_val, perc_retirada = percentual_retirada_sobre_resultado(
            df_receitas_validas=df_receitas,
            df_desp_sem_prolab=df_desp_sem_prolab,
            total_retiradas=total_retiradas
        )

        resumo_retirada = pd.DataFrame([{
            "receitas_validas": df_receitas['receita'].sum(),
            "despesas_sem_prolabore": df_desp_sem_prolab['valor'].sum(),
            "resultado_liquido_sem_prolabore": resultado_liq_sem_pl,
            "retiradas_prolabore_socios": total_retiradas_val,
            "percentual_retirada_sobre_resultado_%": perc_retirada,
            "abatimento_rateio_advocacia_aplicado": -abs(abatimento_adm),
            "ALFA_VOLUME": ALFA_VOLUME,
            "BETA_TICKET": BETA_TICKET
        }])

        # ---------- GRAVAÇÃO ----------
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl') as writer:
            # Bases
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
            if not df_ticket_geral.empty:
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
            if not df_regime_sem_peso.empty:
                _safe_to_excel(df_regime_sem_peso, writer, 'Resultado_Regime_Sem_Peso')
            if not df_seg_sem_peso.empty:
                _safe_to_excel(df_seg_sem_peso, writer, 'Resultado_Segmento_Sem_Peso')
            if not df_regime_com_peso.empty:
                _safe_to_excel(df_regime_com_peso, writer, 'Resultado_Regime_Com_Peso')
            if not df_seg_com_peso.empty:
                _safe_to_excel(df_seg_com_peso, writer, 'Resultado_Segmento_Com_Peso')

            # Resultado por regime/segmento - CENÁRIO SEM PRÓ-LABORE
            if not df_regime_sem_peso__noPL.empty:
                _safe_to_excel(df_regime_sem_peso__noPL, writer, 'Res_Regime_Sem_Peso_Sem_PL')
            if not df_seg_sem_peso__noPL.empty:
                _safe_to_excel(df_seg_sem_peso__noPL, writer, 'Res_Segmento_Sem_Peso_Sem_PL')
            if not df_regime_com_peso__noPL.empty:
                _safe_to_excel(df_regime_com_peso__noPL, writer, 'Res_Regime_Com_Peso_Sem_PL')
            if not df_seg_com_peso__noPL.empty:
                _safe_to_excel(df_seg_com_peso__noPL, writer, 'Res_Segmento_Com_Peso_Sem_PL')

            # Resumo de retirada
            _safe_to_excel(resumo_retirada, writer, 'Resumo_Retirada')

            # Log como aba
            _safe_to_excel(pd.DataFrame({"log": log}), writer, 'Log')

        # Log em arquivo texto
        try:
            with open(ARQUIVO_LOG, "w", encoding="utf-8") as f:
                f.write("\n".join(_sanitize_excel_text(l) for l in log))
        except Exception as e:
            log.append(f"⚠ AVISO: Falha ao gravar arquivo de log: {e}")

        log.append("\n✓ Relatório gerado com sucesso:")
        log.append(f"  • Arquivo: {ARQUIVO_SAIDA}")
        return log

    except Exception as e:
        log.append(f"✗ ERRO FATAL: {e}")
        return log

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    logs = gera_relatorio_completo()
    print("\n".join(logs))
