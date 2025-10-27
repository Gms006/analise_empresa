# analise_empresa

## v4.0 - Modernização do Modelo de Custos

### Removido
- ❌ Aba "Resultado_Regime_Sem_Peso"
- ❌ Aba "Resultado_Segmento_Sem_Peso"
- ❌ KPI "Resultado Total (Sem Peso)"
- ❌ Tab "Sem Peso" na interface
- ❌ Seção "Comparativo Sem Peso × Com Peso"

### Adicionado
- ✅ Peso único: `peso_base × qtd_clientes`
- ✅ Aba "DRE_Simplificada"
- ✅ Campos em Resumo_Retirada: retiradas_total, %_retiradas_sobre_receita, %_retiradas_sobre_resultado
- ✅ Tratamento pró-labore como retirada (não custo)

### Modificado
- 🔄 Abatimento advocacia não afeta pró-labores
- 🔄 Apropriação de custos sempre usa peso
- 🔄 KPI renomeado: "Resultado Total (Com Peso)" → "Resultado Total"
