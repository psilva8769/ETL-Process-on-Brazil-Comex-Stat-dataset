import os
import pandas as pd
import numpy as np

# Caminho dos arquivos que serão utilizados para formatação. (Especificar com extensão)
# Insira os caminhos entre as aspas
ARQUIVO_IMP = r'D:\IMP_2022.csv'
ARQUIVO_EXP = r'D:\EXP_2022.csv'

# Pasta onde os arquivos serão salvos.
DIRETORIO_DE_EXPORTACAO = r'C:\Users\Eckesaht\Desktop\as'




imp = pd.read_csv(ARQUIVO_IMP, sep=';')
exp = pd.read_csv(ARQUIVO_EXP, sep=';')
os.makedirs(DIRETORIO_DE_EXPORTACAO, exist_ok=True)

# Renomear a coluna 'CO_NCM''
MAPEAMENTO = {'CO_NCM': 'NCM'}
imp = imp.rename(columns=MAPEAMENTO)
exp = exp.rename(columns=MAPEAMENTO)

# Descarta colunas não desejadas.
COLUNAS_DESEJADAS = ['NCM', 'VL_FOB', 'CO_MES', 'SG_UF_NCM']
COLUNAS_REMOVIDAS_imp = [colunas for colunas in imp.columns if colunas not in COLUNAS_DESEJADAS]
COLUNAS_REMOVIDAS_exp = [colunas for colunas in exp.columns if colunas not in COLUNAS_DESEJADAS]
imp = imp.drop(columns=COLUNAS_REMOVIDAS_imp)
exp = exp.drop(columns=COLUNAS_REMOVIDAS_exp)

# Transforma os valores da coluna 'SG_UF_NCM' em valores únicos, evitando repetições por mês.
ufs_imp = imp['SG_UF_NCM'].unique()
ufs_exp = exp['SG_UF_NCM'].unique()
UFS = set(ufs_imp).union(ufs_exp)

# Transforma a coluna 'CO_MES' em colunas represetando os meses.
imp['CO_MES'] = imp['CO_MES'].replace({
    1: 'Imp_jan',
    2: 'Imp_fev',
    3: 'Imp_mar',
    4: 'Imp_abr',
    5: 'Imp_mai',
    6: 'Imp_jun',
    7: 'Imp_jul',
    8: 'Imp_ago',
    9: 'Imp_set',
    10: 'Imp_out',
    11: 'Imp_nov',
    12: 'Imp_dez'
})

exp['CO_MES'] = exp['CO_MES'].replace({
    1: 'Exp_jan',
    2: 'Exp_fev',
    3: 'Exp_mar',
    4: 'Exp_abr',
    5: 'Exp_mai',
    6: 'Exp_jun',
    7: 'Exp_jul',
    8: 'Exp_ago',
    9: 'Exp_set',
    10: 'Exp_out',
    11: 'Exp_nov',
    12: 'Exp_dez'
})

# Transforma os meses em colunas.
for uf in UFS:
    uf_filtrada_imp = imp[imp['SG_UF_NCM'] == uf]
    uf_filtrada_exp = exp[exp['SG_UF_NCM'] == uf]

    uf_filtrada_imp = uf_filtrada_imp.pivot_table(index=['NCM', 'SG_UF_NCM'], columns='CO_MES', values='VL_FOB', aggfunc='sum').reset_index()
    uf_filtrada_exp = uf_filtrada_exp.pivot_table(index=['NCM', 'SG_UF_NCM'], columns='CO_MES', values='VL_FOB', aggfunc='sum').reset_index()

    specified_order = ['NCM', 'Imp_jan', 'Imp_fev', 'Imp_mar', 'Imp_abr', 'Imp_mai', 'Imp_jun', 'Imp_jul', 'Imp_ago', 'Imp_set', 'Imp_out', 'Imp_nov', 'Imp_dez']
    uf_filtrada_imp = uf_filtrada_imp.reindex(columns=specified_order)

    specified_order = ['NCM', 'Exp_jan', 'Exp_fev', 'Exp_mar', 'Exp_abr', 'Exp_mai', 'Exp_jun', 'Exp_jul', 'Exp_ago', 'Exp_set', 'Exp_out', 'Exp_nov', 'Exp_dez']
    uf_filtrada_exp = uf_filtrada_exp.reindex(columns=specified_order)

    # Mescla os dois arquivos num único arquivo.
    planilha_mesclada = pd.merge(uf_filtrada_imp, uf_filtrada_exp, on=['NCM'], how='outer')

    # Organiza as colunas na ordem certa.
    ordem_certa = ['NCM', 'Exp_jan', 'Imp_jan', 'Net_jan', 'Exp_fev', 'Imp_fev', 'Net_fev', 'Exp_mar', 'Imp_mar', 'Net_mar', 'Exp_abr', 'Imp_abr', 'Net_abr', 'Exp_mai', 'Imp_mai', 'Net_mai', 'Exp_jun', 'Imp_jun', 'Net_jun', 'Exp_jul', 'Imp_jul', 'Net_jul', 'Exp_ago', 'Imp_ago', 'Net_ago', 'Exp_set', 'Imp_set', 'Net_set', 'Exp_out', 'Imp_out', 'Net_out', 'Exp_nov', 'Imp_nov', 'Net_nov', 'Exp_dez', 'Imp_dez', 'Net_dez', 'Exp_2022', 'Imp_2022', 'Net_2022']
    planilha_mesclada = planilha_mesclada.reindex(columns=ordem_certa)

    # Atribui os nomes das colunas que serão somadas às três constantes.
    meses = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
    for mes in meses:
        COLUNA_NET = 'Net_' + mes
        COLUNA_EXP = 'Exp_' + mes
        COLUNA_IMP = 'Imp_' + mes

        # Verifica se alguma célula está vazia.
        EXP_NULO = planilha_mesclada[COLUNA_EXP].isnull()
        IMP_NULO = planilha_mesclada[COLUNA_IMP].isnull()
        EXP_OU_IMP_NULO = np.logical_or(EXP_NULO, IMP_NULO)

        # Realiza o cálculo de EXP - IMP e atribui o resultado à célula NET.
        planilha_mesclada[COLUNA_NET] = np.where(EXP_NULO, -planilha_mesclada[COLUNA_IMP], np.where(IMP_NULO, planilha_mesclada[COLUNA_EXP], planilha_mesclada[COLUNA_EXP] - planilha_mesclada[COLUNA_IMP]))

    # Soma todos os números das células e atribui os resultados às suas últimas linhas respectivamente.
    planilha_mesclada['Exp_2022'] = planilha_mesclada.filter(like='Exp_').sum(axis=1)
    planilha_mesclada['Imp_2022'] = planilha_mesclada.filter(like='Imp_').sum(axis=1)
    planilha_mesclada['Net_2022'] = planilha_mesclada.filter(like='Net_').sum(axis=1)

    # Exporta o arquivo para o formato CSV.
    CAMINHO = os.path.join(DIRETORIO_DE_EXPORTACAO, f'{uf}.csv')
    planilha_mesclada.to_csv(CAMINHO, index=False, sep=',')