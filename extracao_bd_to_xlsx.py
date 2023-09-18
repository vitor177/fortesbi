import pyodbc
import datetime
import pandas as pd

connection_string = 'DSN=FORTESBI;'
conn = pyodbc.connect(connection_string)

cursor0 = conn.cursor()
cursor = conn.cursor()
cursor2 = conn.cursor()
cursor3 = conn.cursor()

data = datetime.datetime(2023,3,31)

cursor0.execute(f"SELECT EST.NOME, LAN.CODIGO, LAN.DATA, LAN.VALOR, LAN.HISTORICO, LAN.ORIGEM, LAN.NATUREZA, CRD.NOME, CRS.NOME FROM LAN LEFT JOIN EST ON LAN.EST_CODIGO=EST.CODIGO AND EST.EMP_CODIGO=LAN.EMP_CODIGO LEFT JOIN CRS ON LAN.CRS_CODIGO=CRS.CODIGO AND CRS.EMP_CODIGO=LAN.EMP_CODIGO LEFT JOIN CRD ON CRD.CODIGO=LAN.CRD_CODIGO AND CRD.EMP_CODIGO=LAN.EMP_CODIGO WHERE LAN.DATA > '{data}' AND LAN.NATUREZA IS NOT NULL")
cursor.execute(f"SELECT DISTINCT CRE.EMP_CODIGO, CRE.VALOR, CRD.NOME, CRS.NOME, CRE.DTEMISSAO, EST.NOME, CRE.CODIGO, CRE.OBS, CRE.VALOR, CRE.ISS, CRE.IRRF, CRE.INSS, CRE.PISCOFINSCSL, CRE.PIS, CRE.COFINS, CRE.CSL, CRE.VALOROUTRASRETENCOES FROM CRE INNER JOIN CRD ON CRE.CRD_CODIGO=CRD.CODIGO AND CRE.EMP_CODIGO=CRD.EMP_CODIGO INNER JOIN EST ON CRE.EST_CODIGO=EST.CODIGO AND EST.EMP_CODIGO=CRE.EMP_CODIGO INNER JOIN CRS ON CRS.EMP_CODIGO=CRE.EMP_CODIGO AND CRS.CODIGO=CRE.CRS_CODIGO")
cursor2.execute(f"SELECT EMP_CODIGO, VDR_CRE_CODIGO, VALOR FROM BVD")
cursor3.execute(f"SELECT DISTINCT CRE.EMP_CODIGO, CRE.VALOR, CRD.NOME, CRS.NOME, CRE.DTEMISSAO, EST.NOME, CRE.CODIGO, CRE.OBS, CRE.VALOR, CRE.ISS, CRE.IRRF, CRE.INSS, CRE.PISCOFINSCSL, CRE.PIS, CRE.COFINS, CRE.CSL, CRE.VALOROUTRASRETENCOES FROM CRE INNER JOIN CRD ON CRE.CRD_CODIGO=CRD.CODIGO AND CRE.EMP_CODIGO=CRD.EMP_CODIGO INNER JOIN EST ON CRE.EST_CODIGO=EST.CODIGO AND EST.EMP_CODIGO=CRE.EMP_CODIGO INNER JOIN CRS ON CRS.EMP_CODIGO=CRE.EMP_CODIGO AND CRS.CODIGO=CRE.CRS_CODIGO")


rows0 = cursor0.fetchall()
rows = cursor.fetchall()
rows2 = cursor2.fetchall()
rows3 =cursor3.fetchall()


columns_3 = [column[0] for column in cursor3.description]
df_3 = pd.DataFrame.from_records(rows3, columns=columns_3)

df_3.columns = ['EMPCODIGO','VALOR', 'NOMECRD', 'NOMECRS', 'DTEMISSAO', 'EMPRESA', 'CODIGO', 'OBS', 'VALOR2', 'ISS', 'IRRF', 'INSS',
       'PISCOFINSCSL', 'PIS', 'COFINS', 'CSL', 'VALOROUTRASRETENCOES']

insercoes3 = []

columns = [column[0] for column in cursor.description]
df = pd.DataFrame.from_records(rows, columns=columns)

insercoes = []

columns_group_by = [column[0] for column in cursor2.description]
df_group_by = pd.DataFrame.from_records(rows2, columns=columns_group_by)

columns_0 = [column[0] for column in cursor0.description]
df_0 = pd.DataFrame.from_records(rows0, columns=columns_0)

df.columns = ['EMPCODIGO','VALORBVDCRE', 'NOMECRD', 'NOMECRS', 'DATA', 'EMPRESA', 'CODIGO', 'OBS', 'VALOR', 'ISS', 'IRRF', 'INSS',
       'PISCOFINSCSL', 'PIS', 'COFINS', 'CSL', 'VALOROUTRASRETENCOES']

df_group_by.columns = ['EMPRESA','CODIGO', 'VALOR']

df_0.columns = ['EMPRESA', 'CODIGO', 'DATA', 'VALOR', 'HISTORICO', 'ORIGEM', 'NATUREZA', 'CRD_NOME', 'CRS_NOME']

insercoes0 = []

df['OBS'] = [bytes(str(string), 'utf-8').decode('unicode-escape').encode().decode('utf-8')[2:-1] for string in df['OBS']]
df_0['HISTORICO'] = [bytes(str(string), 'utf-8').decode('unicode-escape').encode().decode('utf-8')[2:-1] for string in df_0['HISTORICO']]


# Lançamentos da tabela CRE 
for indice,linha in df_3.iterrows():
    empresa = linha['EMPRESA']
    cre_codigo = linha['CODIGO']
    cre_data = linha['DTEMISSAO']
    valor = linha['VALOR']
    cre_historico = linha['OBS']
    cre_origem = "ORIGEM CRE"
    cre_natureza = "ORIGEM CRE"
    crd_nome = linha['NOMECRD']
    crs_nome = linha['NOMECRS']
    cod_empresa = linha['EMPCODIGO']
    insercoes3.append((empresa, cre_codigo, cre_data, valor, cre_historico, cre_origem, cre_natureza, crd_nome, crs_nome))
df_3 = pd.DataFrame(insercoes3)


# Lançamentos da tabela LAN
for indice, linha in df_0.iterrows():
    empresa_df0 = linha['EMPRESA']
    codigo_df0 = linha['CODIGO']
    data_df0 = linha['DATA']
    valor_df0 = linha['VALOR']
    historico_df0 = linha['HISTORICO']
    origem_df0 = linha['ORIGEM']
    natureza_df0 = linha['NATUREZA']
    crd_nome_df0 = linha['CRD_NOME']
    crs_nome_df0 = linha['CRS_NOME']

    if natureza_df0 == 'S':
        valor_df0 = valor_df0*(-1)

    insercoes0.append((empresa_df0, codigo_df0, data_df0, valor_df0, historico_df0, origem_df0, natureza_df0, crd_nome_df0, crs_nome_df0))

df_1 = pd.DataFrame(insercoes0)

############################################################

# Lançamentos para Receita
for indice, linha in df.iterrows():
    valor = linha['VALOR']
    crd_nome = linha['NOMECRD']
    crs_nome = linha['NOMECRS']
    empresa = linha['EMPRESA']
    lan_codigo = linha['CODIGO']
    lan_data = linha['DATA']
    lan_historico = linha['OBS']
    cod_empresa = linha['EMPCODIGO']
    somaImpostos = linha['ISS'] +  linha['IRRF'] + linha['COFINS'] + linha['CSL'] + linha['INSS'] + linha['PIS']

    lista_cdgs = []

    # BVD
    for indice2, linha2 in df_group_by.iterrows():
        lan_origem = "DFC ORIGEM"
        lan_natureza = "DFC ORIGEM"

        if lan_codigo == linha2['CODIGO'] and cod_empresa == linha2['EMPRESA']:

            cdg = linha2['VALOR']/((valor-somaImpostos)/valor) ##   valorLiquido      /    (valorBruto-SomaImpostos)/ValorBruto

            if linha2['CODIGO'] =="20230400006" and linha2['EMPRESA'] == "0003":
                print("valor LIQUIDO: ", linha2['VALOR'])
                print("VALOR DIVISAO CDG: ", cdg)
                print("valor BRUTO: ", valor)
                print("SomaImpostos: ", somaImpostos)
                print("CRD NOME: ", crd_nome)
                print("ISS:  ", linha['ISS'])
                print("IRRF: ", linha['IRRF'])
                print("COFINS: ", linha['COFINS'])
                print("CSL: ", linha['CSL'])
                print("INSS: ", linha['INSS'])
                print("PIS: ", linha['PIS'])

            lista_nomes = ['PRESTAÇÃO DE SERVIÇO DE ENGENHARIA - MANUTENÇÃO',
                                    'RECEITA - SERVIÇOS REFORMA PREDIAL',
                                    'RECEITA - SERVIÇOS DE MANUTENCAO',
                                    'PRESTAÇÃO DE SERVIÇO DE MANUT - ESGOT SANITÁRIO',
                                    'PRESTAÇÃO DE SERVIÇO DE MAO DE OBRA - ENGENHARIA',
                                    'RECEITA - SERVIÇOS DE MANUTENCAO - BRUTO']
            
            for i in lista_nomes:
                if crd_nome == i:
                    insercoes.append((empresa, lan_codigo, lan_data, cdg, lan_historico, lan_origem, lan_natureza, crd_nome + " - BRUTO", crs_nome)) 
            lista_cdgs.append(cdg)


############################

# Lançamentos para Impostos da DFC
for indice, linha in df.iterrows():
    valor = linha['VALOR']
    valorBruto = valor
    crd_nome = linha['NOMECRD']
    crs_nome = linha['NOMECRS']
    empresa = linha['EMPRESA']
    lan_codigo = linha['CODIGO']
    lan_data = linha['DATA']
    lan_historico = linha['OBS']
    cod_empresa = linha['EMPCODIGO']
    somaImpostos = linha['ISS'] +  linha['IRRF'] + linha['COFINS'] + linha['CSL'] + linha['INSS'] + linha['PIS']


    # BVD
    for indice2, linha2 in df_group_by.iterrows():
        lan_origem = "DFC ORIGEM"
        if lan_codigo == linha2['CODIGO'] and cod_empresa == linha2['EMPRESA']:
            cdg = linha2['VALOR'] / ((valorBruto - somaImpostos) / valorBruto) if valorBruto != somaImpostos else 0.0
            if linha['ISS'] == 0 and linha['IRRF'] == 0 and linha['PISCOFINSCSL'] == 0 and linha['COFINS'] and linha['CSL'] == 0 and linha['INSS'] == 0 and linha['VALOROUTRASRETENCOES'] == 0:
                pass
            else:
                lan_historico = linha['OBS']
                if linha['ISS'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"            
                    linha_CRD_NOME = "ISS - S/FATURAMENTO"
                    valor = linha['ISS']
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['IRRF'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['IRRF']
                    linha_CRD_NOME = "IRRF - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['PIS'] !=  0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['PIS']
                    linha_CRD_NOME ="PIS - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['COFINS'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['COFINS']
                    linha_CRD_NOME = "COFINS - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['CSL'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['CSL']
                    linha_CRD_NOME = "CSL - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['INSS'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['INSS']
                    linha_CRD_NOME = "INSS - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                if linha['VALOROUTRASRETENCOES'] != 0:
                    lan_origem = "R"
                    lan_natureza = "S"
                    valor = linha['VALOROUTRASRETENCOES']
                    linha_CRD_NOME = "VALOROUTRASRETENCOES - S/FATURAMENTO"
                    insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*linha2['VALOR']*-1, lan_historico, "DFC ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
                



#################################################################################################


# Lançamentos para impostos da DRE
for indice, linha in df.iterrows():
    valor = linha['VALOR']
    valorBruto = valor
    crd_nome = linha['NOMECRD']
    crs_nome = linha['NOMECRS']
    empresa = linha['EMPRESA']
    lan_codigo = linha['CODIGO']
    lan_data = linha['DATA']
    lan_historico = linha['OBS']
    cod_empresa = linha['EMPCODIGO']
    somaImpostos = linha['ISS'] +  linha['IRRF'] + linha['COFINS'] + linha['CSL'] + linha['INSS'] + linha['PIS']



    if linha['ISS'] == 0 and linha['IRRF'] == 0 and linha['PISCOFINSCSL'] == 0 and linha['COFINS'] and linha['CSL'] == 0 and linha['INSS'] == 0 and linha['VALOROUTRASRETENCOES'] == 0:
        pass
    else:
        lan_historico = linha['OBS']
        if linha['ISS'] != 0:
            lan_origem = "R"
            lan_natureza = "S"            
            linha_CRD_NOME = "ISS - S/FATURAMENTO"
            valor = linha['ISS']*-1
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
        if linha['IRRF'] != 0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['IRRF']*-1
            linha_CRD_NOME = "IRRF - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
        if linha['PIS'] !=  0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['PIS']*-1
            linha_CRD_NOME ="PIS - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
        if linha['COFINS'] != 0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['COFINS']*-1
            linha_CRD_NOME = "COFINS - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
        if linha['CSL'] != 0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['CSL']*-1
            linha_CRD_NOME = "CSL - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))
        if linha['INSS'] != 0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['INSS']*-1
            linha_CRD_NOME = "INSS - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))

        if linha['VALOROUTRASRETENCOES'] != 0:
            lan_origem = "R"
            lan_natureza = "S"
            valor = linha['VALOROUTRASRETENCOES']*-1
            linha_CRD_NOME = "VALOROUTRASRETENCOES - S/FATURAMENTO"
            insercoes.append((empresa, lan_codigo, lan_data, (valor/valorBruto)*valorBruto*-1, lan_historico, "DRE ORIGEM", lan_natureza, linha_CRD_NOME, crs_nome))


df_2 = pd.DataFrame(insercoes)
df_final = pd.concat([df_1, df_2, df_3])
df_final.columns = ['EMPRESA', 'CODIGO', 'DATA', 'VALOR', 'HISTORICO', 'ORIGEM', 'NATUREZA', 'CRD', 'CRS']
df_final.to_excel('FINAL.xlsx', index=False)
cursor.close()
conn.close()

import pandas as pd
import re
df10 = pd.read_excel('./FINAL.xlsx')


df10.columns = ['EMPRESA', 'CODIGO', 'DATA', 'VALOR', 'HISTORICO', 'ORIGEM', 'NATUREZA', 'CATEGORIA', 'CENTRO DE CUSTO']


lista = []
for string in df10['HISTORICO']:
    string = bytes(str(string), 'utf-8')
    #print(type(string))
    string = string.decode('unicode-escape').encode().decode('utf-8')


    lista.append(string)

df10['HISTORICO'] = lista


output_filename = "./Final_vf.xlsx"


writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')


df10.to_excel(output_filename,index=False, engine='xlsxwriter')

import os

arquivo_a_apagar = 'FINAL.xlsx'  # Substitua pelo nome do arquivo que deseja apagar

if os.path.isfile(arquivo_a_apagar):
    os.unlink(arquivo_a_apagar)

print("Terminou")
