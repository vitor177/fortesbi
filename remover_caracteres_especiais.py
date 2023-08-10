
import pandas as pd
import re
df10 = pd.read_excel('./FINAL.xlsx')

#print(df10)


df10.columns = ['EMPRESA', 'CODIGO', 'DATA', 'VALOR', 'HISTORICO', 'NATUREZA', 'ORIGEM', 'CATEGORIA', 'CENTRO DE CUSTO']
print(df10.columns)

#print(type(df10['OBS'][0]))

lista = []
for string in df10['HISTORICO']:
    string = bytes(str(string), 'utf-8')
    #print(type(string))
    string = string.decode('unicode-escape').encode().decode('utf-8')[2:-1:]


    lista.append(string)

df10['HISTORICO'] = lista



output_filename = "Final_vf.xlsx"


writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')


df10.to_excel(output_filename,index=False, engine='xlsxwriter')
