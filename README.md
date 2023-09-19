# fortesbi

Fortesbi é um projeto para extração de dados de um banco de dados disponibilizado pela Fortes para visualização de dados no programa Power BI

A ideia desse código é gerar um arquivo final em formato .xlsx com todas as informações necessárias para visualização no Power BI

Para execução primeiramente precisa ter o arquivo .fdb do banco com a conexão odbc estabelecida em sua máquina local 

Em seguida configure o ambiente python para suporte às seguintes bibliotecas:

[pyodbc,
datetime,
pandas,
openpyxl]

Rode  script.py e obtenha o arquivo excel de saída
