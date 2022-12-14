import PyInstaller
import datetime

import pandas as pd
import pyodbc

import warnings

warnings.simplefilter("ignore")

now = datetime.datetime.now()

dia = 0
mes = 0

if now.day < 10:
    dia = "0" + str(now.day)
else:
    dia = now.day

if now.month < 10:
    mes = "0" + str(now.month)
else:
    mes = now.month

SERVER_NAME = 'SOALV3SQLPROD,1438'
DATABASE_NAME = 'dbEAcesso'

conexao = pyodbc.connect("""
        Driver={{SQL Server Native Client 11.0}};
        Server={0};
        Database={1};
        Trusted_Connection=yes;""".format(SERVER_NAME, DATABASE_NAME))

cursor_sql = conexao.cursor()
cursor_sql2 = conexao.cursor()
cursor_sql3 = conexao.cursor()

# PARA LER A PLANILHA

df = pd.read_excel(
    f"C:\\Users\\agsilva11\\OneDrive - Stefanini\\Documents\\Particular\\"
    f"Atualizar_TBLCHAMADOS\\Diário_Chamados {dia}-{mes}-{now.year}.xlsx")
df.drop(labels=["Solicitante", "Descrição", "Detalhes", "Organização do solicitante"], axis=1, inplace=True)
df['Resolver em'] = df['Resolver em'].fillna(value=pd.to_datetime('01-01-1900 00:00:00'))

df2 = pd.read_excel(
    f"C:\\Users\\agsilva11\\OneDrive - Stefanini\\Documents\\Particular\\"
    f"Atualizar_TBLCHAMADOS\\Diário_PesquisaDeSatisfacao {dia}-{mes}-{now.year}.xlsx")
df2['RESPOSTA'] = df2['RESPOSTA'].fillna('')

df3 = pd.read_excel(
    f"C:\\Users\\agsilva11\\OneDrive - Stefanini\\Documents\\Particular\\"
    f"Atualizar_TBLCHAMADOS\\Diário_ReaberturaChamados {dia}-{mes}-{now.year}.xlsx")
df3['Data encerramento do chamado'] = df3['Data encerramento do chamado'].fillna(
    value=pd.to_datetime('01-01-1900 00:00:00'))

# APAGAR REGISTROS NA TABELA
cursor_sql.execute('DELETE TBLCHAMADOS')
cursor_sql2.execute('DELETE TBLCHAMADOSPESQUISA')
cursor_sql3.execute('DELETE TBLCHAMADOSREABERTOS')
conexao.commit()

# LINHA DE TABELA E INSERINDO NO BANCO DE DADOS

# TBLCHAMADOS
for index, row in df.iterrows():
    dtcriacao = row['Data de criação']
    dtprazo = row['Resolver em']
    dtatualizacao = row['Atualizado']
    cursor_sql.execute("""
        INSERT INTO TBLCHAMADOS (ID, STATUS, ATRIBUID, CATEGORIZACAO, MOTIVO, DTCRIACAO, DTPRAZO, DTATUALIZACAO, STATUSSLA, PRIORIDADE, GRUPOATRIBUIDO, TIPO)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row['ID do chamado'], row['Status'], row['Atribuído'], row['Categorização'], row['Motivo'], dtcriacao,
                       dtprazo, dtatualizacao, row['Status do SLA'], row['Prioridade'], row['Grupo atribuído'],
                       row['Tipo de Ticket'])

# TBLCHAMADOSPESQUISA
for index, row in df2.iterrows():
    dtenvio = row['Data Envio']
    dtresposta = row['Data Resposta']
    analista = row['ANALISTA']
    cursor_sql2.execute("""
        INSERT INTO TBLCHAMADOSPESQUISA (ID, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, row['TICKET'], row['QUEM_RESPONDEU'], dtenvio, dtresposta, row['PERGUNTA'], row['RESPOSTA'],
                        row['GRUPO_SOLUCIONADOR'], analista)

# TBLCHAMADOSREABERTOS
for index, row in df3.iterrows():
    dtabertura = row['Data Abertura do chamado']
    dtencerramento = row['Data encerramento do chamado']
    dtacao = row['Data da ação']
    cursor_sql3.execute("""
        INSERT INTO TBLCHAMADOSREABERTOS (ID, DTABERTURA, DTENCERRAMENTO, STATUS, CATEGORIZACAO, DTACAO, ACAO, GRUPO, ANALISTA)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row['TicketID'], dtabertura, dtencerramento, row['Status Atual'], row['Categorização'], dtacao, row['Ação'],
                        row['Resolvido pelo grupo'], row['Resolvido por'])

conexao.commit()
cursor_sql.close()
cursor_sql2.close()
cursor_sql3.close()
conexao.close()

