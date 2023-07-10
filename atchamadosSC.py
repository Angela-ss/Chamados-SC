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

# PARA TESTES:
# SERVER_NAME = 'SOALV3HMLCL01,1438'
# DATABASE_NAME = 'dbEAcesso_daily'


conexao = pyodbc.connect("""
        Driver={{SQL Server Native Client 11.0}};
        Server={0};
        Database={1};
        Trusted_Connection=yes;""".format(SERVER_NAME, DATABASE_NAME))

cursor_sql = conexao.cursor()
cursor_sql.fast_executemany = True

# PARA LER A PLANILHA

df = pd.read_excel(
    f"C:\\Users\\agsilva11\\OneDrive - Stefanini\\Documents\\Particular\\"
    f"Atualizar_TBLCHAMADOS\\Diário_Chamados {dia}-{mes}-{now.year}.xlsx")
df['Resolver em'] = df['Resolver em'].fillna(value=pd.to_datetime('01-01-1900 00:00:00'))
df['DATA RESOLUÇÃO'] = df['DATA RESOLUÇÃO'].fillna(value=pd.to_datetime('01-01-1900 00:00:00'))

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
cursor_sql.execute('DELETE TBLCHAMADOSPESQUISA')
cursor_sql.execute('DELETE TBLCHAMADOSREABERTOS')
conexao.commit()

# LINHA DE TABELA E INSERINDO NO BANCO DE DADOS

# TBLCHAMADOS
lista = []
for index, row in df.iterrows():
    dtcriacao = row['Data de criação']
    dtprazo = row['Resolver em']
    dtatualizacao = row['Atualizado']
    dtresolucao = row['DATA RESOLUÇÃO']
    solicitante = str(row['Solicitante'])
    email = str(row['Email do solicitante'])
    cpf = str(row['CPF do solicitante'])
    descricao = str(row['Descrição'])
    detalhes = str(row['Detalhes'])
    lista.append([row['ID do chamado'], row['Status'], row['Atribuído'], row['Categorização'], row['Motivo'], dtcriacao,
                       dtprazo, dtresolucao, dtatualizacao, row['Status do SLA'], row['Prioridade'], row['Grupo atribuído'],
                       row['Tipo de Ticket'], solicitante, email, cpf, descricao, detalhes])

cursor_sql.executemany("""
INSERT INTO TBLCHAMADOS (ID, STATUS, ATRIBUID, CATEGORIZACAO, MOTIVO, DTCRIACAO, DTPRAZO, DTRESOLUCAO, DTATUALIZACAO, STATUSSLA, PRIORIDADE, GRUPOATRIBUIDO, TIPO, SOLICITANTE, EMAIL, CPF, DESCRICAO, DETALHES)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
""", lista)
lista.clear()

# TBLCHAMADOSPESQUISA
for index, row in df2.iterrows():
    dtenvio = row['Data Envio']
    dtresposta = row['Data Resposta']
    analista = row['ANALISTA']

    lista.append([row['TICKET'], row['QUEM_RESPONDEU'], dtenvio, dtresposta, row['PERGUNTA'], row['RESPOSTA'],
                        row['GRUPO_SOLUCIONADOR'], analista])

cursor_sql.executemany("""
    INSERT INTO TBLCHAMADOSPESQUISA (ID, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", lista)

lista.clear()

# TBLCHAMADOSREABERTOS
for index, row in df3.iterrows():
    dtabertura = row['Data Abertura do chamado']
    dtencerramento = row['Data encerramento do chamado']
    dtacao = row['Data da ação']
    lista.append([row['TicketID'], dtabertura, dtencerramento, row['Status Atual'], row['Categorização'], dtacao, row['Ação'], row['Resolvido pelo grupo'], row['Resolvido por']])

cursor_sql.executemany("""
    INSERT INTO TBLCHAMADOSREABERTOS (ID, DTABERTURA, DTENCERRAMENTO, STATUS, CATEGORIZACAO, DTACAO, ACAO, GRUPO, ANALISTA)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", lista)

cursor_sql.execute("""
WITH ID AS (
    SELECT
        ID,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID) AS RowNumber
    FROM
        TBLCHAMADOSREABERTOS
)
DELETE FROM ID WHERE RowNumber > 1;
""")

# ATUALIZANDO A TBLCHAMADOSPESQUISA COM A PESQUISA NOVA
cursor_sql.execute("""
CREATE TABLE #PESQUISADASH
(
TICKET VARCHAR(10)
,QUEMRESPONDEU VARCHAR(65)
,DTENVIO DATETIME
,DTRESPOSTA DATETIME
,PERGUNTA VARCHAR(90)
,RESPOSTA VARCHAR(MAX)
,GRUPO VARCHAR(50)
,ANALISTA VARCHAR(65)
)

BEGIN

DECLARE

 @PERGUNTA VARCHAR(90)
,@PERGUNTA2 VARCHAR(90)
,@PERGUNTA3 VARCHAR(90)



SET @PERGUNTA = ('Qual o nível de satisfação em relação a entrega dos serviços deste chamado?')
SET @PERGUNTA2 = ('Qual é o nível de satisfação com a atuação do PO ou Analista?')
SET @PERGUNTA3 = ('Deixe um comentário, crítica, sugestão ou elogio a respeito destes nossos atendimentos.')


	IF EXISTS (
	SELECT TICKET
	FROM #PESQUISADASH
	)

		BEGIN
		RETURN
		END

	ELSE

	INSERT INTO #PESQUISADASH (TICKET, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
	SELECT A.TICKET, D.SOLICITANTE, A.DTCRIACAOTICKET, A.DTRESPOSTA, @PERGUNTA, C.VLDOMINIO, D.GRUPOATRIBUIDO, D.ATRIBUID
	FROM TBLPESQUISASATISFACAO AS A
	LEFT JOIN TBLDOMINIO AS B ON B.IDDOMINIO = A.IDNIVELSATISFACAOANALISTA
	LEFT JOIN TBLDOMINIO AS C ON C.IDDOMINIO = A.IDNIVELSATISFACAOSERVICO
	INNER JOIN TBLCHAMADOS D ON D.ID = A.TICKET
	WHERE A.LGUSUARIO NOT IN ('JOB')

	INSERT INTO #PESQUISADASH (TICKET, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
	SELECT A.TICKET, D.SOLICITANTE, A.DTCRIACAOTICKET, A.DTRESPOSTA, @PERGUNTA2, B.VLDOMINIO, D.GRUPOATRIBUIDO, D.ATRIBUID
	FROM TBLPESQUISASATISFACAO AS A
	LEFT JOIN TBLDOMINIO AS B ON B.IDDOMINIO = A.IDNIVELSATISFACAOANALISTA
	LEFT JOIN TBLDOMINIO AS C ON C.IDDOMINIO = A.IDNIVELSATISFACAOSERVICO
	INNER JOIN TBLCHAMADOS D ON D.ID = A.TICKET
	WHERE A.LGUSUARIO NOT IN ('JOB')

	INSERT INTO #PESQUISADASH (TICKET, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
	SELECT A.TICKET, D.SOLICITANTE, A.DTCRIACAOTICKET, A.DTRESPOSTA, @PERGUNTA3, A.COMENTARIO, D.GRUPOATRIBUIDO, D.ATRIBUID
	FROM TBLPESQUISASATISFACAO AS A
	LEFT JOIN TBLDOMINIO AS B ON B.IDDOMINIO = A.IDNIVELSATISFACAOANALISTA
	LEFT JOIN TBLDOMINIO AS C ON C.IDDOMINIO = A.IDNIVELSATISFACAOSERVICO
	INNER JOIN TBLCHAMADOS D ON D.ID = A.TICKET
	WHERE A.LGUSUARIO NOT IN ('JOB')


END

INSERT INTO TBLCHAMADOSPESQUISA (ID, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA, RESPOSTA, GRUPO, ANALISTA)
SELECT TICKET, QUEMRESPONDEU, DTENVIO, DTRESPOSTA, PERGUNTA,
CASE
WHEN RESPOSTA = 'BOM'		THEN 'Bom'
WHEN RESPOSTA = 'OTIMO'		THEN 'Ótimo'
WHEN RESPOSTA = 'RUIM'		THEN 'Ruim'
WHEN RESPOSTA = 'REGULAR'	THEN 'Regular'
WHEN RESPOSTA = 'PESSIMO'	THEN 'Péssimo'
WHEN RESPOSTA = NULL		THEN 'undefined'
ELSE RESPOSTA
END AS RESPOSTA, GRUPO, ANALISTA
FROM #PESQUISADASH

DROP TABLE #PESQUISADASH
""")

# REMOVENDO OS DUPLICADOS

cursor_sql.execute("""
WITH ID AS (
    SELECT
        ID,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID) AS RowNumber
    FROM
        TBLCHAMADOSPESQUISA
)
DELETE FROM ID WHERE RowNumber > 3;
""")


conexao.commit()
cursor_sql.close()
conexao.close()

