import pandas as pd
from zeep import Client
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

# === CONFIGURAÇÕES DO GOOGLE ===
SERVICE_ACCOUNT_FILE = 'credenciais.json'
SPREADSHEET_ID = '13_q2iGwqjpyY6JaCREiaJdeSqF9ZyRb7wvzGp9XcKNQ'
SHEET_NAME = 'ConsultarOcorrencias'

# === CONFIGURAÇÕES DO WEBSERVICE ===
params_base = {
    'USUARIO_WS': os.getenv('USUARIO_WS'),
    'SENHA_WS': os.getenv('SENHA_WS'),

    'TIPO_DATA': 'data_abertura',
    'RETORNO': 'json',
    'CAMPOS': (
        'data_abertura,prioridade_desc,numero,idcamposvariaveis_572,idocorrencia_parent,'
        'cliente_nome,aberto_por,descricao,area,oco_status,operador_responsavel,'
        'sla_resposta,sla_resp_horas,sla_solucao,sla_solucao_horas,idade_oc,tempo_dependencia_user,nome_projeto,'
        'problema,hora_ultima_modificacao,contato_email,data_fechamento,'
        'horas_lancadas,stat_cnt_16,stat_cnt_100,stat_cnt_5017,oco_status_simples,vencimento_sla_solucao'
    ),
    'CODIGO_AUXILIAR_CLIENTE': '',
}

# === ESCOPOS DO GOOGLE ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# === AUTENTICAÇÃO GOOGLE ===
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheets_service = build('sheets', 'v4', credentials=credentials)

# === FUNÇÃO PARA EXTRAIR DATA/HORA REMOVENDO TAGS HTML ===
def extrair_data_hora(html_str):
    if not isinstance(html_str, str):
        return ''
    texto_limpo = re.sub(r'<.*?>', '', html_str)
    return texto_limpo.strip().split()[0]

# === FUNÇÃO PARA FORMATAR DATAS ===
def formatar_data(valor):
    try:
        data = pd.to_datetime(valor, dayfirst=True)
        return data.strftime('%Y-%m-%d')
    except:
        return ''

# === FUNÇÃO PARA GERAR INTERVALOS MENSAIS ===
def gerar_intervalos_mensais(data_inicio, data_fim):
    intervalos = []
    atual = data_inicio.replace(day=1)
    while atual <= data_fim:
        proximo = (atual + relativedelta(months=1)).replace(day=1)
        fim_mes = proximo - relativedelta(seconds=1)
        fim_mes = min(fim_mes, data_fim)
        intervalos.append((
            atual.strftime('%Y-%m-%d 00:00:00'),
            fim_mes.strftime('%Y-%m-%d 23:59:59')
        ))
        atual = proximo
    return intervalos

# === CHAMADA DA API MÊS A MÊS ===
client = Client(wsdl_url)
data_inicio = datetime(2024, 1, 1)
data_fim = datetime.now()
intervalos = gerar_intervalos_mensais(data_inicio, data_fim)

todos_dados = []
for ini, fim in intervalos:
    print(f"🔄 Consultando período: {ini} até {fim}")
    params = params_base.copy()
    params['DATA_INI'] = ini
    params['DATA_FIM'] = fim
    response = client.service.ConsultarOcorrencias(**params)
    dados_mes = json.loads(response) if isinstance(response, str) else response
    todos_dados.extend(dados_mes)

df = pd.DataFrame(todos_dados)

df['area'] = df['area'].str.strip()
df['oco_status_simples'] = df['oco_status_simples'].str.strip()

df = df[
    df['area'].isin([
        'Em desenvolvimento ABAP, PI, WF, WD, .NET',
        'EC para ECP (colaboradores)',
        'EC para Enterprise SQL/SAP IBS (colaboradores)',
        'EC para WFS',
        'ECP para ADP',
        'ECP para EC (CIPA, Demais estabilidades)',
        'ECP para Enterprise SQL (ficha financeira)',
        'ECP para SAP IBS (contábil)',
        'ECP para Senior (férias)',
        'ECP para SOC (Unidade, Setor, Cargo, Hierarquia, M',
        'ECP para Vacation Control (contingente)',
        'GDP para EC (onboarding)',
        'Integração ALE',
        'Senior para ECP (ausências)',
        'Senior para ECP (fechamento ponto)',
        'SOC para ECP (Atestados, CIPA)',
        'WFS para ADP',
        'WFS para ECP',
        'WFS para Senior',
        'SFSF Integrations - EC Payroll, Boomi/SCI, API'
    ]) & 
    (~df['oco_status_simples'].isin(['Encerrada', 'Ocorrência Cancelada']))
]

# === FORMATAÇÃO E LIMPEZA DE COLUNAS ===
if 'data_abertura' in df.columns:
    df['data_abertura'] = df['data_abertura'].apply(formatar_data)

for col in ['vencimento_sla_solucao', 'sla_solucao_horas', 'sla_solucao', 'sla_resp_horas', 'sla_resposta']:
    if col in df.columns:
        df[col] = df[col].apply(extrair_data_hora)

if 'vencimento_sla_solucao' in df.columns:
    df['vencimento_sla_solucao'] = df['vencimento_sla_solucao'].apply(formatar_data)

# === REORGANIZAR E RENOMEAR COLUNAS ===
df = df[[ 
    "data_abertura","prioridade_desc","numero","idcamposvariaveis_572","idocorrencia_parent",
    "cliente_nome","aberto_por","descricao","area","oco_status","operador_responsavel",
    "sla_resposta","sla_resp_horas","sla_solucao","sla_solucao_horas","idade_oc","tempo_dependencia_user",
    "nome_projeto","problema","hora_ultima_modificacao","contato_email","data_fechamento",
    "horas_lancadas","stat_cnt_16","stat_cnt_100","stat_cnt_5017", "oco_status_simples","vencimento_sla_solucao"
]]

df.rename(columns={
    "data_abertura": "Data/Hora abertura",
    "prioridade_desc": "Prioridade",
    "numero": "N.º",
    "idcamposvariaveis_572": "Código Sistema de Chamados do Cliente",
    "idocorrencia_parent": "OC Pai : N.º",
    "cliente_nome": "Cliente",
    "aberto_por": "Aberto por",
    "descricao": "Descrição",
    "area": "Divisão",
    "oco_status": "Status",
    "operador_responsavel": "Operador responsável",
    "sla_resposta": "SLA de resposta",
    "sla_resp_horas": "Status do SLA de resposta",
    "sla_solucao": "SLA de solução",
    "sla_solucao_horas": "Status do SLA de solução",
    "idade_oc": "Idade da Ocorrência",
    "tempo_dependencia_user": "Tempo de dependência do usuario",
    "nome_projeto": "Projeto",
    "problema": "Solicitação",
    "hora_ultima_modificacao": "Data/Hora da Última modificação",
    "contato_email": "Email do Contato do Cliente",
    "data_fechamento": "Data/hora de encerramento",
    "horas_lancadas": "Horas Lançadas (em minutos)",
    "stat_cnt_16": "Cnt. Status : CLIENTE - Aguardando Retorno",
    "stat_cnt_100": "Cnt. Status : INTELLIGENZA - Feedback Retornado",
    "stat_cnt_5017": "Cnt. Status : INTELLIGENZA - Feedback retornado do cliente",
    "oco_status_simples": "Status (sem tempo decorrido)",
    "vencimento_sla_solucao": "Data de Vencimento do SLA de Solução"
}, inplace=True)

# === TRATAR VALORES AUSENTES ===
df = df.fillna('')

# === PREPARAR E ENVIAR PARA O GOOGLE SHEETS ===
values = [df.columns.tolist()] + df.values.tolist()

sheets_service.spreadsheets().values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range=SHEET_NAME
).execute()

sheets_service.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=SHEET_NAME + '!A1',
    valueInputOption='RAW',
    body={'values': values}
).execute()

print("✅ Planilha atualizada com sucesso!")
