# === IMPORTS ===
import os
import re
import json
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from requests import Session
from requests.adapters import HTTPAdapter
from requests.models import Response

from zeep import Client
from zeep.transports import Transport

from google.oauth2 import service_account
from googleapiclient.discovery import build

# === GOOGLE CONFIG ===
SERVICE_ACCOUNT_FILE = 'credenciais.json'
SPREADSHEET_ID = '13_q2iGwqjpyY6JaCREiaJdeSqF9ZyRb7wvzGp9XcKNQ'
SHEET_NAME = 'ConsultarOcorrencias'
import os
# === API SOAP CONFIG ===
# === wsdl_url = 'https://intelligenza.multidadosti.com.br/_vmulti_b/webservices/index.php/?wsdl'
wsdl_url = 'https://intelligenza.multidadosti.com.br/Webservices/index.php?wsdl'
params_base = {
    'USUARIO_WS': os.getenv('USUARIO_WS'),
    'SENHA_WS': os.getenv('SENHA_WS'),
    'TIPO_DATA': 'ultima_modificacao',
    'RETORNO': 'json',
    'CAMPOS': (
        'data_abertura,prioridade_desc,numero,idcamposvariaveis_572,idocorrencia_parent,'
        'cliente_nome,aberto_por,descricao,area,oco_status,operador_responsavel_logado,'
        'sla_resposta,sla_resp_horas,sla_solucao,sla_solucao_horas,idade_oc,tempo_dependencia_user,nome_projeto,'
        'problema,hora_ultima_modificacao,contato_email,data_fechamento,'
        'horas_lancadas,stat_cnt_16,stat_cnt_100,stat_cnt_5017,oco_status_simples,vencimento_sla_solucao,resposta_dentro_sla,solucao_dentro_sla'
    ),
    'CODIGO_AUXILIAR_CLIENTE': '',
}

# === GOOGLE AUTH ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheets_service = build('sheets', 'v4', credentials=credentials)

# === FUNÇÕES ===
def limpar_html(texto):
    if not isinstance(texto, str):
        return ''
    return re.sub(r'<.*?>', '', texto).replace('\xa0', ' ').strip()

def formatar_data(valor):
    try:
        data = pd.to_datetime(valor, dayfirst=True)
        return data.strftime('%Y-%m-%d')
    except:
        return ''

# === DEFINIR COLUNAS PADRÃO ===
colunas = [
    "Data/Hora abertura", "Prioridade", "N.º", "Código Sistema de Chamados do Cliente",
    "OC Pai : N.º", "Cliente", "Aberto por", "Descrição", "Divisão", "Status",
    "Operador responsável", "SLA de resposta", "Status do SLA de resposta", "SLA de solução",
    "Status do SLA de solução", "Idade da Ocorrência", "Tempo de dependência do usuario", "Projeto",
    "Solicitação", "Data/Hora da Última modificação", "Email do Contato do Cliente", "Data/hora de encerramento",
    "Horas Lançadas (em minutos)", "Cnt. Status : CLIENTE - Aguardando Retorno",
    "Cnt. Status : INTELLIGENZA - Feedback Retornado", "Cnt. Status : INTELLIGENZA - Feedback retornado do cliente",
    "Status (sem tempo decorrido)", "Data de Vencimento do SLA de Solução", "Resposta dentro do SLA","Solução dentro do SLA"
]

# === CONSULTAR DADOS MODIFICADOS DESDE ONTEM ===
inicio = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
fim = inicio + timedelta(days=2) - timedelta(seconds=1)

params = params_base.copy()
params['DATA_INI'] = inicio.strftime('%Y-%m-%d %H:%M:%S')
params['DATA_FIM'] = fim.strftime('%Y-%m-%d %H:%M:%S')

print(f"🔄 Consultando ocorrências modificadas entre {params['DATA_INI']} e {params['DATA_FIM']}...")
client = Client(wsdl_url)
response = client.service.ConsultarOcorrencias(**params)

dados_novos = json.loads(response) if isinstance(response, str) and response.strip() else response
df_novo = pd.DataFrame(dados_novos)

if not df_novo.empty:
    # Limpar e formatar dados
    df_novo = df_novo.applymap(limpar_html)
    for campo in ['data_abertura', 'vencimento_sla_solucao', 'data_fechamento']:
        if campo in df_novo.columns:
            df_novo[campo] = df_novo[campo].apply(formatar_data)

    df_novo = df_novo.rename(columns={
        "data_abertura": "Data/Hora abertura", "prioridade_desc": "Prioridade", "numero": "N.º",
        "idcamposvariaveis_572": "Código Sistema de Chamados do Cliente", "idocorrencia_parent": "OC Pai : N.º",
        "cliente_nome": "Cliente", "aberto_por": "Aberto por", "descricao": "Descrição", "area": "Divisão",
        "oco_status": "Status", "operador_responsavel_logado": "Operador responsável", "sla_resposta": "SLA de resposta",
        "sla_resp_horas": "Status do SLA de resposta", "sla_solucao": "SLA de solução",
        "sla_solucao_horas": "Status do SLA de solução", "idade_oc": "Idade da Ocorrência",
        "tempo_dependencia_user": "Tempo de dependência do usuario", "nome_projeto": "Projeto",
        "problema": "Solicitação", "hora_ultima_modificacao": "Data/Hora da Última modificação",
        "contato_email": "Email do Contato do Cliente", "data_fechamento": "Data/hora de encerramento",
        "horas_lancadas": "Horas Lançadas (em minutos)", "stat_cnt_16": "Cnt. Status : CLIENTE - Aguardando Retorno",
        "stat_cnt_100": "Cnt. Status : INTELLIGENZA - Feedback Retornado",
        "stat_cnt_5017": "Cnt. Status : INTELLIGENZA - Feedback retornado do cliente",
        "oco_status_simples": "Status (sem tempo decorrido)", "vencimento_sla_solucao": "Data de Vencimento do SLA de Solução",
        "resposta_dentro_sla": "Resposta dentro do SLA", "solucao_dentro_sla": "Solução dentro do SLA"
    })[colunas]

    # === LER DADOS EXISTENTES ===
    resultado = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2:AB"
    ).execute()

    dados_planilha = resultado.get('values', [])
    for linha in dados_planilha:
        while len(linha) < len(colunas):
            linha.append('')
    df_existente = pd.DataFrame(dados_planilha, columns=colunas)

    df_existente['N.º'] = df_existente['N.º'].astype(str)
    df_novo['N.º'] = df_novo['N.º'].astype(str)

    # === ATUALIZAR REGISTROS EXISTENTES COM OS MODIFICADOS ===
    df_final = df_existente[~df_existente['N.º'].isin(df_novo['N.º'])]
    df_final = pd.concat([df_final, df_novo], ignore_index=True)

    # === APLICAR FILTROS APÓS ATUALIZAR ===
    areas_permitidas = [
        'Em desenvolvimento ABAP, PI, WF, WD, .NET', 'EC para ECP (colaboradores)',
        'EC para Enterprise SQL/SAP IBS (colaboradores)', 'EC para WFS', 'ECP para ADP',
        'ECP para EC (CIPA, Demais estabilidades)', 'ECP para Enterprise SQL (ficha financeira)',
        'ECP para SAP IBS (contábil)', 'ECP para Senior (férias)', 'ECP para SOC (Unidade, Setor, Cargo, Hierarquia, M',
        'ECP para Vacation Control (contingente)', 'GDP para EC (onboarding)', 'Integração ALE',
        'Senior para ECP (ausências)', 'Senior para ECP (fechamento ponto)',
    #    'SOC para ECP (Atestados, CIPA)', 
        'Tecnologia - Dev & Integração',
        'WFS para ADP', 'WFS para ECP', 'WFS para Senior',
        'SFSF Integrations - EC Payroll, Boomi/SCI, API', 'R - Integrations - EC Payroll, Boomi/SCI, API'
    ]

    df_final = df_final[
        df_final['Divisão'].isin(areas_permitidas) &
        (~df_final['Status (sem tempo decorrido)'].isin(['Encerrada', 'Ocorrência Cancelada', 'Encerrar SUB-Ocorrência']))
    ]

    # === REMOVER DUPLICADOS, ORDENAR E ESCREVER PLANILHA ===
    df_final = df_final.drop_duplicates(subset=['N.º'], keep='last').sort_values(by="Data/Hora abertura")
    df_final = df_final.fillna('')
    valores = [colunas] + df_final.values.tolist()

    sheets_service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME
    ).execute()

    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption='RAW',
        body={'values': valores}
    ).execute()

    print(f"✅ Planilha atualizada com {len(df_final)} registros.")

    # === ATUALIZAR METADATA ===
    ultima_modificacao = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range='metadata!A1',
        valueInputOption='RAW',
        body={'values': [['ÚltimaAtualizacao']]}
    ).execute()
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range='metadata!A2',
        valueInputOption='RAW',
        body={'values': [[ultima_modificacao]]}
    ).execute()

    print(f"📅 Metadata atualizada: {ultima_modificacao}")
else:
    print("⚠️ Nenhuma ocorrência nova/modificada hoje.")






