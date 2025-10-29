import os
import re
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build


# === GOOGLE CONFIG ===
SERVICE_ACCOUNT_FILE = 'credenciais.json'
SPREADSHEET_ID = '13_q2iGwqjpyY6JaCREiaJdeSqF9ZyRb7wvzGp9XcKNQ'
SHEET_NAME = 'ConsultarOcorrencias'

# === SOAP CONFIG ===
SOAP_ENDPOINT = "https://intelligenza.multidadosti.com.br/webservices/index.php"
SOAP_ACTION = "urn:server.Multidados#ConsultarOcorrencias"
SOAP_NS = "urn:server.Multidados"
SOAP_ENV = "http://schemas.xmlsoap.org/soap/envelope/"

USUARIO_WS = os.getenv("USUARIO_WS")
SENHA_WS = os.getenv("SENHA_WS")

# === CAMPOS QUE VOCÊ QUER TRAZER ===
CAMPOS = (
    "data_abertura,prioridade_desc,numero,idcamposvariaveis_572,idocorrencia_parent,"
    "cliente_nome,aberto_por,descricao,area,oco_status,operador_responsavel_logado,"
    "sla_resposta,sla_resp_horas,sla_solucao,sla_solucao_horas,idade_oc,tempo_dependencia_user,nome_projeto,"
    "problema,hora_ultima_modificacao,contato_email,data_fechamento,"
    "horas_lancadas,stat_cnt_16,stat_cnt_100,stat_cnt_5017,oco_status_simples,"
    "vencimento_sla_solucao,resposta_dentro_sla,solucao_dentro_sla"
)

# === JANELA DE CONSULTA ===
# ontem 00:00 até amanhã 23:59:59
inicio = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
fim = inicio + timedelta(days=2) - timedelta(seconds=1)

DATA_INI = inicio.strftime('%Y-%m-%d %H:%M:%S')
DATA_FIM = fim.strftime('%Y-%m-%d %H:%M:%S')

print(f"🔄 Consultando ocorrências modificadas entre {DATA_INI} e {DATA_FIM}...")


# === MONTAÇÃO DO ENVELOPE SOAP ===
def montar_envelope():
    # IMPORTANTE: o WSDL mostra que os parâmetros vão como partes do método ConsultarOcorrencias,
    # não dentro de "request"/"payload" etc. É RPC style:
    #
    # <ConsultarOcorrencias>
    #   <USUARIO_WS>...</USUARIO_WS>
    #   <SENHA_WS>...</SENHA_WS>
    #   ...
    # </ConsultarOcorrencias>
    #
    # Vamos gerar isso dinamicamente.
    body_inner = f"""
        <USUARIO_WS>{USUARIO_WS}</USUARIO_WS>
        <SENHA_WS>{SENHA_WS}</SENHA_WS>
        <TIPO_DATA>ultima_modificacao</TIPO_DATA>
        <DATA_INI>{DATA_INI}</DATA_INI>
        <DATA_FIM>{DATA_FIM}</DATA_FIM>
        <RETORNO>json</RETORNO>
        <CAMPOS>{CAMPOS}</CAMPOS>
        <CODIGO_AUXILIAR_CLIENTE></CODIGO_AUXILIAR_CLIENTE>
    """.strip()

    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="{SOAP_ENV}" xmlns:urn="{SOAP_NS}">
  <soapenv:Body>
    <urn:ConsultarOcorrencias>
      {body_inner}
    </urn:ConsultarOcorrencias>
  </soapenv:Body>
</soapenv:Envelope>"""
    return envelope


def chamar_servico():
    envelope_xml = montar_envelope()

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
    }

    resp = requests.post(
        SOAP_ENDPOINT,
        data=envelope_xml.encode("utf-8"),
        headers=headers,
        timeout=60
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"❌ Erro HTTP {resp.status_code} na chamada SOAP.\n"
            f"Resposta (início): {resp.text[:500]}"
        )

    return resp.text


def extrair_json_do_retorno(soap_response_text):
    # A resposta SOAP deve ter algo como:
    #
    # <soapenv:Envelope ...>
    #   <soapenv:Body>
    #     <ns1:ConsultarOcorrenciasResponse>
    #       <return>{"[...]": ...}</return>
    #     </ns1:ConsultarOcorrenciasResponse>
    #   </soapenv:Body>
    # </soapenv:Envelope>
    #
    # Estratégia simples: pega o conteúdo de <return>...</return>.

    m = re.search(r"<return>(.*?)</return>", soap_response_text, flags=re.DOTALL)
    if not m:
        raise RuntimeError(
            "❌ Não encontrei a tag <return> na resposta SOAP.\n"
            "Prévia da resposta:\n" + soap_response_text[:500]
        )

    raw = m.group(1).strip()

    # às vezes vem escapado (ex: &quot; etc.). Vamos desserializar HTML entities básicas
    # mas primeiro tenta json direto:
    try:
        return json.loads(raw)
    except Exception:
        # tentar des-escape básico de XML:
        unescaped = (
            raw
            .replace("&quot;", '"')
            .replace("&apos;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
        )
        try:
            return json.loads(unescaped)
        except Exception as e:
            raise RuntimeError(
                "❌ O conteúdo de <return> não é JSON válido.\n"
                f"Conteúdo de <return> (início): {raw[:500]}\nErro: {e}"
            )


# === GOOGLE AUTH ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)
sheets_service = build('sheets', 'v4', credentials=credentials)


def limpar_html(texto):
    if not isinstance(texto, str):
        return ''
    return re.sub(r'<.*?>', '', texto).replace('\xa0', ' ').strip()


def formatar_data(valor):
    try:
        data = pd.to_datetime(valor, dayfirst=True)
        return data.strftime('%Y-%m-%d')
    except Exception:
        return ''


colunas = [
    "Data/Hora abertura", "Prioridade", "N.º", "Código Sistema de Chamados do Cliente",
    "OC Pai : N.º", "Cliente", "Aberto por", "Descrição", "Divisão", "Status",
    "Operador responsável", "SLA de resposta", "Status do SLA de resposta", "SLA de solução",
    "Status do SLA de solução", "Idade da Ocorrência", "Tempo de dependência do usuario", "Projeto",
    "Solicitação", "Data/Hora da Última modificação", "Email do Contato do Cliente", "Data/hora de encerramento",
    "Horas Lançadas (em minutos)", "Cnt. Status : CLIENTE - Aguardando Retorno",
    "Cnt. Status : INTELLIGENZA - Feedback Retornado", "Cnt. Status : INTELLIGENZA - Feedback retornado do cliente",
    "Status (sem tempo decorrido)", "Data de Vencimento do SLA de Solução",
    "Resposta dentro do SLA", "Solução dentro do SLA"
]


# === 1) CHAMAR O WEB SERVICE E OBTER OS DADOS ===
soap_raw = chamar_servico()
dados_novos = extrair_json_do_retorno(soap_raw)

# A resposta pode ser lista de dicts direto, ou um dict com lista dentro
if isinstance(dados_novos, dict):
    # heurística: pega a primeira lista encontrada
    lista = None
    for v in dados_novos.values():
        if isinstance(v, list):
            lista = v
            break
    if lista is None:
        lista = []
    dados_novos = lista

df_novo = pd.DataFrame(dados_novos)

if df_novo.empty:
    print("⚠️ Nenhuma ocorrência nova/modificada hoje.")
    raise SystemExit(0)

# === 2) LIMPAR CAMPOS HTML E FORMATAR DATAS ===
df_novo = df_novo.applymap(limpar_html)

for campo in ['data_abertura', 'vencimento_sla_solucao', 'data_fechamento']:
    if campo in df_novo.columns:
        df_novo[campo] = df_novo[campo].apply(formatar_data)

df_novo = df_novo.rename(columns={
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
    "operador_responsavel_logado": "Operador responsável",
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
    "vencimento_sla_solucao": "Data de Vencimento do SLA de Solução",
    "resposta_dentro_sla": "Resposta dentro do SLA",
    "solucao_dentro_sla": "Solução dentro do SLA"
})

# Forçar ordem de colunas que a planilha espera
df_novo = df_novo[colunas]

# === 3) BUSCAR O QUE JÁ ESTÁ NA PLANILHA ===
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

# === 4) MERGE INCREMENTAL ===
df_final = df_existente[~df_existente['N.º'].isin(df_novo['N.º'])]
df_final = pd.concat([df_final, df_novo], ignore_index=True)

# === 5) FILTROS ===
areas_permitidas = [
    'Em desenvolvimento ABAP, PI, WF, WD, .NET', 'EC para ECP (colaboradores)',
    'EC para Enterprise SQL/SAP IBS (colaboradores)', 'EC para WFS', 'ECP para ADP',
    'ECP para EC (CIPA, Demais estabilidades)', 'ECP para Enterprise SQL (ficha financeira)',
    'ECP para SAP IBS (contábil)', 'ECP para Senior (férias)',
    'ECP para SOC (Unidade, Setor, Cargo, Hierarquia, M',
    'ECP para Vacation Control (contingente)', 'GDP para EC (onboarding)', 'Integração ALE',
    'Senior para ECP (ausências)', 'Senior para ECP (fechamento ponto)',
    # 'SOC para ECP (Atestados, CIPA)',
    'Tecnologia - Dev & Integração',
    'WFS para ADP', 'WFS para ECP', 'WFS para Senior',
    'SFSF Integrations - EC Payroll, Boomi/SCI, API',
    'R - Integrations - EC Payroll, Boomi/SCI, API'
]

df_final = df_final[
    df_final['Divisão'].isin(areas_permitidas) &
    (~df_final['Status (sem tempo decorrido)'].isin([
        'Encerrada',
        'Ocorrência Cancelada',
        'Encerrar SUB-Ocorrência'
    ]))
]

df_final = (
    df_final
    .drop_duplicates(subset=['N.º'], keep='last')
    .sort_values(by="Data/Hora abertura")
    .fillna('')
)

# === 6) ESCREVER NA PLANILHA ===
valores = [colunas] + df_final.values.tolist()

sheets_service.spreadsheets().values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range=SHEET_NAME
).execute()

sheets_service.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}!A1",
    valueInputOption='RAW',
    body={'values': valores}
).execute()

print(f"✅ Planilha atualizada com {len(df_final)} registros.")

# === 7) METADATA ===
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
