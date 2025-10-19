# AJU.py - Versão Final com Correção de Colunas
import logging
import os
import json
import threading
import pytz
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pathlib import Path

# ====================================================================
# CONFIGURAÇÕES GERAIS
# ====================================================================
DIRETORIO_ATUAL = Path(__file__).parent
IS_ON_RENDER = os.environ.get('RENDER') == 'true'
PASTA_SEGURA = DIRETORIO_ATUAL if IS_ON_RENDER else Path.home() / "Documents"
TOKEN_FILE = PASTA_SEGURA / 'token.json'

DRIVE_FOLDER_ID = "1O5sLCfgQ4pC42KgldkuRJRpZH8nQuSgK"
SPREADSHEET_ID = "1F7J2HTY-1PefF9UTajvQbq8jgAdEc1vrU0TeR3np8cI"
SHEET_NAME = "Base"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
TZ_SAO_PAULO = pytz.timezone('America/Sao_Paulo')

# ====================================================================
# MAPEAMENTO DE COLUNAS (CORRIGIDO E REVISADO)
# ====================================================================
COL_DATE_TIME = 1; COL_VIGILANTE = 2; COL_ORIGEM = 3; COL_DESTINO = 4;
COL_TRANSPORTADORA = 5; COL_MOTORISTA = 6; COL_PLACA_CAVALO = 7; COL_PLACA_CARRETA = 8;
COL_LACRE_CARRETA = 9; COL_LACRE_VOID = 10; COL_FOTO_CARRETA_SAIDA = 11;
COL_FOTO_REGISTRO_SAIDA = 12; COL_FOTO_LACRE_SAIDA = 13;

# --- ESTA É A SEÇÃO CRÍTICA ---
COL_DATE_TIME_FINALIZACAO = 14; # Coluna N
COL_LACRE_VIOLADO = 15;         # Coluna O
COL_INFORMACOES_PROCEDEM = 16;  # Coluna P
COL_OBSERVACOES = 17;           # Coluna Q
COL_FOTO_STATUS = 18;           # Coluna R
COL_VIDEO_ABERTURA = 19;        # Coluna S
COL_FOTO_LACRE_STATUS = 20;     # Coluna T
COL_STATUS_FINAL = 21;          # Coluna U

# ====================================================================
# INICIALIZAÇÃO E AUTENTICAÇÃO
# ====================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__, template_folder=DIRETORIO_ATUAL)
CORS(app)
sheet_lock = threading.Lock()

creds = None; gspread_client = None; drive_service = None; spreadsheet = None; worksheet = None;

try:
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"ERRO: 'token.json' não encontrado em '{PASTA_SEGURA}'.")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    gspread_client = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    spreadsheet = gspread_client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    logging.info("✅ Autenticação bem-sucedida.")
except Exception as e:
    logging.error(f"❌ Falha crítica na inicialização: {type(e).__name__} - {e}")
    raise e

# ====================================================================
# FUNÇÕES E ROTAS DA API
# ====================================================================

def _get_drive_link_by_id(file_id):
    if not file_id: return ""
    try:
        permission = {'type': 'anyone', 'role': 'reader'}
        drive_service.permissions().create(fileId=file_id, body=permission).execute()
        return f'https://drive.google.com/uc?export=view&id={file_id}'
    except Exception as e:
        logging.error(f"Erro ao definir permissão para fileId {file_id}: {e}")
        return f'https://drive.google.com/uc?export=view&id={file_id}'

@app.route('/')
def index():
    return render_template('Index.html')

@app.route('/generate_upload_url', methods=['POST'])
def generate_upload_url():
    try:
        data = request.get_json()
        file_name, mime_type = data.get('fileName'), data.get('mimeType')
        if not file_name or not mime_type: return jsonify({'erro': 'Nome ou tipo de arquivo ausente.'}), 400

        file_metadata = {'name': file_name, 'parents': [DRIVE_FOLDER_ID]}
        file = drive_service.files().create(body=file_metadata, fields='id').execute()
        file_id = file.get('id')

        from googleapiclient.http import build_http
        headers = {"Authorization": f"Bearer {creds.token}"}
        http = build_http()
        resp, _ = http.request(uri=f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=resumable", method='PATCH', headers=headers)

        if 'location' in resp:
            upload_url = resp['location']
            logging.info(f"URL de upload gerada para: {file_name} (ID: {file_id})")
            return jsonify({'uploadUrl': upload_url, 'fileId': file_id})
        else:
            raise Exception("Não foi possível obter a URL de upload.")
    except Exception as e:
        logging.error(f"Erro ao gerar URL de upload: {e}")
        return jsonify({'erro': str(e)}), 500

@app.route('/registrar_saida', methods=['POST'])
def registrar_saida():
    try:
        form = request.get_json()
        
        if form.get('isBiTrem'): lacre_para_verificar = f"{form.get('lacreCarreta1', '').strip()} / {form.get('lacreCarreta2', '').strip()}"
        else: lacre_para_verificar = form.get('lacreCarreta', '').strip().upper()
        
        with sheet_lock:
            coluna_lacres = worksheet.col_values(COL_LACRE_CARRETA)
            if lacre_para_verificar.upper() in [l.upper() for l in coluna_lacres]:
                return jsonify({'sucesso': False, 'mensagem': f'Erro: O Lacre "{lacre_para_verificar}" já foi registrado.'}), 409

            links_carreta = _get_drive_link_by_id(form.get('fileCarreta', {}).get('id'))
            links_registro = _get_drive_link_by_id(form.get('fileRegistroSaida', {}).get('id'))
            links_lacre = _get_drive_link_by_id(form.get('fileLacre', {}).get('id'))
            
            if form.get('isBiTrem'): placa_carreta_completa = f"{form.get('placaCarreta1', '').upper().strip()} / {form.get('placaCarreta2', '').upper().strip()}"
            else: placa_carreta_completa = form.get('placaCarreta', '').upper().strip()

            new_row = [
                datetime.now(TZ_SAO_PAULO).strftime('%d/%m/%Y %H:%M:%S'), form.get('vigilante', '').upper().strip(),
                form.get('origem', ''), form.get('destino', ''), form.get('transportadora', ''),
                form.get('motorista', '').upper().strip(), "'" + form.get('placaCavalo', '').upper().strip(),
                "'" + placa_carreta_completa, "'" + lacre_para_verificar.upper(), "V" + form.get('lacreNumero', '').strip(),
                links_carreta, links_registro, links_lacre, '', '', '', '', '', '', '', 'PENDENTE'
            ]
            worksheet.append_row(new_row, value_input_option='USER_ENTERED')
            logging.info(f"Nova saída registrada com lacre: {lacre_para_verificar}")
        
        return jsonify({'sucesso': True, 'mensagem': f'Saída registrada com sucesso!'})
    except Exception as e:
        logging.error(f"ERRO REAL em registrarSaida: {e}")
        return jsonify({'sucesso': False, 'mensagem': f"Erro no servidor ao registrar na planilha."}), 500

@app.route('/buscar_recebimento', methods=['POST'])
def buscar_recebimento():
    try:
        data = request.get_json()
        lacre_busca = data.get('lacreCarretaBusca', '')
        todos_os_dados = worksheet.get_all_values()
        if len(todos_os_dados) < 2: return jsonify({'erro': "Não há dados na planilha."}), 404
        lacre_formatado = lacre_busca.strip().replace(' ', '').upper()
        for i, linha in reversed(list(enumerate(todos_os_dados))):
            if i == 0 or len(linha) < COL_STATUS_FINAL: continue
            lacre_atual = linha[COL_LACRE_CARRETA - 1].strip().replace(' ', '').upper()
            status_atual = linha[COL_STATUS_FINAL - 1].strip().upper()
            if lacre_atual == lacre_formatado and status_atual == "PENDENTE":
                row_index = i + 1
                try: data_formatada = datetime.strptime(linha[COL_DATE_TIME-1], '%d/%m/%Y %H:%M:%S').strftime('%d/%m/%Y, %H:%M:%S')
                except ValueError: data_formatada = linha[COL_DATE_TIME-1]
                resultado = {"sucesso": True, "resultados": [{"Data": data_formatada, "Vigilante": linha[COL_VIGILANTE - 1], "Origem": linha[COL_ORIGEM - 1], "Destino": linha[COL_DESTINO - 1], "Transportadora": linha[COL_TRANSPORTADORA - 1], "Motorista": linha[COL_MOTORISTA - 1], "Placa_Cavalo": linha[COL_PLACA_CAVALO - 1], "Placa_Carreta": linha[COL_PLACA_CARRETA - 1], "Lacre_Carreta": linha[COL_LACRE_CARRETA - 1], "Lacre_Void": linha[COL_LACRE_VOID - 1], "Foto_Carreta_Saida": linha[COL_FOTO_CARRETA_SAIDA - 1], "Foto_Registro_Saida": linha[COL_FOTO_REGISTRO_SAIDA - 1], "Foto_Lacre_Saida": linha[COL_FOTO_LACRE_SAIDA - 1], "rowIndex": row_index}]}
                return jsonify(resultado)
        return jsonify({'erro': f'Nenhuma pendência encontrada para o Lacre "{lacre_busca}"'}), 404
    except Exception as e:
        logging.error(f"Erro em buscarRecebimento: {e}")
        return jsonify({'erro': f'Erro no servidor ao buscar. Detalhes: {e}'}), 500

@app.route('/finalizar_recebimento', methods=['POST'])
def finalizar_recebimento():
    try:
        form = request.get_json()
        row_index = int(form.get('rowIndex'))
        
        links_status = _get_drive_link_by_id(form.get('fileStatus', {}).get('id'))
        links_video = _get_drive_link_by_id(form.get('fileVideoAbertura', {}).get('id'))
        links_lacre_status = _get_drive_link_by_id(form.get('fileLacreStatus', {}).get('id'))

        values_to_update = [
            datetime.now(TZ_SAO_PAULO).strftime('%d/%m/%Y %H:%M:%S'), form.get('lacreViolado'), 
            form.get('informacoesProcedem'), form.get('observacoes', ""), 
            links_status, links_video, links_lacre_status, "FINALIZADO"
        ]
        
        # --- ESTA É A LÓGICA CORRIGIDA ---
        start_cell = gspread.utils.rowcol_to_a1(row_index, COL_DATE_TIME_FINALIZACAO)
        end_cell = gspread.utils.rowcol_to_a1(row_index, COL_STATUS_FINAL)
        
        with sheet_lock:
            current_status = worksheet.cell(row_index, COL_STATUS_FINAL).value
            if current_status and current_status.strip().upper() == "FINALIZADO":
                return jsonify({'sucesso': False, 'mensagem': "Erro: Este registro já foi finalizado."}), 409
            worksheet.update(f'{start_cell}:{end_cell}', [values_to_update])
            logging.info(f"Recebimento finalizado para linha {row_index}")
        
        return jsonify({'sucesso': True, 'mensagem': "Conferência finalizada com sucesso!"})
    except Exception as e:
        logging.error(f"ERRO REAL em finalizarRecebimento: {e}")
        return jsonify({'sucesso': False, 'mensagem': f"Erro no servidor ao finalizar na planilha."}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)