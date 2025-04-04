import logging
import time
import threading
import requests
import json
from config.settings import Settings


# ------------------------ CLIENTE API Facturas de (FE y BOMC) Notas de Credito (NC y NDP)------------------------
# Esta clase se encarga de interactuar con la API para obtener el token y consultar documentos.
# Se ha añadido un manejo de errores más robusto y se han mejorado los mensajes de logging para facilitar la depuración.
# Además, se ha añadido un método para programar la ejecución de tareas a intervalos regulares.
class APIClient:
    @staticmethod
    def obtener_token():
        start_time = time.time()

        url_base = f"{Settings.API_LOGIN_URL}?username={Settings.API_USERNAME}&password={Settings.API_PASSWORD}"
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(url_base, headers=headers)
            response.raise_for_status()

            token = response.text.strip().replace('"', '')

            if not token:
                logging.error("No se recibió un token válido.")
                return None

            elapsed_time = time.time() - start_time
            minutos, segundos = divmod(int(elapsed_time), 60)

            token_mascarado = f"{token[:5]}...{token[-5:]}"
            logging.info(f"✅ Token obtenido correctamente: {token_mascarado}")
            logging.info(f"⏱ Tiempo de obtención del token: {minutos}m {segundos}s")

            return token

        except requests.RequestException as e:
            logging.error(f"❌ Error en la solicitud del token: {e}")
            return None
        except ValueError as e:
            logging.error(f"❌ Error al procesar la respuesta del token: {e}")
            return None

    @staticmethod
    def obtener_documento(token, document_id, document_type):
        if document_type is None:
            logging.warning(f"Tipo de documento inválido para DocumentID {document_id}. No se enviará la solicitud.")
            return None

        url_base = f"{Settings.API_STATUS_URL}?SchemaID={Settings.API_SCHEMAID}&DocumentType={document_type}&IDNumber={Settings.API_IDNUMBER}&DocumentID={document_id.strip()}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            logging.info(f"Consultando detalles para DocumentID: {document_id} con DocumentType: {document_type}")
            
            response = requests.post(url_base, headers=headers)  # Sin JSON en el cuerpo, porque los parámetros van en la URL
            response.raise_for_status()  # Lanza excepción si hay error HTTP

            data = response.json()

            if not data:
                logging.warning(f"La respuesta de la API está vacía para DocumentID {document_id}")
                return None

            logging.info(f"Consulta de documento exitosa para DocumentID: {document_id}")
            return data

        except requests.RequestException as e:
            logging.error(f"Error en la solicitud API para DocumentID {document_id}: {e}")
            return None
        except ValueError as e:
            logging.error(f"Error al procesar la respuesta JSON para DocumentID {document_id}: {e}")
            return None
        
# ---------------- CLIENTE API Documento Soporte de (BRS y DE) Notas de Ajuste (NCDS)------------------------
# Esta clase se encarga de interactuar con la API para obtener el token y consultar documentos.
# Se ha añadido un manejo de errores más robusto y se han mejorado los mensajes de logging para facilitar la depuración.
# Además, se ha añadido un método para programar la ejecución de tareas a intervalos regulares.
    @staticmethod
    def obtener_documento_2(token, document_id, document_type):
        if document_type is None:
            logging.warning(f"Tipo de documento inválido para DocumentID {document_id}. No se enviará la solicitud.")
            return None

        url_base = f"{Settings.API_STATUS_URL_2}?SchemaID={Settings.API_SCHEMAID}&DocumentType={document_type}&IDNumber={Settings.API_IDNUMBER}&DocumentID={document_id.strip()}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            logging.info(f"Consultando detalles para DocumentID: {document_id} con DocumentType: {document_type}")
            
            response = requests.post(url_base, headers=headers)  # Sin JSON en el cuerpo, porque los parámetros van en la URL
            response.raise_for_status()  # Lanza excepción si hay error HTTP

            data = response.json()

            if not data:
                logging.warning(f"La respuesta de la API está vacía para DocumentID {document_id}")
                return None

            logging.info(f"Consulta de documento exitosa para DocumentID: {document_id}")
            return data

        except requests.RequestException as e:
            logging.error(f"Error en la solicitud API para DocumentID {document_id}: {e}")
            return None
        except ValueError as e:
            logging.error(f"Error al procesar la respuesta JSON para DocumentID {document_id}: {e}")
            return None

# ------------------------ PROGRAMAR TAREAS CON INTERVALOS ------------------------

def ejecutar_cada_10_minutos():
    while True:
        token = APIClient.obtener_token()
        if token:
            APIClient.obtener_documento(token, "12345")  # Ejemplo de consulta
        time.sleep(600)  # 600 segundos = 10 minutos

if __name__ == "__main__":
    logging.basicConfig(
        filename='log_validacion.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("🔄 Iniciando proceso de autenticación con la API...")

    token = APIClient.obtener_token()

    if token:
        logging.info("✅ Proceso finalizado con éxito.")
    else:
        logging.error("❌ Falló la obtención del token.")