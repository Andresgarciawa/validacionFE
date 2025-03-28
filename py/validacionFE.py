import pyodbc  # type: ignore
import smtplib # Nuevo para enviar correos
from email.message import EmailMessage # Nuevo para enviar correos
import os # Nuevo para cargar variables de entorno
from dotenv import load_dotenv # Nuevo para cargar variables de entorno
import re # Nuevo para validar correos electrónicos
import logging # Nuevo para registrar eventos
import requests # Nuevo para manejar peticiones HTTP
import time  # Nuevo para medir el tiempo de ejecución

# Configuración del log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Muestra logs en consola
        logging.FileHandler('documento_request.log')  # Guarda logs en archivo
    ]
)

def obtener_token():
    start_time = time.time()
    url = "https://www.misfacturas.com.co/IntegrationAPI_2/api/login"
    
    payload = {
        "username": "830129024", 
        "password": "830129024"
    }
    
    headers = {"Content-Type": "application/json"}
    
    try:
        logging.info("Intentando obtener el token...")
        
        response = requests.post(url, json=payload, headers=headers)
        
        logging.info(f"Código de estado: {response.status_code}")
        logging.info(f"Respuesta cruda: {response.text}")
        
        response.raise_for_status()
        
        data = response.json()
        token = data.get("access_token")
        
        if not token:
            logging.error("No se recibió un token válido en la respuesta.")
            logging.error(f"Respuesta completa: {data}")
            return None
        
        token_mascarado = f"{token[:5]}...{token[-5:]}"
        elapsed_time = time.time() - start_time
        minutos, segundos = divmod(int(elapsed_time), 60)
        
        logging.info(f"Token obtenido correctamente: {token_mascarado}")
        logging.info(f"Tiempo de obtención: {minutos}m {segundos}s")
        
        return token
    
    except requests.RequestException as e:
        logging.error(f"Error en la solicitud del token: {e}")
        return None
    except ValueError as e:
        logging.error(f"Error al procesar la respuesta JSON: {e}")
        return None

def obtener_documento(token, document_id):
    """
    Consulta los detalles de un documento específico usando la API GetDocument
    
    Args:
        token (str): Token de autenticación
        document_id (str): ID del documento a consultar
    
    Returns:
        dict: Respuesta de la API o None si hay error
    """
    url_base = "https://www.misfacturas.com.co/IntegrationAPI_2/api/GetDocument"
    
    params = {
        "SchemaID": 31,
        "DocumentType": 1,
        "IDNumber": "830129024",
        "DocumentID": document_id
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        logging.info(f"Consultando detalles para DocumentID: {document_id}")
        
        response = requests.get(url_base, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        logging.info(f"Consulta de documento exitosa para DocumentID: {document_id}")
        return data
    
    except requests.RequestException as e:
        logging.error(f"Error al consultar DocumentID {document_id}: {e}")
        return None
    except ValueError as e:
        logging.error(f"Error al procesar respuesta JSON para DocumentID {document_id}: {e}")
        return None

def consultar_estado_documento(token, document_id):
    """
    Consulta el estado de un documento específico
    
    Args:
        token (str): Token de autenticación
        document_id (str): ID del documento a consultar
    
    Returns:
        dict: Respuesta de la API o None si hay error
    """
    url_base = "https://www.misfacturas.com.co/integrationAPI_2/api/GetDocumentStatus"
    
    params = {
        "SchemaID": 31,
        "DocumentType": 1,
        "IDNumber": "830129024",
        "DocumentID": document_id
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        logging.info(f"Consultando estado para DocumentID: {document_id}")
        
        response = requests.get(url_base, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        logging.info(f"Consulta exitosa para DocumentID: {document_id}")
        return data
    
    except requests.RequestException as e:
        logging.error(f"Error al consultar DocumentID {document_id}: {e}")
        return None
    except ValueError as e:
        logging.error(f"Error al procesar respuesta JSON para DocumentID {document_id}: {e}")
        return None

def actualizar_estado_documento(cursor, conn, documento):
    """
    Actualiza el estado del documento en la tabla de control
    
    Args:
        cursor: Cursor de la base de datos
        conn: Conexión a la base de datos
        documento (dict): Detalles del documento obtenidos de la API
    
    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    try:
        # Extraer los datos necesarios del documento
        document_id = documento.get('documentId', '')
        cufe = documento.get('cufe', '')
        estado_dian = documento.get('estadoDian', '')
        fecha_recepcion = documento.get('fechaRecepcionDian', '')
        
        # Consulta SQL para actualizar el estado
        update_query = """
        UPDATE CtrlFacEleCol 
        SET 
            docStatus = ?, 
            CUFE = ?, 
            EstadoDian = ?, 
            FechaRecepcionDian = ?,
            FechaActualizacion = GETDATE()
        WHERE ProveeTec = 'cenet' AND DocumentID = ?
        """
        
        # Ejecutar la actualización
        cursor.execute(update_query, (estado_dian, cufe, estado_dian, fecha_recepcion, document_id))
        conn.commit()
        
        logging.info(f"Documento {document_id} actualizado exitosamente")
        return True
    
    except Exception as e:
        logging.error(f"Error al actualizar documento {document_id}: {e}")
        conn.rollback()
        return False

# Resto del código anterior (enviar_correo, ESTADOS_DESCRIPCION, etc.) se mantiene igual

def procesar_documentos_pendientes():
    """
    Procesa los documentos pendientes de actualización
    """
    # Obtener token
    token = obtener_token()
    
    if not token:
        logging.error("No se pudo obtener el token. Terminando.")
        return
    
    # Conexión a la segunda base de datos
    try:
        conn2 = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={os.getenv("SQL_SERVER_2")};'
            f'DATABASE={os.getenv("SQL_DATABASE_2")};'
            f'UID={os.getenv("SQL_USER_2")};'
            f'PWD={os.getenv("SQL_PASSWORD_2")};'
            'Timeout=10;'
        )
        cursor2 = conn2.cursor()
        logging.info("Conexión exitosa a la segunda base de datos.")
    except Exception as e:
        logging.error(f"Error en la conexión a la segunda base de datos: {e}")
        return
    
    try:
        # Consultar documentos pendientes usando el query proporcionado
        query_pendientes = """
        SELECT documentId, docStatus, Cufe
        FROM CtrlFacEleCol
        WHERE docStatus NOT IN ('72','73','74')
        """
        
        cursor2.execute(query_pendientes)
        documentos_pendientes = cursor2.fetchall()
        
        logging.info(f"Se encontraron {len(documentos_pendientes)} documentos pendientes")
        
        # Procesar cada documento pendiente
        for documento in documentos_pendientes:
            document_id = documento[0]
            estado_actual = documento[1]
            cufe_actual = documento[2]
            
            try:
                # Consultar detalles del documento
                detalles_documento = obtener_documento(token, document_id)
                
                if detalles_documento:
                    # Verificar si hay cambios relevantes
                    nuevo_estado = detalles_documento.get('estadoDian', '')
                    nuevo_cufe = detalles_documento.get('cufe', '')
                    
                    # Solo actualizar si hay cambios significativos
                    if nuevo_estado != estado_actual or nuevo_cufe != cufe_actual:
                        # Actualizar estado en la base de datos
                        resultado = actualizar_estado_documento(cursor2, conn2, detalles_documento)
                        
                        if resultado:
                            logging.info(f"Documento {document_id} actualizado. Estado anterior: {estado_actual}, Nuevo estado: {nuevo_estado}")
                        else:
                            logging.warning(f"No se pudo actualizar el documento {document_id}")
                    else:
                        logging.info(f"Documento {document_id} no requiere actualización")
                
            except Exception as e:
                logging.error(f"Error procesando documento {document_id}: {e}")
        
        logging.info("Procesamiento de documentos pendientes completado")
    
    except Exception as e:
        logging.error(f"Error general al procesar documentos pendientes: {e}")
    
    finally:
        cursor2.close()
        conn2.close()

def actualizar_estado_documento(cursor, conn, documento):
    """
    Actualiza el estado del documento en la tabla de control
    
    Args:
        cursor: Cursor de la base de datos
        conn: Conexión a la base de datos
        documento (dict): Detalles del documento obtenidos de la API
    
    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    try:
        # Extraer los datos necesarios del documento
        document_id = documento.get('documentId', '')
        cufe = documento.get('cufe', '')
        estado_dian = documento.get('estadoDian', '')
        fecha_recepcion = documento.get('fechaRecepcionDian', '')
        
        # Consulta SQL para actualizar el estado
        update_query = """
        UPDATE CtrlFacEleCol 
        SET 
            docStatus = ?, 
            CUFE = ?, 
            EstadoDian = ?, 
            FechaRecepcionDian = ?,
            FechaActualizacion = GETDATE()
        WHERE DocumentID = ?
        """
        
        # Ejecutar la actualización
        cursor.execute(update_query, (estado_dian, cufe, estado_dian, fecha_recepcion, document_id))
        conn.commit()
        
        logging.info(f"Documento {document_id} actualizado exitosamente")
        return True
    
    except Exception as e:
        logging.error(f"Error al actualizar documento {document_id}: {e}")
        conn.rollback()
        return False

def main():
    # Procesar documentos pendientes antes de las validaciones
    procesar_documentos_pendientes()
    
    # Obtener token
    token = obtener_token()
    
    if not token:
        logging.error("No se pudo obtener el token. Terminando.")
        return
    
    # Ejemplo de consulta de documento
    document_id = "ddab75e4-9763-46b6-814b-77e5bbae2893"
    resultado = consultar_estado_documento(token, document_id)
    
    if resultado:
        # Puedes procesar aquí la respuesta como necesites
        print("Respuesta completa:")
        print(resultado)
    else:
        logging.error("No se pudo consultar el estado del documento")

load_dotenv()  # Carga variables de entorno para credenciales

ESTADOS_DESCRIPCION = {
    "40": "Creado",
    "41": "Anulado",
    "42": "Actualizado",
    "46": "Generando XML",
    "47": "Generando ZIP",
    "48": "Enviado a DIAN",
    "49": "Esperando respuesta DIAN",
    "70": "Inválido",
    "72": "Válido",
    "73": "AD Generado PDF",
    "74": "AD Enviado a Cliente",
    "80": "Fallido",
    "90": "Aceptación Expresa",
    "91": "Recibo Bien o Servicio",
    "92": "Acusado",
    "93": "Aceptación Tácita",
    "94": "Reclamado",
    "96": "Contingencia",
    "97": "Pendiente por envío de correo",
    "98": "Pendiente por validación DIAN"
}

# Cargar credenciales de correo electrónico
def enviar_correo(errores, asunto):
    if errores:
        mensaje = "<html><body>"
        mensaje += "<h2 style='color: red; text-align: center;'>❗Errores en la base de datos para Factura Electrónica❗</h2>"
        mensaje += f"<h3>{asunto}</h3>"
        mensaje += "<table border='1' cellpadding='8' cellspacing='0' style='border-collapse: collapse; width: 100%; table-layout: fixed;'>"
        mensaje += "<tr><th>Documento</th><th>Nombre</th><th>Codigo</th><th>Error</th></tr>"

        for error in errores:
            partes = error.split('-')
            if len(partes) >= 3:
                documento = partes[0].strip()
                nombre = partes[1].strip()
                codigo = partes[2].strip()
                error_desc = partes[3].strip()
                mensaje += f"<tr><td>{documento}</td><td>{nombre}</td><td>{codigo}</td><td>{error_desc}</td></tr>"
            else:
                mensaje += f"<tr><td colspan='3'>{error}</td></tr>"  # Error fuera de formato

        mensaje += "</table>"
        mensaje += "<p>Por favor, corrija los errores mencionados.</p>"
        mensaje += "</body></html>"

        email = EmailMessage()
        email['From'] = os.getenv("EMAIL_USER")
        email['To'] = os.getenv("EMAIL_USER")#,"navila@nikkenlatam.com","mdiaz@nikkenlatam.com","aporras@nikkenlatam.com"
        email['Subject'] = asunto
        email.set_content(mensaje, subtype='html')

        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
                smtp.starttls()
                smtp.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"))
                smtp.send_message(email)
            logging.info(f"Correo enviado correctamente: {asunto}")
        except Exception as e:
            logging.error(f"Error enviando el correo: {e}")
    else:
        logging.info(f"No se encontraron errores en la base de datos para: {asunto}")

# Conexión a SQL Server - Primera Base de Datos
try:
    conn1 = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={os.getenv("SQL_SERVER")};'
        f'DATABASE={os.getenv("SQL_DATABASE")};'
        f'UID={os.getenv("SQL_USER")};'
        f'PWD={os.getenv("SQL_PASSWORD")};'
        'Timeout=10;'
    )
    cursor1 = conn1.cursor()
    logging.info("Conexión exitosa a la primera base de datos.")
except Exception as e:
    logging.error(f"Error en la conexión a la primera base de datos: {e}")
    exit()

errores_1 = []

# Consulta de la primera base de datos
query1 = """
SELECT DISTINCT 
    T0.[DocDate], 
    'Factura FV' AS tipo_documentoFE, 
    T0.[DocNum], 
    T0.[NumAtCard], 
    T0.[CardCode], 
    T0.[CardName] AS FV, 
    T2.[CardName], 
    T2.[LicTradNum], 
    T2.[U_BPCO_RTC], 
    T2.[U_BPCO_TDC], 
    T2.[U_BPCO_CS],
    T2.[U_BPCO_City], 
    T2.[U_BPCO_TP], 
    T2.[U_BPCO_Nombre], 
    T2.[U_BPCO_1Apellido],
    T2.[U_BPCO_2Apellido], 
    T2.[U_BPCO_BPExt], 
    T2.[U_BPCO_Address], 
    T2.[E_mail], 
    T2.[Phone1]
FROM OINV T0  
INNER JOIN INV1 T1 ON T0.[DocEntry] = T1.[DocEntry] 
INNER JOIN OCRD T2 ON T0.[CardCode] = T2.[CardCode] 
WHERE T0.[DocDate] BETWEEN '20250326' AND '20250326'

UNION ALL

SELECT DISTINCT 
    T0.[DocDate], 
    'Factura NC' AS tipo_documentoFE, 
    T0.[DocNum], 
    T0.[NumAtCard], 
    T0.[CardCode], 
    T0.[CardName] AS FV, 
    T2.[CardName], 
    T2.[LicTradNum], 
    T2.[U_BPCO_RTC], 
    T2.[U_BPCO_TDC], 
    T2.[U_BPCO_CS],
    T2.[U_BPCO_City], 
    T2.[U_BPCO_TP], 
    T2.[U_BPCO_Nombre], 
    T2.[U_BPCO_1Apellido],
    T2.[U_BPCO_2Apellido], 
    T2.[U_BPCO_BPExt], 
    T2.[U_BPCO_Address], 
    T2.[E_mail], 
    T2.[Phone1]
FROM ORIN T0  
INNER JOIN RIN1 T1 ON T0.[DocEntry] = T1.[DocEntry] 
INNER JOIN OCRD T2 ON T0.[CardCode] = T2.[CardCode] 
WHERE T0.[DocDate] BETWEEN '20250326' AND '20250326'

UNION ALL

SELECT DISTINCT 
    T0.[DocDate], 
    'Factura BOMC' AS tipo_documentoFE, 
    T0.[DocNum], 
    NULL AS [NumAtCard],  
    T0.[CardCode], 
    T2.[CardName] AS FV, 
    T2.[CardName], 
    T2.[LicTradNum], 
    NULL AS [U_BPCO_RTC],  
    NULL AS [U_BPCO_TDC], 
    NULL AS [U_BPCO_CS], 
    T2.[City] AS [U_BPCO_City], 
    NULL AS [U_BPCO_TP], 
    T2.[CardName] AS [U_BPCO_Nombre], 
    NULL AS [U_BPCO_1Apellido], 
    NULL AS [U_BPCO_2Apellido], 
    NULL AS [U_BPCO_BPExt], 
    T4.[Address] AS [U_BPCO_Address], 
    T2.[E_mail], 
    T2.[Phone1]
FROM [dbo].[ORPC] T0 
INNER JOIN [dbo].[NNM1] T1 ON T0.[Series] = T1.[Series] 
INNER JOIN [dbo].[OCRD] T2 ON T0.[CardCode] = T2.[CardCode] 
INNER JOIN RPC1 T3 ON T0.[DocEntry] = T3.[DocEntry] 
INNER JOIN CRD1 T4 ON T2.[CardCode] = T4.[CardCode] 
WHERE T0.[DocDate] BETWEEN '20250326' AND '20250326'
AND T1.[SeriesName] = 'Bonif_MC'

UNION ALL

SELECT DISTINCT 
    T0.[DocDate], 
    'Doc. Sopo' AS tipo_documentoFE, 
    T0.[DocNum], 
    NULL AS [NumAtCard],  
    T0.[CardCode], 
    T0.[CardName] AS FV, 
    T2.[CardName], 
    T2.[LicTradNum], 
    T2.[U_BPCO_RTC], 
    T2.[U_BPCO_TDC], 
    T2.[U_BPCO_CS],
    T2.[U_BPCO_City], 
    T2.[U_BPCO_TP], 
    T2.[U_BPCO_Nombre], 
    T2.[U_BPCO_1Apellido], 
    T2.[U_BPCO_2Apellido], 
    T2.[U_BPCO_BPExt], 
    T4.[Street] AS [U_BPCO_Address], 
    T2.[E_mail], 
    T2.[Phone1]
FROM OPCH T0  
INNER JOIN NNM1 T1 ON T0.[Series] = T1.[Series] 
INNER JOIN OCRD T2 ON T0.[CardCode] = T2.[CardCode] 
INNER JOIN PCH5 T3 ON T0.[DocEntry] = T3.[AbsEntry]
INNER JOIN CRD1 T4 ON T2.[CardCode] = T4.[CardCode] 
WHERE T0.[DocDate] BETWEEN '20250326' AND '20250326'
AND T1.[SeriesName] BETWEEN 'BRS' AND 'DE'
"""
cursor1.execute(query1)

# Validación de datos para la primera base
errores_1 = []
for row in cursor1:
    tipo_documentoFE = row[1]  
    doc_num = row[2]
    cardcode = row[4]
    nombre = row[5]
    correo = row[18]
    direccion = row[17] if row[17] else ''
    regimen = row[8]
    tipdocumento = row[9]
    documento = row[7]
    telefono = row[19] if len(row) > 19 else ''

    error_msg = []

    if direccion and "'" in direccion:
        error_msg.append("Comillas en la dirección")
    if not correo or not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", correo):
        error_msg.append("Correo inválido")
    if not regimen:
        error_msg.append("Falta el régimen")
    if not tipdocumento:
        error_msg.append("Falta el tipo de documento")
    if not documento:
        error_msg.append("Falta el documento")
    if not telefono or not any(char.isdigit() for char in telefono):
        error_msg.append("Teléfono no válido")

    if error_msg:
        errores_1.append(f"{tipo_documentoFE} {doc_num} - {nombre} - Código {cardcode} - {', '.join(error_msg)}")

enviar_correo(errores_1, "Errores en la base de datos SAP")

cursor1.close()
conn1.close()

# Conexión a SQL Server - Segunda Base de Datos
try:
    conn2 = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={os.getenv("SQL_SERVER_2")};'
        f'DATABASE={os.getenv("SQL_DATABASE_2")};'
        f'UID={os.getenv("SQL_USER_2")};'
        f'PWD={os.getenv("SQL_PASSWORD_2")};'
        'Timeout=10;'
    )
    cursor2 = conn2.cursor()
    logging.info("Conexión exitosa a la segunda base de datos.")
except Exception as e:
    logging.error(f"Error en la conexión a la segunda base de datos: {e}")
    exit()

errores_2 = []

# Consulta de la segunda base de datos
query2 = """
SELECT TipDoc, Series, DocNum, CardCode, CardName, FecEnvio, ProveeTec, docStatus 
FROM CtrlFacEleCol 
WHERE ProveeTec = 'cenet' 
AND FecEnvio BETWEEN '20250326' AND '20250326' 
AND docStatus NOT IN ('72', '73', '74')
"""
cursor2.execute(query2)

errores_2 = []
for row in cursor2:
    TipDoc = row[0]
    doc_num = row[2]
    cardname = row[4]
    cardcode = row[3]
    doc_status = str(row[7]).strip()  # Convertir a string y limpiar espacios

    # Log temporal para verificar el valor recibido
    print(f"Valor recibido para doc_status: '{doc_status}'")

    # Obtener la descripción del estado
    estado_descripcion = ESTADOS_DESCRIPCION.get(doc_status, "Descripción no encontrada")

    # Construir el mensaje de error
    errores_2.append(
        f"TipDoc {TipDoc} DocNum {doc_num} - {cardname} - {cardcode} - Estado: {doc_status} {estado_descripcion}"
    )

enviar_correo(errores_2, "Errores en la base de datos Tabla de Control")

cursor2.close()
conn2.close()

logging.info("Proceso finalizado.")