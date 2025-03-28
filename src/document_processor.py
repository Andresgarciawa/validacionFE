import logging
import os
import re
import time
from datetime import datetime
from src.database import DatabaseConnection
from src.api_client import APIClient
from src.email_notifier import EmailNotifier
from config.settings import Settings, ESTADOS_DESCRIPCION

class DocumentProcessor:

    # Método para validar las variables de entorno necesarias
    # Este método verifica si las variables de entorno requeridas están configuradas
    @staticmethod
    def validar_variables_entorno():
        variables_requeridas = ["DB1_HOST", "DB1_USER", "DB1_PASS", "DB2_HOST", "DB2_USER", "DB2_PASS", "API_URL", "API_TOKEN"]
        faltantes = [var for var in variables_requeridas if not os.getenv(var)]

        if faltantes:
            logging.error(f"Faltan las siguientes variables de entorno: {', '.join(faltantes)}")
            return False

    # Método para procesar la base de datos 1 y validar los datos
    # Este método se conecta a la base de datos SAP y ejecuta una consulta SQL para obtener los documentos
    @staticmethod
    def procesar_base_datos_1():
        try:
            conn = DatabaseConnection.conectar_base_datos_1()
            cursor = conn.cursor()
            
            # SQL query para obtener los documentos de la base de datos SAP
            # Esta consulta obtiene información de varias tablas y filtra por fechas específicas
            # Se utiliza UNION ALL para combinar resultados de diferentes tipos de documentos
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
            WHERE T0.[DocDate] BETWEEN '20250328' AND '20250328'

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
            WHERE T0.[DocDate] BETWEEN '20250328' AND '20250328'

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
            WHERE T0.[DocDate] BETWEEN '20250328' AND '20250328'
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
            WHERE T0.[DocDate] BETWEEN '20250328' AND '20250328'
            AND T1.[SeriesName] BETWEEN 'BRS' AND 'DE'
            """
            
            cursor.execute(query1)
            errores_1 = []
            
            for row in cursor:
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
                if not telefono or not any(char.isdigit() for char in telefono) or \
                ('-' in telefono and len(telefono) != 8) or \
                ('-' not in telefono and len(telefono) < 6):
                    error_msg.append("Teléfono no válido")

                if error_msg:
                    errores_1.append(f"{tipo_documentoFE} {doc_num} - {nombre} - Código {cardcode} - {', '.join(error_msg)}")

            cursor.close()
            conn.close()
            
            return errores_1
        
        except Exception as e:
            logging.error(f"Error procesando base de datos 1: {e}")
            return []

    @staticmethod
    def procesar_base_datos_2():
        try:
            conn = DatabaseConnection.conectar_base_datos_2()
            cursor = conn.cursor()
            
            # SQL query para obtener los documentos de la base de datos de control
            # Esta consulta obtiene información de la tabla CtrlFacEleCol y filtra por fechas específicas
            query2 = """
            SELECT TipDoc, Series, DocNum, CardCode, CardName, FecEnvio, ProveeTec, docStatus 
            FROM CtrlFacEleCol 
            WHERE ProveeTec = 'cenet' 
            AND docStatus NOT IN ('72', '73', '74') AND FecEnvio BETWEEN '20250328' AND '20250328'
            """
            
            cursor.execute(query2)
            errores_2 = []
            
            for row in cursor:
                TipDoc = row[0]
                doc_num = row[2]
                cardname = row[4]
                cardcode = row[3]
                doc_status = str(row[7]).strip()
                
                estado_descripcion = ESTADOS_DESCRIPCION.get(doc_status, "Descripción no encontrada")
                errores_2.append(
                    f"TipDoc {TipDoc} DocNum {doc_num} - {cardname} - {cardcode} - Estado: {doc_status} {estado_descripcion}"
                )
            
            cursor.close()
            conn.close()
            
            return errores_2
        
        except Exception as e:
            logging.error(f"Error procesando base de datos 2: {e}")
            return []

    @staticmethod
    def procesar_documentos_pendientes():
        token = APIClient.obtener_token()

        if not token:
            logging.error("No se pudo obtener el token. Terminando.")
            return

        try:
            conn = DatabaseConnection.conectar_base_datos_2()
            cursor = conn.cursor()

            # SQL query para obtener documentos pendientes
            query_pendientes = """
            SELECT documentId, docStatus, Cufe, tipDoc
            FROM CtrlFacEleCol
            WHERE docStatus NOT IN ('72', '73', '74')
            AND TipDoc NOT IN ('BRS', 'DE') AND FecEnvio BETWEEN '2025-03-28' AND '2025-03-28'
            """

            cursor.execute(query_pendientes)
            documentos_pendientes = cursor.fetchall()

            logging.info(f"Se encontraron {len(documentos_pendientes)} documentos pendientes")

            for documento in documentos_pendientes:
                document_id = documento[0]
                estado_actual = documento[1]
                cufe_actual = documento[2]
                tip_doc = documento[3]

                if tip_doc in ['FV', 'NCP']:
                    tipo_documento = 'FV'
                elif tip_doc in ['NC', 'NDP']:
                    tipo_documento = 'NC'
                else:
                    logging.warning(f"Tipo de documento desconocido: {tip_doc}. Se usará 'FV' por defecto.")
                    tipo_documento = 'FV'

                detalles_documento = APIClient.obtener_documento(token, document_id, tipo_documento)

                if detalles_documento:
                    nuevo_estado = detalles_documento.get('estadoDian', '')
                    nuevo_cufe = detalles_documento.get('cufe', '')
                    dian_errors = detalles_documento.get('DIANErrors', '')
                    docnum = detalles_documento.get('DocumentNumber', '').strip().replace(" ", "").translate(str.maketrans('', '', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'))

                    if nuevo_estado != estado_actual or nuevo_cufe != cufe_actual:
                        DocumentProcessor.actualizar_estado_documento(cursor, conn, detalles_documento)

                    if dian_errors and docnum:
                        update_query = "UPDATE CtrlFacEleCol SET RespDian = ? WHERE docnum = ?"
                        cursor.execute(update_query, (dian_errors, docnum))

            cursor.close()
            conn.close()

            logging.info("Procesamiento de documentos pendientes completado")

        except Exception as e:
            logging.error(f"Error general al procesar documentos pendientes: {e}")


    @staticmethod
    def actualizar_estado_documento(cursor, conn, documento):
        try:
            document_id = documento.get('documentId', '')
            cufe = documento.get('cufe', '')
            doc_num = documento.get('DocumentNumber', '')

            # Actualizar el estado y CUFE en la base de datos
            update_query = """
            UPDATE CtrlFacEleCol
            SET Cufe = %s,
                docStatus = '74'
            WHERE DocNum = %s
            """

            cursor.execute(update_query, (cufe, doc_num))
            conn.commit()

            logging.info(f"Documento {document_id} actualizado exitosamente con CUFE: {cufe}")
            return True

        except Exception as e:
            logging.error(f"Error al actualizar documento {document_id}: {e}")
            conn.rollback()
            return False