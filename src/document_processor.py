import logging
import os
import re
import time
from datetime import datetime, timedelta
from src.database import DatabaseConnection
from src.api_client import APIClient
from src.email_notifier import EmailNotifier
from config.settings import Settings, ESTADOS_DESCRIPCION
from collections import defaultdict

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
            # Obtener la fecha actual en formato YYYYMMDD para las consultas SQL
            fecha_actual = datetime.now().strftime('%Y%m%d')
            
            conn = DatabaseConnection.conectar_base_datos_1()
            cursor = conn.cursor()
            
            # SQL query para obtener los documentos de la base de datos SAP
            # Esta consulta obtiene información de varias tablas y filtra por fechas dinámicas
            # Se utiliza UNION ALL para combinar resultados de diferentes tipos de documentos
            query1 = f"""
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
            WHERE T0.[DocDate] BETWEEN '{fecha_actual}' AND '{fecha_actual}'
            UNION ALL
            SELECT DISTINCT 
                T0.[DocDate], 
                'Nota Credito' AS tipo_documentoFE, 
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
            WHERE T0.[DocDate] BETWEEN '{fecha_actual}' AND '{fecha_actual}'
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
            WHERE T0.[DocDate] BETWEEN '{fecha_actual}' AND '{fecha_actual}'
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
            WHERE T0.[DocDate] BETWEEN '{fecha_actual}' AND '{fecha_actual}'
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
                telefono = row[19].strip() if len(row) > 19 and row[19] else ''

                error_msg = []

                # 🔹 Validación de dirección (sin comillas simples)
                if direccion and "'" in direccion:
                    error_msg.append("Comillas en la dirección")

                # 🔹 Validación de correo
                if not correo or not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", correo):
                    error_msg.append("Correo inválido")

                # 🔹 Validaciones de datos requeridos
                if not regimen:
                    error_msg.append("Falta el régimen")
                # Validacion tipo de documento 
                if not tipdocumento:
                    error_msg.append("Falta el tipo de documento")
                # Validación de documento (sin tamaño valido para NIT)
                if not documento:
                    error_msg.append("Falta el documento")
                else:
                    # 🔹 Solo validar formato si el tipo de documento es "31"
                    if tipdocumento == "31":
                        if "-" in documento:
                            partes = documento.split("-")
                            if len(partes) != 2 or not partes[0].isdigit() or not partes[1].isdigit():
                                error_msg.append("Documento inválido (debe ser en formato XXXXXXXX-X o XXXXXXXXX-X)")
                            elif len(partes[0]) not in [8, 9]:
                                error_msg.append("Documento inválido (los dígitos antes del '-' deben ser 8 o 9)")
                        else:
                            error_msg.append("Documento inválido (debe contener '-X')")

                # 🔹 Validación de teléfono (Solo si no está vacío)
                if telefono:  
                    if '-' in telefono:
                        error_msg.append("Teléfono inválido: contiene '-'")
                    elif len(re.sub(r"\D", "", telefono)) < 6:  # Elimina no numéricos y verifica longitud
                        error_msg.append("Teléfono debe tener al menos 6 números")

                # 🔹 Si hay errores, los agregamos a la lista
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
            
            # Obtener la fecha actual en formato YYYYMMDD
            from datetime import datetime
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            
            # SQL query con fechas dinámicas
            query2 = f"""
            SELECT TipDoc, Series, DocNum, CardCode, CardName, FecEnvio, ProveeTec, docStatus 
            FROM CtrlFacEleCol 
            WHERE ProveeTec = 'cenet' 
            AND docStatus NOT IN ('72', '73', '74') AND FecEnvio BETWEEN '{fecha_actual}' AND '{fecha_actual}'
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
        
    # Método para comparar los documentos de SAP con CtrlFacEleCol
    # Este método compara los documentos obtenidos de SAP con los de CtrlFacEleCol
    @staticmethod
    def comparar_sap_ctl():
        try:
            logging.info("Iniciando comparación entre SAP y CtrlFacEleCol...")

            errores_sin_envio = []
            documentos_enviados = []

            # 1. Obtener documentos desde SAP con el query unificado
            conn_sap = DatabaseConnection.conectar_base_datos_1()
            cursor_sap = conn_sap.cursor()

            fecha_ayer = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

            query_sap = """
            SELECT 'FV' AS tipo_FE, DocNum, CardCode, CardName, VatSum, Doctotal 
            FROM OINV
            WHERE CAST(DocDate AS DATE) = ?
            UNION ALL
            SELECT 'NC' AS tipo_FE, DocNum, CardCode, CardName, VatSum, Doctotal 
            FROM ORIN
            WHERE CAST(DocDate AS DATE) = ?
            UNION ALL
            SELECT 'NCP' AS tipo_FE, DocNum, CardCode, CardName, VatSum, Doctotal 
            FROM ORPC
            WHERE CAST(DocDate AS DATE) = ?
            UNION ALL
            SELECT 'NDP' AS tipo_FE, DocNum, CardCode, CardName, VatSum, Doctotal 
            FROM ORIN
            WHERE CAST(DocDate AS DATE) = ?
            UNION ALL
            SELECT 'DS' AS tipo_FE, P.DocNum, P.CardCode, P.CardName, P.VatSum, P.Doctotal
            FROM OPCH P
            INNER JOIN NNM1 T1 ON P.Series = T1.Series
            WHERE T1.SeriesName BETWEEN 'BRS' AND 'DE' AND CAST(P.DocDate AS DATE) = ?
            """

            cursor_sap.execute(query_sap, (fecha_ayer,) * 5)
            sap_docs = cursor_sap.fetchall()
            cursor_sap.close()
            conn_sap.close()

            # 2. Obtener documentos desde CtrlFacEleCol generados AYER
            conn = DatabaseConnection.conectar_base_datos_2()
            cursor = conn.cursor()

            query_ctrl = """
            SELECT TipDoc, DocNum, CardCode, CardName, VatSum, Doctotal, docStatus
            FROM CtrlFacEleCol
            WHERE CAST(FecEnvio AS DATE) = ?
            """
            cursor.execute(query_ctrl, (fecha_ayer,))
            ctrl_docs = cursor.fetchall()

            ctrl_dict = {}
            for row in ctrl_docs:
                tip_doc = str(row[0]).strip()
                doc_num = str(row[1]).strip()
                card_code = str(row[2]).strip()
                card_name = row[3].strip() if row[3] else ""
                vat_sum = row[4]
                doc_total = row[5]
                try:
                    doc_status = int(str(row[6]).strip())
                except (ValueError, TypeError):
                    doc_status = None

                ctrl_dict[(tip_doc, doc_num, card_code)] = {
                    "CardName": card_name,
                    "VatSum": vat_sum,
                    "DocTotal": doc_total,
                    "docStatus": doc_status
                }

            # Comparar SAP vs CtrlFacEleCol
            for row in sap_docs:
                tip_doc = str(row[0]).strip()
                doc_num = str(row[1]).strip()
                card_code = str(row[2]).strip()
                card_name = row[3].strip() if row[3] else ""
                vat_sum = row[4]
                doc_total = row[5]
                clave = (tip_doc, doc_num, card_code)

                if clave not in ctrl_dict:
                    errores_sin_envio.append({
                        "TipDoc": tip_doc,
                        "DocNum": doc_num,
                        "CardCode": card_code,
                        "CardName": card_name,
                        "VatSum": vat_sum,
                        "DocTotal": doc_total,
                        "Error": "No enviado a DIAN"
                    })
                else:
                    info = ctrl_dict[clave]
                    if info["docStatus"] in [72, 73, 74]:
                        documentos_enviados.append({
                            "TipDoc": tip_doc,
                            "DocNum": doc_num,
                            "CardCode": card_code,
                            "CardName": info["CardName"],
                            "VatSum": info["VatSum"],
                            "DocTotal": info["DocTotal"],
                            "docStatus": info["docStatus"]  # 👈 aquí está el campo adicional
                        })

                    else:
                        errores_sin_envio.append({
                            "TipDoc": tip_doc,
                            "DocNum": doc_num,
                            "CardCode": card_code,
                            "CardName": info["CardName"],
                            "VatSum": info["VatSum"],
                            "DocTotal": info["DocTotal"],
                            "docStatus": info["docStatus"],  # 👈 se agrega este
                            "Error": f"Estado inválido: {info['docStatus']}"
                        })

            cursor.close()
            conn.close()

            return errores_sin_envio, documentos_enviados

        except Exception as e:
            logging.error(f"Error al comparar SAP con CtrlFacEleCol: {e}")
            return [], []

    # Método para procesar documentos pendientes
    @staticmethod
    def procesar_documentos_pendientes():
        token = APIClient.obtener_token()
        if not token:
            logging.error("No se pudo obtener el token. Terminando.")
            return

        try:
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            conn = DatabaseConnection.conectar_base_datos_2()

            with conn.cursor() as cursor:
                query_pendientes = """
                SELECT documentId, docStatus, Cufe, tipDoc
                FROM CtrlFacEleCol
                WHERE docStatus NOT IN ('72', '73', '74')
                AND tipDoc IN ('FV', 'NCP', 'NC', 'NDP')
                AND FecEnvio BETWEEN ? AND ?
                """
                cursor.execute(query_pendientes, (fecha_actual, fecha_actual))
                documentos_pendientes = cursor.fetchall()

                # Agrupamos por tipo de documento
                conteo_por_tipo = defaultdict(int)
                for _, _, _, tip_doc in documentos_pendientes:
                    conteo_por_tipo[tip_doc.strip()] += 1

                total = len(documentos_pendientes)
                logging.info(f"Se encontraron {total} documentos pendientes de FE, NCP, NC y NDP")
                for tipo, cantidad in conteo_por_tipo.items():
                    logging.info(f"  - {tipo}: {cantidad} documento(s)")

            for document_id, estado_actual, cufe_actual, tip_doc in documentos_pendientes:
                tipo_documento = DocumentProcessor.determinar_tipo_documento(tip_doc)

                if tipo_documento is None:
                    continue

                logging.info(f"Enviando DocumentID {document_id} con DocumentType {tipo_documento}")
                detalles_documento = APIClient.obtener_documento(token, document_id, tipo_documento)

                if detalles_documento:
                    DocumentProcessor.actualizar_estado_documento(cursor, conn, detalles_documento)

            conn.close()
            logging.info("Procesamiento de documentos pendientes completado")

        except Exception as e:
            logging.error(f"Error al procesar documentos pendientes: {e}")

    @staticmethod
    def procesar_documentos_pendientes_2():
        token = APIClient.obtener_token()
        if not token:
            logging.error("No se pudo obtener el token. Terminando.")
            return

        try:
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            conn = DatabaseConnection.conectar_base_datos_2()

            with conn.cursor() as cursor:
                query_pendientes = """
                SELECT documentId, docStatus, Cufe, tipDoc
                FROM CtrlFacEleCol
                WHERE docStatus NOT IN ('72', '73', '74')
                AND tipDoc IN ('DE', 'BRS', 'NCDS')
                AND FecEnvio BETWEEN ? AND ?
                """
                cursor.execute(query_pendientes, (fecha_actual, fecha_actual))
                documentos_pendientes = cursor.fetchall()

                # Agrupamos por tipo de documento
                conteo_por_tipo = defaultdict(int)
                for _, _, _, tip_doc in documentos_pendientes:
                    conteo_por_tipo[tip_doc.strip()] += 1

                total = len(documentos_pendientes)
                logging.info(f"Se encontraron {total} documentos pendientes de BRS, DE y NCDS")
                for tipo, cantidad in conteo_por_tipo.items():
                    logging.info(f"  - {tipo}: {cantidad} documento(s)")

            for document_id, estado_actual, cufe_actual, tip_doc in documentos_pendientes:
                tipo_documento = DocumentProcessor.determinar_tipo_documento(tip_doc)

                if tipo_documento is None:
                    continue

                logging.info(f"Enviando DocumentID {document_id} con DocumentType {tipo_documento}")
                detalles_documento = APIClient.obtener_documento(token, document_id, tipo_documento)

                if detalles_documento:
                    DocumentProcessor.actualizar_estado_documento_2(cursor, conn, detalles_documento)

            conn.close()
            logging.info("Procesamiento de documentos pendientes completado")

        except Exception as e:
            logging.error(f"Error al procesar documentos pendientes: {e}")

    @staticmethod
    def determinar_tipo_documento(tip_doc):
        mapping = {
            'FV': 1,
            'NCP': 1,
            'NC': 2,
            'NDP': 2,
            'BRS': 1,
            'DE': 1,
            'NCDS': 2
        }

        tip_doc = tip_doc.strip()
        tipo_documento = mapping.get(tip_doc)

        if tipo_documento is None:
            logging.warning(f"Tipo de documento inválido: '{tip_doc}'. No se enviará la solicitud.")

        return tipo_documento

    @staticmethod
    def actualizar_estado_documento(cursor, conn, documento):
        try:
            document_id = documento.get('DocumentID', '').strip()
            cufe = documento.get('CUFE', '').strip()
            doc_num = documento.get('DocumentNumber', '').strip()

            # Eliminamos las letras del número de documento
            doc_num = ''.join(filter(str.isdigit, doc_num))

            update_query = """
            UPDATE CtrlFacEleCol
            SET Cufe = ?, docStatus = '74'
            WHERE DocNum = ?
            """

            cursor.execute(update_query, (cufe, doc_num))
            conn.commit()
            logging.info(f"Documento {document_id} actualizado exitosamente con CUFE: {cufe} y DocumentNumber: {doc_num}")
            return True
        except Exception as e:
            logging.error(f"Error al actualizar documento {document_id}: {e}")
            conn.rollback()
            return False
    
    @staticmethod
    def actualizar_estado_documento_2(cursor, conn, documento):
        try:
            document_id = documento.get('DocumentID', '').strip()
            cufe = documento.get('CUFE', '').strip()
            doc_num = documento.get('DocumentNumber', '').strip()

            # Eliminamos las letras del número de documento
            doc_num = ''.join(filter(str.isdigit, doc_num))

            update_query = """
            UPDATE CtrlFacEleCol
            SET Cufe = ?, docStatus = '74'
            WHERE DocNum = ?
            """

            cursor.execute(update_query, (cufe, doc_num))  # Pasa solo dos parámetros

            conn.commit()
            logging.info(f"Documento {document_id} actualizado exitosamente con CUFE: {cufe} y DocumentNumber: {doc_num}")
            return True
        except Exception as e:
            logging.error(f"Error al actualizar documento {document_id}: {e}")
            conn.rollback()
            return False
        
    # Método para eliminar documentos sin estado en la tabla de control
    @staticmethod
    def eliminar_documento_por_clave():
        try:
            conn = DatabaseConnection.conectar_base_datos_2()
            with conn.cursor() as cursor:
                # Consultar todos los documentos con docstatus NULL
                select_query = """
                SELECT tipdoc, docnum FROM CtrlFacEleCol WHERE docstatus IS NULL
                """
                cursor.execute(select_query)
                documentos = cursor.fetchall()

                total = len(documentos)
                if total == 0:
                    logging.info("✅ No hay documentos con docstatus NULL para eliminar.")
                    return

                logging.info(f"📋 Se encontraron {total} documentos con docstatus NULL. Procediendo a eliminarlos...")

                delete_query = """
                DELETE FROM CtrlFacEleCol WHERE tipdoc = ? AND docnum = ? AND docstatus IS NULL
                """
                eliminados = 0
                for tipdoc, docnum in documentos:
                    cursor.execute(delete_query, (tipdoc, docnum))
                    eliminados += cursor.rowcount

                conn.commit()
                logging.info(f"🗑️ Se eliminaron {eliminados} documentos sin estado correctamente.")

            conn.close()

        except Exception as e:
            logging.error(f"❌ Error al eliminar documentos sin estado: {e}")
            if conn:
                conn.rollback()
                conn.close()