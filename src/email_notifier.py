import os
import logging
import smtplib
import threading
import time
from email.message import EmailMessage
from config.settings import Settings
from datetime import datetime, timedelta

class EmailNotifier:
    @staticmethod
    def enviar_correo(errores, asunto):
        # Añadir fecha al asunto
        fecha_actual = datetime.now().strftime('%d/%m/%Y' + ' ' + '%H:%M:%S')
        asunto_con_fecha = f"{asunto} - {fecha_actual}"
        
        if errores:
            mensaje = EmailNotifier._crear_mensaje_html(errores, asunto_con_fecha)
            
            email = EmailMessage()
            email['From'] = Settings.EMAIL_USER
            email['To'] = Settings.EMAIL_RECIPIENTS
            email['Subject'] = asunto_con_fecha
            email.set_content(mensaje, subtype='html')
            
            try:
                with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
                    smtp.starttls()
                    smtp.login(Settings.EMAIL_USER, Settings.EMAIL_PASSWORD)
                    smtp.send_message(email)
                
                logging.info(f"Correo enviado correctamente: {asunto_con_fecha}")
            except Exception as e:
                logging.error(f"Error enviando el correo: {e}")
        else:
            logging.info(f"No se encontraron errores en la base de datos para: {asunto_con_fecha}")
    
    @staticmethod
    def _crear_mensaje_html(errores, asunto):
        # Definir la paleta de colores para prioridades
        colores_prioridad = {
            'alta': '#ff3333',     # Rojo para errores críticos
            'media': '#fff301',    # Amarillo para errores importantes
            'baja': '#77fbe7'      # Verde claro para errores menores
        }
        
        mensaje = "<html><body>"
        mensaje += "<h2 style='color: red; text-align: center;'>❗Errores en la base de datos para Factura Electrónica❗</h2>"
        mensaje += f"<h3>{asunto}</h3>"
        mensaje += "<p>Leyenda de prioridades:</p>"
        mensaje += f"<div style='margin-bottom: 10px;'>"
        mensaje += f"<span style='background-color: {colores_prioridad['alta']}; padding: 3px 10px; margin-right: 10px;'>Alta prioridad</span>"
        mensaje += f"<span style='background-color: {colores_prioridad['media']}; padding: 3px 10px; margin-right: 10px;'>Media prioridad</span>"
        mensaje += f"<span style='background-color: {colores_prioridad['baja']}; padding: 3px 10px;'>Baja prioridad</span>"
        mensaje += "</div>"
        mensaje += "<table border='1' cellpadding='8' cellspacing='0' style='border-collapse: collapse; width: 100%; table-layout: fixed;'>"
        mensaje += "<tr style='background-color: #5eff33; color: white;'><th>Documento</th><th>Nombre</th><th>Código</th><th>Error</th></tr>"
        
        for i, error in enumerate(errores):
            partes = error.split('-')
            if len(partes) >= 3:
                documento = partes[0].strip()
                nombre = partes[1].strip()
                codigo = partes[2].strip()
                error_desc = partes[3].strip() if len(partes) > 3 else ""
                
                # Determinar la prioridad basada en el tipo de error
                prioridad = 'baja'  # Por defecto
                
                # Determinar prioridad basada en palabras clave o condiciones
                # Aquí puedes personalizar la lógica según tus necesidades
                error_lower = error_desc.lower()
                if "falta el documento" in error_lower or "correo inválido" in error_lower:
                    prioridad = 'baja'
                elif "falta el régimen" in error_lower or "falta el tipo de documento" in error_lower:
                    prioridad = 'media'
                elif "Comillas en la dirección" in error_lower or "Teléfono no válido" in error_lower:
                    prioridad = 'alta'
                elif "Error en la base de datos" in error_lower or "Error de conexión" in error_lower:
                    prioridad = 'alta'
                elif "70 Inválido" in error_lower or "97 Pendiente por envío de correo" in error_lower:
                    prioridad = 'alta'
                elif "Descripción no encontrada" in error_lower or "98 Pendiente por validación DIAN" in error_lower:
                    prioridad = 'alta'
                
                # Alternar colores de fondo para mejorar la legibilidad
                color_fondo = colores_prioridad[prioridad]
                
                # Ajustar color de texto para mejor contraste
                color_texto = 'black' if prioridad != 'alta' else 'black'
                
                mensaje += f"<tr style='background-color: {color_fondo}; color: {color_texto};'>"
                mensaje += f"<td>{documento}</td><td>{nombre}</td><td>{codigo}</td><td>{error_desc}</td></tr>"
            else:
                mensaje += f"<tr><td colspan='4'>{error}</td></tr>"
        
        mensaje += "</table>"
        mensaje += "<p>Por favor, corrija los errores mencionados lo antes posible.</p>"
        mensaje += "<p><i>Este correo fue generado automáticamente. No responda a este mensaje.</i></p>"
        mensaje += "</body></html>"
        
        return mensaje


# ------------------------ CORREO DE COMPARACION ------------------------
    @staticmethod
    def enviar_comparacion(registros_con_errores, registros_enviados):
        from datetime import datetime, timedelta
        fecha_anterior = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        asunto = f"Comparación de documentos SAP y CTL - {fecha_anterior}"

        enviados = 0
        no_enviados = 0

        registros = registros_enviados + registros_con_errores  # Mezclamos todos los registros

        mensaje = "<html><body>"
        mensaje += "<h2 style='color: #003366;'>📊 Resultado de la comparación de documentos</h2>"
        mensaje += f"<p>Fecha del proceso: <strong>{fecha_anterior}</strong></p>"

        mensaje += """
        <table border='1' cellpadding='6' cellspacing='0' style='border-collapse: collapse; width: 100%;'>
            <tr style='background-color: #5FFBF1;'>
                <th>DocNum</th><th>CardName</th><th>CardCode</th><th>Iva</th><th>Total</th><th>docStatus</th><th>Estado</th>
            </tr>
        """

        for row in registros:
            iva_formateado = f"${row['VatSum']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            total_formateado = f"${row['DocTotal']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            estado = row.get('docStatus')
            if estado in (73, 72, 74, 90, 91, 92, 93):
                estado_texto = "<span style='color: green;'>Enviado</span>"
                enviados += 1
            else:
                estado_texto = "<span style='color: red;'>No enviado</span>"
                no_enviados += 1

            mensaje += f"""
                <tr>
                    <td>{row['DocNum']}</td>
                    <td>{row['CardName']}</td>
                    <td>{row['CardCode']}</td>
                    <td>{iva_formateado}</td>
                    <td>{total_formateado}</td>
                    <td>{row.get('docStatus', 'N/A')}</td>
                    <td>{estado_texto}</td>
                </tr>
            """

        mensaje += "</table><br>"

        # Resumen final
        mensaje += f"""
        <h3>📌 Resumen del envío</h3>
        <ul>
            <li style='color: green;'>✅ Documentos enviados: <strong>{enviados}</strong></li>
            <li style='color: red;'>❌ Documentos no enviados: <strong>{no_enviados}</strong></li>
            <li>📄 Total de documentos procesados: <strong>{len(registros)}</strong></li>
        </ul>
        """

        if not registros:
            mensaje += "<p>No se encontraron registros para el día de hoy.</p>"

        mensaje += "<p><i>Este correo fue generado automáticamente.</i></p>"
        mensaje += "</body></html>"

        # Logs
        logging.info(f"📧 Preparando correo para: {Settings.EMAIL_RECIPIENTS}")
        logging.info(f"Asunto: {asunto}")
        logging.info(f"Cantidad de registros procesados: {len(registros)}")
        logging.info(f"Vista previa del correo (HTML truncado): {mensaje[:300]}...")

        if not Settings.EMAIL_USER or not Settings.EMAIL_PASSWORD:
            logging.error("❌ Faltan credenciales de correo en el archivo .env.")
            return

        if not Settings.EMAIL_RECIPIENTS:
            logging.error("❌ No hay destinatarios definidos en EMAIL_RECIPIENTS.")
            return

        # Envío de correo
        email = EmailMessage()
        email['From'] = Settings.EMAIL_USER
        email['To'] = Settings.EMAIL_RECIPIENTS
        email['Subject'] = asunto
        email.set_content(mensaje, subtype='html')

        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
                smtp.set_debuglevel(1)
                smtp.starttls()
                smtp.login(Settings.EMAIL_USER, Settings.EMAIL_PASSWORD)
                smtp.send_message(email)

            logging.info("✅ Correo de comparación enviado correctamente.")
        except Exception as e:
            logging.error(f"❌ Error enviando correo de comparación: {e}")

# ------------------------ PROGRAMAR TAREAS CON INTERVALOS ------------------------

# def ejecutar_cada(intervalo, funcion, *args):
#     while True:
#         funcion(*args)
#         time.sleep(intervalo)

# if __name__ == "__main__":
#     logging.basicConfig(
#         filename='log_validacion.txt',
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s'
#     )

#     # Iniciar los hilos con diferentes tiempos
#     threading.Thread(target=ejecutar_cada, args=(86400, EmailNotifier.enviar_correo, ["Error1", "Error2"], "Errores en SAP")).start()  # Cada 5 minutos
#     threading.Thread(target=ejecutar_cada, args=(1000, EmailNotifier.enviar_correo, ["Error3", "Error4"], "Errores en Tabla de Control")).start()  # Cada 10 minutos

#     logging.info("El proceso de envío se ha iniciado correctamente.")
