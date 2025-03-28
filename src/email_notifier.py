import os
import logging
import smtplib
import threading
import time
from email.message import EmailMessage
from config.settings import Settings

class EmailNotifier:
    @staticmethod
    def enviar_correo(errores, asunto):
        if errores:
            mensaje = EmailNotifier._crear_mensaje_html(errores, asunto)
            
            email = EmailMessage()
            email['From'] = Settings.EMAIL_USER
            email['To'] = Settings.EMAIL_RECIPIENTS
            email['Subject'] = asunto
            email.set_content(mensaje, subtype='html')
            
            try:
                with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
                    smtp.starttls()
                    smtp.login(Settings.EMAIL_USER, Settings.EMAIL_PASSWORD)
                    smtp.send_message(email)
                
                logging.info(f"Correo enviado correctamente: {asunto}")
            except Exception as e:
                logging.error(f"Error enviando el correo: {e}")
        else:
            logging.info(f"No se encontraron errores en la base de datos para: {asunto}")

    @staticmethod
    def _crear_mensaje_html(errores, asunto):
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
                mensaje += f"<tr><td colspan='3'>{error}</td></tr>"
        
        mensaje += "</table>"
        mensaje += "<p>Por favor, corrija los errores mencionados.</p>"
        mensaje += "</body></html>"
        
        return mensaje

# ------------------------ PROGRAMAR TAREAS CON INTERVALOS ------------------------

def ejecutar_cada(intervalo, funcion, *args):
    while True:
        funcion(*args)
        time.sleep(intervalo)

if __name__ == "__main__":
    logging.basicConfig(
        filename='log_validacion.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Iniciar los hilos con diferentes tiempos
    threading.Thread(target=ejecutar_cada, args=(86400, EmailNotifier.enviar_correo, ["Error1", "Error2"], "Errores en SAP")).start()  # Cada 5 minutos
    threading.Thread(target=ejecutar_cada, args=(1000, EmailNotifier.enviar_correo, ["Error3", "Error4"], "Errores en Tabla de Control")).start()  # Cada 10 minutos

    logging.info("El proceso de envío se ha iniciado correctamente.")
