import logging
import os
import time
import threading
from config.settings import *
from src.document_processor import DocumentProcessor
from src.email_notifier import EmailNotifier
from datetime import datetime, timedelta

def configurar_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'documento_request.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )

# Proceso regular cada 60 minutos
def ejecutar_proceso_regular():
    while True:
        try:
            logging.info("⏱️ Iniciando proceso regular de validación...")

            DocumentProcessor.procesar_documentos_pendientes()
            DocumentProcessor.procesar_documentos_pendientes_2()

            errores_1 = DocumentProcessor.procesar_base_datos_1()
            EmailNotifier.enviar_correo(errores_1, "Errores en la base de datos SAP")

            errores_2 = DocumentProcessor.procesar_base_datos_2()
            EmailNotifier.enviar_correo(errores_2, "Errores en la base de datos Tabla de Control")

            logging.info("✅ Proceso regular finalizado correctamente.")

        except Exception as e:
            logging.error(f"❌ Error en el proceso regular: {e}")

        logging.info("🕒 Esperando 60 minutos para la siguiente ejecución del proceso regular...")
        time.sleep(3600)  # 60 minutos

# Comparación separada cada 24 horas
def ejecutar_comparacion_diaria():
    while True:
        ahora = datetime.now()
        hora_objetivo = ahora.replace(hour=9, minute=0, second=0, microsecond=0)

        if ahora >= hora_objetivo:
            # Si ya pasaron las 9:00 AM de hoy, se programa para mañana a las 9:00 AM
            hora_objetivo += timedelta(days=1)

        tiempo_espera = (hora_objetivo - ahora).total_seconds()
        logging.info(f"🕒 Esperando hasta las 9:00 AM para iniciar comparación diaria (esperando {tiempo_espera/60:.1f} minutos)...")
        time.sleep(tiempo_espera)

        try:
            logging.info("📊 Iniciando proceso de comparación diaria...")

            registros_con_errores, registros_enviados = DocumentProcessor.comparar_sap_ctl()
            if registros_con_errores or registros_enviados:
                fecha_actual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                EmailNotifier.enviar_comparacion(fecha_actual, registros_con_errores, registros_enviados)

            logging.info("✅ Comparación diaria finalizada.")

        except Exception as e:
            logging.error(f"❌ Error en el proceso de comparación diaria: {e}")

        # Espera un minuto antes de calcular la siguiente espera (para evitar ejecuciones múltiples si toma muy poco tiempo)
        time.sleep(60)

def main():
    configurar_logging()

    # Crear y arrancar los dos hilos
    hilo_regular = threading.Thread(target=ejecutar_proceso_regular, daemon=True)
    hilo_comparacion = threading.Thread(target=ejecutar_comparacion_diaria, daemon=True)

    hilo_regular.start()
    hilo_comparacion.start()

    # Mantener vivo el hilo principal
    hilo_regular.join()
    hilo_comparacion.join()

if __name__ == "__main__":
    main()
