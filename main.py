import logging
import os
from config.settings import *
from src.document_processor import DocumentProcessor
from src.email_notifier import EmailNotifier

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

def main():
    configurar_logging()
    
    try:
        # Procesar documentos pendientes
        DocumentProcessor.procesar_documentos_pendientes()
        
        # Validar y enviar errores de base de datos
        errores_1 = DocumentProcessor.procesar_base_datos_1()
        EmailNotifier.enviar_correo(errores_1, "Errores en la base de datos SAP")
        
        errores_2 = DocumentProcessor.procesar_base_datos_2()
        EmailNotifier.enviar_correo(errores_2, "Errores en la base de datos Tabla de Control")
        
        logging.info("Proceso finalizado exitosamente.")
    
    except Exception as e:
        logging.error(f"Error fatal en el proceso: {e}")

if __name__ == "__main__":
    main()