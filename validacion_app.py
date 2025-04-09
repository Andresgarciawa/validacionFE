import os
import sys
import time
import logging

# Configuración de logging
log_dir = r'C:\validacion FE\logs'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'servicio_validacion.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración del ejecutable a ejecutar
SCRIPT_DIR = r'C:\validacion FE'
SCRIPT_NAME = r'ValidacionApp.exe'

def ejecutar_script():
    """Ejecuta el ejecutable de validación y registra el resultado."""
    try:
        ruta_completa = os.path.join(SCRIPT_DIR, SCRIPT_NAME)

        # Verificar si el ejecutable existe
        if not os.path.isfile(ruta_completa):
            logging.error(f"❌ El ejecutable no fue encontrado en la ruta: {ruta_completa}")
            return  # No continúa si no existe

        logging.info(f"Ejecutando {SCRIPT_NAME} en {SCRIPT_DIR}")

        # Cambiar al directorio del ejecutable
        os.chdir(SCRIPT_DIR)

        # Ejecutar el ejecutable directamente
        exit_code = os.system(f'"{SCRIPT_NAME}"')

        if exit_code != 0:
            logging.error(f"El ejecutable terminó con código de error: {exit_code}")
        else:
            logging.info("✅ Ejecutable ejecutado correctamente")

    except Exception as e:
        logging.error(f"⚠️ Error ejecutando el ejecutable: {str(e)}")

def main():
    logging.info("🟢 Aplicación de validación iniciada.")

    try:
        while True:
            ejecutar_script()
            logging.info("⏳ Esperando 10 minutos para la próxima ejecución...")
            time.sleep(600)  # Espera 10 minutos antes de la siguiente ejecución

    except KeyboardInterrupt:
        logging.info("🔴 Aplicación detenida manualmente.")

if __name__ == "__main__":
    main()
