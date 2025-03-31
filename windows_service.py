import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import time
import logging
import subprocess

# Configuración de logging
log_file = os.path.join(r'C:\Users\wgacol\Documents\Proyecto\COL\validacion FE\logs', 'servicio.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ajusta esta ruta al directorio donde está tu script
SCRIPT_DIR = r'C:\Users\wgacol\Documents\Proyecto\COL\validacion FE\dist'
# Ajusta esto al nombre de tu script
SCRIPT_NAME = r'validacionFE.py'

class AppService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ValidacionFEService"
    _svc_display_name_ = "Validación de Documentos Electrónicos"
    _svc_description_ = "Servicio que ejecuta la verificación de documentos electrónicos"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True
        logging.info("Servicio inicializado")

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False
        logging.info("Servicio detenido")

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        logging.info("Servicio iniciado")
        self.main()

    def main(self):
        while self.is_running:
            try:
                logging.info(f"Ejecutando script {SCRIPT_NAME} en {SCRIPT_DIR}")
                
                # Cambiar al directorio de tu script
                os.chdir(SCRIPT_DIR)
                
                # Ejecutar el script usando subprocess
                process = subprocess.Popen(
                    ['python', SCRIPT_NAME],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                stdout, stderr = process.communicate()

                # Revisar si hubo algún error
                if process.returncode != 0:
                    logging.error(f"Error al ejecutar el script: {stderr.decode()}")
                else:
                    logging.info(f"Salida del script:\n{stdout.decode()}")
                
                # Comprobar si se ha solicitado detener el servicio
                rc = win32event.WaitForSingleObject(self.hWaitStop, 0)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                
                # Esperar un tiempo determinado antes de volver a ejecutar
                logging.info("Esperando para la próxima ejecución...")
                
                # Esperar en bloques de 10 segundos, comprobando si se solicita parar
                remaining_time = 3600  # 1 hora (ajusta según sea necesario)
                while remaining_time > 0 and self.is_running:
                    wait_time = min(10, remaining_time)  # Esperar como máximo 10 segundos
                    rc = win32event.WaitForSingleObject(self.hWaitStop, wait_time * 1000)
                    if rc == win32event.WAIT_OBJECT_0:
                        # Se ha solicitado detener el servicio
                        logging.info("Interrupción recibida durante espera")
                        break
                    remaining_time -= wait_time
            
            except Exception as e:
                logging.error(f"Error en el servicio: {str(e)}")
                # Esperar un poco antes de intentar de nuevo
                time.sleep(60)  # Esperar 1 minuto en caso de error

        logging.info("Bucle principal del servicio finalizado")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(AppService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(AppService)