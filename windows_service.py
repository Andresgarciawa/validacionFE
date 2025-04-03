import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import time
import logging

# Configuración de logging
log_dir = r'C:\Users\wgacol\Documents\Proyecto\COL\validacion FE\logs'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'servicio_validacion.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuración del script a ejecutar
SCRIPT_DIR = r'C:\Users\wgacol\Documents\Proyecto\COL\validacion FE\dist'
SCRIPT_NAME = r'validacionFE.py'

class ValidacionService(win32serviceutil.ServiceFramework):
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
                
                # Ejecutar tu script con la ruta completa al intérprete de Python
                python_exe = sys.executable
                exit_code = os.system(f'"{python_exe}" {SCRIPT_NAME}')
                
                if exit_code != 0:
                    logging.error(f"El script terminó con código de error: {exit_code}")
                else:
                    logging.info("Script ejecutado correctamente")
                
                # Comprobar si se ha solicitado detener el servicio
                rc = win32event.WaitForSingleObject(self.hWaitStop, 0)
                if rc == win32event.WAIT_OBJECT_0:
                    break
                
                # Esperar antes de la siguiente ejecución
                logging.info("Esperando para la próxima ejecución...")
                
                # Esperar en intervalos cortos para responder rápidamente a solicitudes de parada
                for _ in range(360):  # 3600 segundos = 1 hora, en intervalos de 10 segundos
                    if not self.is_running:
                        break
                    rc = win32event.WaitForSingleObject(self.hWaitStop, 10000)  # 10 segundos
                    if rc == win32event.WAIT_OBJECT_0:
                        break
                
            except Exception as e:
                logging.error(f"Error en el servicio: {str(e)}")
                time.sleep(60)  # Esperar 1 minuto en caso de error

        logging.info("Bucle principal del servicio finalizado")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ValidacionService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(ValidacionService)