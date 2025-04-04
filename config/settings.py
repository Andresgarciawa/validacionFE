import os
from typing import List, Dict
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Imprimir variables para verificar si están cargadas
print("API_LOGIN_URL:", os.getenv("API_LOGIN_URL"))
print("SQL_SERVER:", os.getenv("SQL_SERVER"))

class ConfigurationError(Exception):
    """Excepción personalizada para errores de configuración."""
    pass

class Settings:
    """
    Clase centralizada de configuraciones con validación de variables de entorno.
    """
    # Configuraciones de Base de Datos
    @classmethod
    def _validate_db_config(cls):
        """Validar configuraciones de bases de datos."""
        db_configs = [
            ('SQL_SERVER', 'SQL_DATABASE', 'SQL_USER', 'SQL_PASSWORD'),
            ('SQL_SERVER_2', 'SQL_DATABASE_2', 'SQL_USER_2', 'SQL_PASSWORD_2')
        ]
        
        for config_set in db_configs:
            for config in config_set:
                if not os.getenv(config):
                    raise ConfigurationError(f"Configuración de base de datos faltante: {config}")

    # Bases de datos
    SQL_SERVER = os.getenv('SQL_SERVER')
    SQL_DATABASE = os.getenv('SQL_DATABASE')
    SQL_USER = os.getenv('SQL_USER')
    SQL_PASSWORD = os.getenv('SQL_PASSWORD')
    SQL_SERVER_2 = os.getenv('SQL_SERVER_2')
    SQL_DATABASE_2 = os.getenv('SQL_DATABASE_2')
    SQL_USER_2 = os.getenv('SQL_USER_2')
    SQL_PASSWORD_2 = os.getenv('SQL_PASSWORD_2')

    # Configuración de API
    API_LOGIN_URL = os.getenv('API_LOGIN_URL')
    API_STATUS_URL = os.getenv('API_STATUS_URL')
    API_STATUS_URL_2 = os.getenv('API_STATUS_URL_2')
    API_DOCUMENT_URL = os.getenv('API_DOCUMENT_URL')
    API_DOCUMENT_URL_2 = os.getenv('API_DOCUMENT_URL_2')
    API_USERNAME = os.getenv('API_USERNAME')
    API_PASSWORD = os.getenv('API_PASSWORD')
    API_SCHEMAID = os.getenv('API_SCHEMAID')
    API_IDNUMBER = os.getenv('API_IDNUMBER')

    # Correo electrónico
    EMAIL_USER = os.getenv('EMAIL_USER')
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
    EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',')

    @classmethod
    def validate_configuration(cls):
        """
        Método para validar toda la configuración antes de iniciar la aplicación.
        """
        try:
            cls._validate_db_config()
            
            # Validar configuraciones críticas
            if not cls.API_USERNAME or not cls.API_PASSWORD:
                raise ConfigurationError("Credenciales de API incompletas")
            
            if not cls.EMAIL_USER or not cls.EMAIL_PASSWORD:
                raise ConfigurationError("Configuración de correo incompleta")
            
            # Otras validaciones específicas
            if not cls.API_LOGIN_URL or not cls.API_STATUS_URL:
                raise ConfigurationError("URLs de API incompletas")
            
        except ConfigurationError as e:
            print(f"Error de configuración: {e}")
            raise

# Estados de documentos
ESTADOS_DESCRIPCION: Dict[str, str] = {
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

# Validar configuración al importar
Settings.validate_configuration()