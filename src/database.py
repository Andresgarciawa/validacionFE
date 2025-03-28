import pyodbc
import logging
from typing import Tuple, Optional
from contextlib import contextmanager
from datetime import datetime
from config.settings import Settings

class DatabaseConnectionError(Exception):
    """Excepción personalizada para errores de conexión a base de datos."""
    pass

class DatabaseConnection:
    @classmethod
    def _crear_cadena_conexion(
        cls, 
        server: str, 
        database: str, 
        username: str, 
        password: str
    ) -> str:
        """
        Crear cadena de conexión para bases de datos SQL Server.
        
        Args:
            server (str): Servidor de base de datos
            database (str): Nombre de la base de datos
            username (str): Usuario de conexión
            password (str): Contraseña de conexión
        
        Returns:
            str: Cadena de conexión ODBC
        """
        return (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'Timeout=10;'
            'TrustServerCertificate=yes;'
        )

    @classmethod
    def conectar_base_datos_1(cls) -> pyodbc.Connection:
        """
        Establecer conexión con la primera base de datos.
        
        Returns:
            pyodbc.Connection: Objeto de conexión a la base de datos
        
        Raises:
            DatabaseConnectionError: Si no se puede establecer la conexión
        """
        try:
            cadena_conexion = cls._crear_cadena_conexion(
                Settings.SQL_SERVER,
                Settings.SQL_DATABASE,
                Settings.SQL_USER,
                Settings.SQL_PASSWORD
            )
            conn = pyodbc.connect(cadena_conexion)
            logging.info("Conexión exitosa a la primera base de datos.")
            return conn
        except pyodbc.Error as e:
            error_msg = f"Error en la conexión a la primera base de datos: {e}"
            logging.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

    @classmethod
    def conectar_base_datos_2(cls) -> pyodbc.Connection:
        """
        Establecer conexión con la segunda base de datos.
        
        Returns:
            pyodbc.Connection: Objeto de conexión a la base de datos
        
        Raises:
            DatabaseConnectionError: Si no se puede establecer la conexión
        """
        try:
            cadena_conexion = cls._crear_cadena_conexion(
                Settings.SQL_SERVER_2,
                Settings.SQL_DATABASE_2,
                Settings.SQL_USER_2,
                Settings.SQL_PASSWORD_2
            )
            conn = pyodbc.connect(cadena_conexion)
            logging.info("Conexión exitosa a la segunda base de datos.")
            return conn
        except pyodbc.Error as e:
            error_msg = f"Error en la conexión a la segunda base de datos: {e}"
            logging.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

    @staticmethod
    def obtener_fecha_actual() -> Tuple[str, str]:
        """
        Obtener fechas actuales en diferentes formatos.
        
        Returns:
            Tuple[str, str]: 
            - Fecha en formato YYYY-MM-DD (SQL)
            - Fecha en formato YYYYMMDD (SAP)
        """
        fecha_actual = datetime.now()
        return (
            fecha_actual.strftime("%Y-%m-%d"),  # Formato SQL
            fecha_actual.strftime("%Y%m%d")     # Formato SAP
        )

    @staticmethod
    @contextmanager
    def conexion_segura(conexion_func):
        """
        Contexto de conexión segura para gestión de recursos de base de datos.
        
        Args:
            conexion_func (callable): Función para establecer conexión
        
        Yields:
            pyodbc.Connection: Conexión a base de datos
        
        Example:
            with DatabaseConnection.conexion_segura(DatabaseConnection.conectar_base_datos_1) as conn:
                cursor = conn.cursor()
                # Operaciones con la base de datos
        """
        conn = None
        try:
            conn = conexion_func()
            yield conn
        except Exception as e:
            logging.error(f"Error durante la operación con base de datos: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    logging.info("Conexión cerrada exitosamente.")
                except Exception as e:
                    logging.error(f"Error al cerrar la conexión: {e}")

# Configuración de logging (si no está configurado globalmente)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/database.log', encoding='utf-8', mode='a')
    ]
)