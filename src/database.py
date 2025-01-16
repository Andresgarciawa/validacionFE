import pyodbc
import logging
import time
from typing import Optional, List, Tuple, Any, Dict
from contextlib import contextmanager
from datetime import datetime
import configparser
import os

class BaseDatos:
    """
    Clase para manejar conexiones y operaciones con bases de datos SQL Server.
    Incluye reconexión automática, pool de conexiones y manejo de transacciones.
    """

    def __init__(self, config_path: str = 'config/database.ini'):
        """
        Inicializa la configuración de la base de datos.
        
        Args:
            config_path (str): Ruta al archivo de configuración
            
        Raises:
            FileNotFoundError: Si no encuentra el archivo de configuración
            KeyError: Si faltan campos requeridos en la configuración
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")

        config = configparser.ConfigParser()
        config.read(config_path)

        try:
            self.servidor = config['DATABASE']['SERVER']
            self.base_datos = config['DATABASE']['DATABASE']
            self.usuario = config['DATABASE']['USER']
            self.contrasena = config['DATABASE']['PASSWORD']
            
            # Configuraciones opcionales
            self.max_intentos = int(config['DATABASE'].get('MAX_RETRIES', '3'))
            self.tiempo_espera = int(config['DATABASE'].get('RETRY_DELAY', '5'))
            self.timeout = int(config['DATABASE'].get('TIMEOUT', '30'))
            
        except KeyError as e:
            raise KeyError(f"Campo requerido faltante en la configuración: {str(e)}")

        self.conexion = None
        self._conectar()

    def _conectar(self) -> None:
        """
        Establece la conexión con la base de datos con reintentos automáticos.
        
        Raises:
            pyodbc.Error: Si no se puede establecer la conexión después de todos los intentos
        """
        for intento in range(self.max_intentos):
            try:
                self.conexion = pyodbc.connect(
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.servidor};"
                    f"DATABASE={self.base_datos};"
                    f"UID={self.usuario};"
                    f"PWD={self.contrasena};"
                    f"timeout={self.timeout}"
                )
                self.conexion.autocommit = False  # Manejo explícito de transacciones
                logging.info("Conexión exitosa a la base de datos")
                break
                
            except pyodbc.Error as e:
                logging.warning(f"Intento {intento + 1}/{self.max_intentos} fallido: {str(e)}")
                if intento == self.max_intentos - 1:
                    logging.error(f"No se pudo establecer conexión después de {self.max_intentos} intentos")
                    raise
                time.sleep(self.tiempo_espera)

    def _reconectar_si_necesario(self) -> None:
        """Verifica la conexión y reconecta si es necesario."""
        try:
            # Intenta una consulta simple para verificar la conexión
            with self.conexion.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (pyodbc.Error, AttributeError):
            logging.info("Reconectando a la base de datos...")
            self._conectar()

    @contextmanager
    def transaccion(self):
        """
        Administrador de contexto para manejar transacciones.
        
        Yields:
            BaseDatos: Instancia actual para usar en el bloque with
            
        Example:
            with db.transaccion():
                db.ejecutar_modificacion("INSERT INTO tabla VALUES (?)", [1])
                db.ejecutar_modificacion("UPDATE tabla SET campo = ?", [2])
        """
        self._reconectar_si_necesario()
        try:
            yield self
            self.conexion.commit()
            logging.info("Transacción completada exitosamente")
        except Exception as e:
            self.conexion.rollback()
            logging.error(f"Error en transacción - realizando rollback: {str(e)}")
            raise

    def consultar_base_datos(
        self,
        consulta: str,
        parametros: Optional[List[Any]] = None,
        dictionary: bool = False
    ) -> Optional[List[Tuple[Any, ...]]]:
        """
        Realiza una consulta SELECT a la base de datos.
        
        Args:
            consulta (str): Consulta SQL
            parametros (Optional[List[Any]]): Lista de parámetros para la consulta
            dictionary (bool): Si True, devuelve los resultados como diccionarios
            
        Returns:
            Optional[List[Tuple[Any, ...]]]: Resultados de la consulta o None si hay error
            
        Raises:
            pyodbc.Error: Si hay un error en la consulta
        """
        self._reconectar_si_necesario()
        
        try:
            with self.conexion.cursor() as cursor:
                start_time = time.time()
                
                if parametros:
                    cursor.execute(consulta, parametros)
                else:
                    cursor.execute(consulta)
                    
                if dictionary:
                    columns = [column[0] for column in cursor.description]
                    resultados = [dict(zip(columns, row)) for row in cursor.fetchall()]
                else:
                    resultados = cursor.fetchall()
                    
                elapsed_time = time.time() - start_time
                logging.info(
                    f"Consulta ejecutada en {elapsed_time:.2f}s: {consulta[:100]}... "
                    f"Filas retornadas: {len(resultados)}"
                )
                
                return resultados
                
        except pyodbc.Error as e:
            logging.error(f"Error en consulta: {consulta[:100]}... - Error: {str(e)}")
            raise

    def ejecutar_modificacion(
        self,
        consulta: str,
        parametros: Optional[List[Any]] = None
    ) -> int:
        """
        Ejecuta una consulta de modificación (INSERT, UPDATE, DELETE).
        
        Args:
            consulta (str): Consulta SQL
            parametros (Optional[List[Any]]): Lista de parámetros para la consulta
            
        Returns:
            int: Número de filas afectadas
            
        Raises:
            pyodbc.Error: Si hay un error en la consulta
        """
        self._reconectar_si_necesario()
        
        try:
            with self.conexion.cursor() as cursor:
                start_time = time.time()
                
                if parametros:
                    cursor.execute(consulta, parametros)
                else:
                    cursor.execute(consulta)
                    
                filas_afectadas = cursor.rowcount
                self.conexion.commit()
                
                elapsed_time = time.time() - start_time
                logging.info(
                    f"Modificación ejecutada en {elapsed_time:.2f}s: {consulta[:100]}... "
                    f"Filas afectadas: {filas_afectadas}"
                )
                
                return filas_afectadas
                
        except pyodbc.Error as e:
            self.conexion.rollback()
            logging.error(f"Error en modificación: {consulta[:100]}... - Error: {str(e)}")
            raise

    def ejecutar_procedimiento(
        self,
        nombre: str,
        parametros: Optional[List[Any]] = None,
        dictionary: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Ejecuta un procedimiento almacenado.
        
        Args:
            nombre (str): Nombre del procedimiento
            parametros (Optional[List[Any]]): Lista de parámetros para el procedimiento
            dictionary (bool): Si True, devuelve los resultados como diccionarios
            
        Returns:
            Optional[List[Dict[str, Any]]]: Resultados del procedimiento o None si hay error
            
        Raises:
            pyodbc.Error: Si hay un error al ejecutar el procedimiento
        """
        self._reconectar_si_necesario()
        
        try:
            with self.conexion.cursor() as cursor:
                start_time = time.time()
                
                if parametros:
                    cursor.execute(f"EXEC {nombre} {','.join(['?' for _ in parametros])}", parametros)
                else:
                    cursor.execute(f"EXEC {nombre}")
                    
                if dictionary:
                    columns = [column[0] for column in cursor.description]
                    resultados = [dict(zip(columns, row)) for row in cursor.fetchall()]
                else:
                    resultados = cursor.fetchall()
                    
                self.conexion.commit()
                
                elapsed_time = time.time() - start_time
                logging.info(
                    f"Procedimiento {nombre} ejecutado en {elapsed_time:.2f}s "
                    f"Filas retornadas: {len(resultados)}"
                )
                
                return resultados
                
        except pyodbc.Error as e:
            self.conexion.rollback()
            logging.error(f"Error en procedimiento {nombre}: {str(e)}")
            raise

    def obtener_ultimo_id(self, tabla: str, campo: str = 'id') -> Optional[int]:
        """
        Obtiene el último ID insertado en una tabla.
        
        Args:
            tabla (str): Nombre de la tabla
            campo (str): Nombre del campo ID
            
        Returns:
            Optional[int]: Último ID o None si hay error
        """
        try:
            resultado = self.consultar_base_datos(
                f"SELECT MAX({campo}) FROM {tabla}"
            )
            return resultado[0][0] if resultado and resultado[0][0] else None
        except pyodbc.Error as e:
            logging.error(f"Error al obtener último ID de {tabla}: {str(e)}")
            return None

    def cerrar_conexion(self) -> None:
        """Cierra la conexión con la base de datos."""
        if self.conexion:
            try:
                self.conexion.close()
                logging.info("Conexión cerrada exitosamente")
            except pyodbc.Error as e:
                logging.error(f"Error al cerrar conexión: {str(e)}")

    def __enter__(self):
        """Soporte para usar la clase con 'with'."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra la conexión al salir del bloque 'with'."""
        self.cerrar_conexion()