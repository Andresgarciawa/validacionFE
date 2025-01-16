import requests
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union
import configparser
from urllib.parse import urljoin
import time
from functools import wraps
import jwt
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class APIHandler:
    """
    Manejador de API con soporte para:
    - Autenticación JWT
    - Caché de tokens
    - Reintentos automáticos
    - Rate limiting
    - Timeouts configurables
    """

    def __init__(self, config_path: str = 'config/api.ini'):
        """
        Inicializa el manejador de API.
        
        Args:
            config_path (str): Ruta al archivo de configuración
            
        Raises:
            FileNotFoundError: Si no encuentra el archivo de configuración
            ValueError: Si faltan campos requeridos en la configuración
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")

        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        try:
            self.base_url = self.config['API']['base_url'].rstrip('/')
            self.username = self.config['API']['username']
            self.password = self.config['API']['password']
            
            # Configuraciones opcionales
            self.timeout = int(self.config['API'].get('timeout', '30'))
            self.max_retries = int(self.config['API'].get('max_retries', '3'))
            self.retry_delay = int(self.config['API'].get('retry_delay', '5'))
            self.rate_limit = float(self.config['API'].get('rate_limit', '0.5'))  # segundos entre requests
            
        except KeyError as e:
            raise ValueError(f"Campo requerido faltante en la configuración: {str(e)}")

        # Estado interno
        self.token = None
        self.token_expiry = None
        self.last_request_time = 0
        
        # Configurar sesión con reintentos
        self.session = self._configurar_session()
        
        logging.info("APIHandler inicializado correctamente")

    def _configurar_session(self) -> requests.Session:
        """Configura una sesión de requests con reintentos automáticos."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def _rate_limit(self):
        """Implementa rate limiting básico."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()

    def _verificar_token(func):
        """Decorador para verificar y renovar el token si es necesario."""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.token or (
                self.token_expiry and datetime.now() >= self.token_expiry - timedelta(minutes=5)
            ):
                self.obtener_token()
            return func(self, *args, **kwargs)
        return wrapper

    def obtener_token(self) -> Optional[str]:
        """
        Obtiene un nuevo token de autenticación.
        
        Returns:
            Optional[str]: Token JWT o None si hay error
            
        Raises:
            requests.RequestException: Si hay un error en la petición
        """
        self._rate_limit()
        
        url = urljoin(self.base_url, '/login')
        data = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = self.session.post(
                url,
                json=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            self.token = response.json()['token']
            
            # Decodificar token para obtener expiración
            try:
                payload = jwt.decode(self.token, options={"verify_signature": False})
                self.token_expiry = datetime.fromtimestamp(payload['exp'])
            except (jwt.InvalidTokenError, KeyError):
                # Si no podemos decodificar, asumimos 1 hora
                self.token_expiry = datetime.now() + timedelta(hours=1)
                
            logging.info("Nuevo token obtenido exitosamente")
            return self.token
            
        except requests.RequestException as e:
            logging.error(f"Error al obtener token: {str(e)}")
            raise

    @_verificar_token
    def hacer_peticion(
        self,
        metodo: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        archivos: Optional[Dict] = None
    ) -> requests.Response:
        """
        Realiza una petición HTTP genérica a la API.
        
        Args:
            metodo (str): Método HTTP (GET, POST, etc)
            endpoint (str): Endpoint de la API
            params (Optional[Dict]): Parámetros de query
            data (Optional[Dict]): Datos para enviar
            headers (Optional[Dict]): Headers adicionales
            archivos (Optional[Dict]): Archivos para enviar
            
        Returns:
            requests.Response: Respuesta de la API
            
        Raises:
            requests.RequestException: Si hay un error en la petición
        """
        self._rate_limit()
        
        url = urljoin(self.base_url, endpoint)
        headers = headers or {}
        headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })
        
        try:
            response = self.session.request(
                method=metodo,
                url=url,
                params=params,
                json=data,
                headers=headers,
                files=archivos,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            logging.error(f"Error en petición {metodo} a {endpoint}: {str(e)}")
            raise

    @_verificar_token
    def consultar_documento(self, id_documento: str) -> Optional[Dict]:
        """
        Consulta el estado de un documento.
        
        Args:
            id_documento (str): ID del documento a consultar
            
        Returns:
            Optional[Dict]: Información del documento o None si hay error
        """
        try:
            params = {
                "SchemaID": self.config['API']['schema_id'],
                "DocumentType": self.config['API']['document_type'],
                "DocumentID": id_documento
            }
            
            response = self.hacer_peticion(
                "GET",
                "/GetDocumentStatus",
                params=params
            )
            
            logging.info(f"Documento {id_documento} consultado exitosamente")
            return response.json()
            
        except requests.RequestException as e:
            logging.error(f"Error al consultar documento {id_documento}: {str(e)}")
            return None

    @_verificar_token
    def insertar_documento(
        self,
        id_documento: str,
        nombre: str,
        estado: str,
        datos_adicionales: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Inserta un nuevo documento.
        
        Args:
            id_documento (str): ID del documento
            nombre (str): Nombre del documento
            estado (str): Estado inicial del documento
            datos_adicionales (Optional[Dict]): Datos adicionales del documento
            
        Returns:
            Optional[Dict]: Respuesta de la API o None si hay error
        """
        try:
            data = {
                "DocumentID": id_documento,
                "Nombre": nombre,
                "Estado": estado,
                **(datos_adicionales or {})
            }
            
            response = self.hacer_peticion(
                "POST",
                "/InsertDocument",
                data=data
            )
            
            logging.info(f"Documento {id_documento} insertado exitosamente")
            return response.json()
            
        except requests.RequestException as e:
            logging.error(f"Error al insertar documento {id_documento}: {str(e)}")
            return None

    @_verificar_token
    def actualizar_documento(
        self,
        id_documento: str,
        datos_actualizacion: Dict[str, Any]
    ) -> Optional[Dict]:
        """
        Actualiza un documento existente.
        
        Args:
            id_documento (str): ID del documento
            datos_actualizacion (Dict[str, Any]): Datos a actualizar
            
        Returns:
            Optional[Dict]: Respuesta de la API o None si hay error
        """
        try:
            data = {
                "DocumentID": id_documento,
                **datos_actualizacion
            }
            
            response = self.hacer_peticion(
                "PUT",
                "/UpdateDocument",
                data=data
            )
            
            logging.info(f"Documento {id_documento} actualizado exitosamente")
            return response.json()
            
        except requests.RequestException as e:
            logging.error(f"Error al actualizar documento {id_documento}: {str(e)}")
            return None

    @_verificar_token
    def cargar_archivo(
        self,
        id_documento: str,
        ruta_archivo: str,
        tipo_archivo: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Carga un archivo asociado a un documento.
        
        Args:
            id_documento (str): ID del documento
            ruta_archivo (str): Ruta al archivo a cargar
            tipo_archivo (Optional[str]): Tipo MIME del archivo
            
        Returns:
            Optional[Dict]: Respuesta de la API o None si hay error
        """
        try:
            if not os.path.exists(ruta_archivo):
                raise FileNotFoundError(f"Archivo no encontrado: {ruta_archivo}")
                
            with open(ruta_archivo, 'rb') as f:
                archivos = {
                    'file': (
                        os.path.basename(ruta_archivo),
                        f,
                        tipo_archivo or 'application/octet-stream'
                    )
                }
                
                response = self.hacer_peticion(
                    "POST",
                    f"/UploadFile/{id_documento}",
                    archivos=archivos
                )
                
            logging.info(f"Archivo cargado exitosamente para documento {id_documento}")
            return response.json()
            
        except (requests.RequestException, FileNotFoundError) as e:
            logging.error(f"Error al cargar archivo para documento {id_documento}: {str(e)}")
            return None

    def cerrar_sesion(self):
        """Cierra la sesión y limpia el estado."""
        if self.session:
            self.session.close()
        self.token = None
        self.token_expiry = None
        logging.info("Sesión cerrada exitosamente")

    def __enter__(self):
        """Soporte para usar la clase con 'with'."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra la sesión al salir del bloque 'with'."""
        self.cerrar_sesion()