import smtplib
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formataddr
import configparser
from typing import List, Optional
from pathlib import Path
import re

class EmailSender:
    """
    Clase para enviar correos electrónicos mediante SMTP con soporte para:
    - Múltiples destinatarios
    - Archivos adjuntos
    - Plantillas HTML
    - Validación de correos
    - Reintentos automáticos
    """
    
    def __init__(self, config_path: str = 'config/config.ini'):
        """
        Inicializa el sender de correos con la configuración especificada.
        
        Args:
            config_path (str): Ruta al archivo de configuración
        
        Raises:
            FileNotFoundError: Si no encuentra el archivo de configuración
            KeyError: Si faltan campos requeridos en la configuración
        """
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Validar existencia del archivo de configuración
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
            
        # Cargar configuración
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        try:
            self.smtp_server = self.config["EMAIL"]["SMTP_SERVER"]
            self.smtp_port = int(self.config["EMAIL"]["SMTP_PORT"])
            self.smtp_user = self.config["EMAIL"]["USER"]
            self.smtp_password = self.config["EMAIL"]["PASSWORD"]
            
            # Configuraciones opcionales
            self.max_retries = int(self.config["EMAIL"].get("MAX_RETRIES", 3))
            self.sender_name = self.config["EMAIL"].get("SENDER_NAME", "")
            self.reply_to = self.config["EMAIL"].get("REPLY_TO", "")
            
        except KeyError as e:
            raise KeyError(f"Campo requerido faltante en la configuración: {str(e)}")
            
        # Compilar regex para validación de correos
        self.email_regex = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

    def validar_correo(self, correo: str) -> bool:
        """
        Valida el formato de una dirección de correo.
        
        Args:
            correo (str): Dirección de correo a validar
            
        Returns:
            bool: True si el formato es válido, False en caso contrario
        """
        return bool(self.email_regex.match(correo))

    def crear_mensaje(
        self,
        destinatarios: List[str],
        asunto: str,
        mensaje: str,
        html: Optional[str] = None,
        adjuntos: Optional[List[str]] = None
    ) -> MIMEMultipart:
        """
        Crea un mensaje de correo con los parámetros especificados.
        
        Args:
            destinatarios (List[str]): Lista de direcciones de correo destino
            asunto (str): Asunto del correo
            mensaje (str): Cuerpo del mensaje en texto plano
            html (Optional[str]): Cuerpo del mensaje en HTML
            adjuntos (Optional[List[str]]): Lista de rutas a archivos adjuntos
            
        Returns:
            MIMEMultipart: Mensaje preparado para enviar
            
        Raises:
            ValueError: Si algún correo es inválido o si falta algún archivo adjunto
        """
        # Validar correos
        for correo in destinatarios:
            if not self.validar_correo(correo):
                raise ValueError(f"Dirección de correo inválida: {correo}")

        msg = MIMEMultipart('alternative')
        msg['Subject'] = asunto
        msg['From'] = formataddr((self.sender_name, self.smtp_user)) if self.sender_name else self.smtp_user
        msg['To'] = ', '.join(destinatarios)
        
        if self.reply_to:
            msg.add_header('Reply-To', self.reply_to)

        # Agregar versión texto plano
        msg.attach(MIMEText(mensaje, 'plain'))
        
        # Agregar versión HTML si existe
        if html:
            msg.attach(MIMEText(html, 'html'))

        # Agregar adjuntos
        if adjuntos:
            for archivo in adjuntos:
                if not os.path.exists(archivo):
                    raise ValueError(f"Archivo adjunto no encontrado: {archivo}")
                    
                with open(archivo, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(archivo))
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(archivo)}"'
                    msg.attach(part)

        return msg

    def enviar_correo(
        self,
        destinatarios: List[str],
        asunto: str,
        mensaje: str,
        html: Optional[str] = None,
        adjuntos: Optional[List[str]] = None,
        intentos: int = None
    ) -> bool:
        """
        Envía un correo electrónico con reintentos automáticos.
        
        Args:
            destinatarios (List[str]): Lista de direcciones de correo destino
            asunto (str): Asunto del correo
            mensaje (str): Cuerpo del mensaje en texto plano
            html (Optional[str]): Cuerpo del mensaje en HTML
            adjuntos (Optional[List[str]]): Lista de rutas a archivos adjuntos
            intentos (Optional[int]): Número máximo de intentos, usa configuración por defecto si es None
            
        Returns:
            bool: True si el envío fue exitoso, False en caso contrario
            
        Raises:
            ValueError: Si algún parámetro es inválido
            smtplib.SMTPException: Si hay un error en el envío después de todos los reintentos
        """
        if not destinatarios:
            raise ValueError("Debe especificar al menos un destinatario")
            
        max_intentos = intentos if intentos is not None else self.max_retries
        intento_actual = 0
        
        while intento_actual < max_intentos:
            try:
                msg = self.crear_mensaje(destinatarios, asunto, mensaje, html, adjuntos)
                
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as servidor:
                    servidor.starttls()
                    servidor.login(self.smtp_user, self.smtp_password)
                    servidor.send_message(msg)
                    
                logging.info(f"Correo enviado exitosamente a {', '.join(destinatarios)}")
                return True
                
            except Exception as e:
                intento_actual += 1
                logging.warning(
                    f"Error en intento {intento_actual}/{max_intentos} "
                    f"al enviar correo a {', '.join(destinatarios)}: {str(e)}"
                )
                
                if intento_actual >= max_intentos:
                    logging.error(
                        f"Error al enviar correo después de {max_intentos} intentos: {str(e)}"
                    )
                    raise
                    
        return False

    def cargar_plantilla(self, ruta: str, **kwargs) -> str:
        """
        Carga una plantilla HTML y reemplaza las variables.
        
        Args:
            ruta (str): Ruta al archivo de plantilla
            **kwargs: Variables a reemplazar en la plantilla
            
        Returns:
            str: Contenido de la plantilla con las variables reemplazadas
            
        Raises:
            FileNotFoundError: Si no encuentra el archivo de plantilla
        """
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"Plantilla no encontrada: {ruta}")
            
        with open(ruta, 'r', encoding='utf-8') as f:
            plantilla = f.read()
            
        for key, value in kwargs.items():
            plantilla = plantilla.replace(f"{{${key}}}", str(value))
            
        return plantilla