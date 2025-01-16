from tkinter import Tk
from gui import Menu
from database import BaseDatos
from api_handler import APIHandler
from email_sender import EmailSender


def main():
    # Crea la ventana principal
    root = Tk()
    root.title("Mi Aplicación")
    
    # Configuración de la base de datos
    db = BaseDatos(
        servidor="localhost",
        base_datos="mi_base_datos",
        usuario="mi_usuario",
        contrasena="mi_contraseña"
    )

    # Configuración de la API
    api_handler = APIHandler()

    # Configuración del manejador de correos
    email_sender = EmailSender()

    # Crea la GUI de la aplicación
    app = Menu(root, db, api_handler, email_sender)

    # Ejecuta la interfaz gráfica
    root.mainloop()

if __name__ == "__main__":
    main()

