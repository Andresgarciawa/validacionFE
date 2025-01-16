from tkinter import *
from tkinter import ttk, messagebox
import threading
import logging
import os
from datetime import datetime

# Asegurar que existe el directorio de logs
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configuración de logging con rotación diaria
log_file = f"logs/app_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Menu:
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Gestión de Ciclos")
        self.root.geometry("800x600")

        # Variables de estado
        self.ejecutando = threading.Event()
        self.hilo_actual = None
        self.contador_ciclos = 0

        # Frame principal
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill='both', expand=True)

        # Título
        ttk.Label(
            main_frame, 
            text="Sistema de Gestión de Ciclos", 
            font=("Arial", 16, "bold")
        ).pack(pady=10)

        # Frame para controles
        control_frame = ttk.LabelFrame(main_frame, text="Configuración", padding="10")
        control_frame.pack(fill='x', padx=10, pady=5)

        # Entrada del intervalo
        ttk.Label(control_frame, text="Intervalo (segundos):").pack(side='left', padx=5)
        self.intervalo_entry = ttk.Entry(control_frame, width=10)
        self.intervalo_entry.pack(side='left', padx=5)
        self.intervalo_entry.insert(0, "60")  # Valor predeterminado

        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=10)

        self.iniciar_button = ttk.Button(
            button_frame, 
            text="Iniciar Ciclo", 
            command=self.iniciar_ciclo,
            style='Primary.TButton'
        )
        self.iniciar_button.pack(side='left', padx=5)

        self.detener_button = ttk.Button(
            button_frame, 
            text="Detener Ciclo", 
            command=self.detener_ciclo,
            state='disabled'
        )
        self.detener_button.pack(side='left', padx=5)

        # Frame para información
        info_frame = ttk.LabelFrame(main_frame, text="Estado del Sistema", padding="10")
        info_frame.pack(fill='x', padx=10, pady=5)

        self.estado_label = ttk.Label(
            info_frame, 
            text="Estado: Inactivo", 
            font=("Arial", 10)
        )
        self.estado_label.pack(pady=5)

        self.ciclos_label = ttk.Label(
            info_frame, 
            text="Ciclos completados: 0", 
            font=("Arial", 10)
        )
        self.ciclos_label.pack(pady=5)

        # Área de logs
        log_frame = ttk.LabelFrame(main_frame, text="Registro de Actividad", padding="10")
        log_frame.pack(fill='both', expand=True, padx=10, pady=5)

        self.log_text = ttk.Text(log_frame, height=10, width=50, state='disabled')
        self.log_text.pack(fill='both', expand=True)

        # Configurar el cierre de la ventana
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def iniciar_ciclo(self):
        """Inicia un ciclo de consulta en un hilo separado."""
        try:
            intervalo = int(self.intervalo_entry.get())
            if intervalo <= 0:
                raise ValueError("El intervalo debe ser mayor que cero")
            
            self.ejecutando.set()
            self.hilo_actual = threading.Thread(
                target=self.ejecutar_ciclo, 
                args=(intervalo,), 
                daemon=True
            )
            self.hilo_actual.start()
            
            # Actualizar UI
            self.actualizar_log(f"Ciclo iniciado con intervalo de {intervalo} segundos")
            self.estado_label.config(text="Estado: En ejecución")
            self.iniciar_button.config(state='disabled')
            self.detener_button.config(state='normal')
            self.intervalo_entry.config(state='disabled')
            
            logging.info(f"Ciclo iniciado con intervalo de {intervalo} segundos")
            
        except ValueError as e:
            messagebox.showerror("Error", f"Error en el intervalo: {str(e)}")
            logging.error(f"Error al iniciar ciclo: {str(e)}")

    def ejecutar_ciclo(self, intervalo):
        """Ejecuta el ciclo de consulta en segundo plano."""
        while self.ejecutando.is_set():
            try:
                self.consultar_datos()
                self.contador_ciclos += 1
                self.root.after(0, self.actualizar_contador)
                threading.Event().wait(intervalo)  # Espera no bloqueante
            except Exception as e:
                self.actualizar_log(f"Error en ciclo: {str(e)}")
                logging.error(f"Error en ciclo: {str(e)}")

    def consultar_datos(self):
        """Simula la consulta de datos."""
        mensaje = f"Consulta #{self.contador_ciclos + 1} realizada"
        self.actualizar_log(mensaje)
        logging.info(mensaje)
        # Aquí iría la lógica real de consulta

    def detener_ciclo(self):
        """Detiene el ciclo de consulta."""
        if self.ejecutando.is_set():
            self.ejecutando.clear()
            if self.hilo_actual:
                self.hilo_actual.join(timeout=1.0)
            
            # Actualizar UI
            self.estado_label.config(text="Estado: Detenido")
            self.iniciar_button.config(state='normal')
            self.detener_button.config(state='disabled')
            self.intervalo_entry.config(state='normal')
            
            mensaje = "Ciclo detenido"
            self.actualizar_log(mensaje)
            logging.info(mensaje)
            messagebox.showinfo("Información", mensaje)

    def actualizar_log(self, mensaje):
        """Actualiza el área de logs en la UI."""
        self.log_text.config(state='normal')
        self.log_text.insert('end', f"{datetime.now().strftime('%H:%M:%S')} - {mensaje}\n")
        self.log_text.see('end')
        self.log_text.config(state='disabled')

    def actualizar_contador(self):
        """Actualiza el contador de ciclos en la UI."""
        self.ciclos_label.config(text=f"Ciclos completados: {self.contador_ciclos}")

    def on_closing(self):
        """Maneja el cierre de la ventana."""
        if self.ejecutando.is_set():
            if messagebox.askokcancel("Confirmar", "¿Desea detener el ciclo y cerrar la aplicación?"):
                self.detener_ciclo()
                self.root.destroy()
        else:
            self.root.destroy()

if __name__ == "__main__":
    root = ttk.Tk()
    app = Menu(root)
    root.mainloop()