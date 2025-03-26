import tkinter as tk
from tkinter import messagebox
import subprocess
import threading
import time
import os

# Variable para controlar el estado del proceso
ejecutando = False

def iniciar_proceso():
    global ejecutando
    tiempo = tiempo_entry.get()
    if not tiempo.isdigit():
        messagebox.showerror("Error", "Por favor, ingrese un tiempo válido en minutos.")
        return

    tiempo = int(tiempo) * 60
    ejecutando = True

    def ejecutar_periodicamente():
        while ejecutando:
            subprocess.run(["python", "validacionFE.py"])
            time.sleep(tiempo)

    threading.Thread(target=ejecutar_periodicamente, daemon=True).start()
    messagebox.showinfo("Proceso iniciado", "El proceso se está ejecutando periódicamente.")

def detener_proceso():
    global ejecutando
    ejecutando = False
    messagebox.showinfo("Proceso detenido", "El proceso se ha detenido correctamente.")

def abrir_log():
    log_path = os.path.join(os.getcwd(), "log_validacion.txt")
    if os.path.exists(log_path):
        os.startfile(log_path)
    else:
        messagebox.showerror("Error", "El archivo de log no se encontró.")

# Crear la interfaz gráfica
root = tk.Tk()
root.title("Validación FE")
root.geometry("300x250")

# Campo para el tiempo de ejecución
tk.Label(root, text="Tiempo de ejecución (min):").pack(pady=5)
tiempo_entry = tk.Entry(root)
tiempo_entry.pack(pady=5)

tk.Button(root, text="Iniciar", command=iniciar_proceso).pack(pady=5)
tk.Button(root, text="Detener", command=detener_proceso).pack(pady=5)
tk.Button(root, text="Abrir Log", command=abrir_log).pack(pady=5)

root.mainloop()
