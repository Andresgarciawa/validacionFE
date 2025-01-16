# Validación de Documentos Electrónicos

## Descripción
Aplicación para validación automática de documentos electrónicos, con consulta periódica a API y base de datos.

## Requisitos Previos
- Python 3.8+
- pip
- Conexión a internet
- Cuenta de correo configurada para envío SMTP

## Instalación

1. Clonar el repositorio
```bash
git clone https://github.tu-usuario/proyecto-validacion-documentos.git
cd proyecto-validacion-documentos
```

2. Crear entorno virtual
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. Instalar dependencias
```bash
pip install -r requirements.txt
```

4. Configurar `config.ini`
- Editar credenciales de API
- Configurar conexión de base de datos
- Establecer credenciales de correo

## Ejecución
```bash
python src/main.py
```

## Configuración
Modificar `config.ini` para personalizar:
- Intervalo de ciclo
- Credenciales
- Configuraciones de log

## Errores Comunes
- Verificar credenciales de API
- Comprobar conexión de base de datos
- Revisar configuración de correo SMTP