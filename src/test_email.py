import smtplib
from email.message import EmailMessage

EMAIL_USER = 'wgarcia@nikkenlatam.com'  # tu correo de Gmail
EMAIL_PASSWORD = 'ndzhbpniqktwpucg'  # tu contraseña de aplicación (sin espacios)
EMAIL_TO = 'wgarcia@nikkenlatam.com'  # te lo envías a ti mismo para probar

msg = EmailMessage()
msg['From'] = EMAIL_USER
msg['To'] = EMAIL_TO
msg['Subject'] = '✅ Prueba desde Python'
msg.set_content('Hola, este es un correo de prueba enviado desde un script.')

try:
    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_USER, EMAIL_PASSWORD)
        smtp.send_message(msg)
    print("✅ Correo enviado correctamente.")
except Exception as e:
    print(f"❌ Error al enviar: {e}")
