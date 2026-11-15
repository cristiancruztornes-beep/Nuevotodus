"""
Prueba rápida: subir un archivito de prueba a TU cuenta de ToDus con tu token
y comprobar si el enlace se puede abrir sin credenciales y si sigue vivo después.

Uso en Render: Start Command = python probar_todus.py; variables TODUS_PHONE y
TODUS_AUTH_TOKEN en Environment; toDus-API en requirements.txt. Mira los Logs.

El token NO se imprime. No lo pegues en chats ni lo subas a GitHub (usa .env / secrets).
"""
import os
import sys
import time

import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from todus import ToDusClient2, FileType

class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    do_HEAD = do_GET

    def log_message(self, *a):
        pass


# Render (Web Service) necesita un puerto abierto para no matar el proceso.
if os.getenv("PORT"):
    threading.Thread(
        target=HTTPServer(("0.0.0.0", int(os.environ["PORT"])), _H).serve_forever,
        daemon=True,
    ).start()

phone = os.getenv("TODUS_PHONE")
token = os.getenv("TODUS_AUTH_TOKEN")
if not phone or not token:
    sys.exit("Faltan TODUS_PHONE y/o TODUS_AUTH_TOKEN en el entorno.")

client = ToDusClient2(phone_number=phone, password=token)
client.login()
print("Login OK")

datos = b"prueba de subida a ToDus " + str(time.time()).encode()
url = client.upload_file(datos, FileType.FILE)
print("URL devuelta:", url)


def estado(u):
    try:
        r = requests.get(u, timeout=30, stream=True)
        return r.status_code
    except Exception as e:  # noqa
        return f"error: {e}"


print("Sin credenciales, ahora mismo:", estado(url))
for minutos in (2, 10):
    time.sleep(minutos * 60 if minutos == 2 else 8 * 60)
    print(f"Sin credenciales, a los {minutos} min:", estado(url), flush=True)

print("Prueba terminada. Puedes ver el resultado arriba en los Logs.", flush=True)
while os.getenv("PORT"):
    time.sleep(3600)
