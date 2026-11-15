#!/usr/bin/env python3
"""
Bot de Telegram -> ToDus (anonimo), version LIGERA para hostings gratis pequenos.

- Enlaces http(s) directos: se pasan de la fuente a ToDus en streaming,
  SIN guardar nada en disco (sirve para archivos de cualquier tamano).
  Si pesa mas de 2 GB, se sube en partes. Necesita que el servidor de origen
  informe Content-Length.
- Archivos enviados por Telegram: hasta ~20 MB (limite de la Bot API oficial);
  se descargan a memoria, no a disco.
- Sin Docker ni servidor local de Telegram.

Variables (.env o entorno):  TELEGRAM_BOT_TOKEN, S3_BASE (opcional), S3_PREFIX (opcional)
"""
import io
import os
import uuid
import datetime
import re
import sys
import time
import asyncio
import logging
from urllib.parse import quote, unquote, urlparse

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, MessageHandler, CallbackQueryHandler, CommandHandler,
    ContextTypes, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("todus_bot")

S3_BASE = os.getenv("S3_BASE", "https://s3.todus.cu/stream").rstrip("/")
S3_PREFIX = os.getenv("S3_PREFIX", "users").strip("/")
CHUNK = 1024 * 1024
EDIT_EVERY = 2.0
SPLIT_SIZE = 2 * 1024 * 1024 * 1024
URL_RE = re.compile(r"^https?://\S+$")

cancel_flags = {}


def build_bar(pct: float) -> str:
    filled = int(20 * pct / 100)
    return "█" * filled + "░" * (20 - filled)


def cancel_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancelar", callback_data=f"cancel:{job_id}")]]
    )


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Pégame un enlace directo de descarga (cualquier tamaño) o envíame un "
        "archivo de hasta ~20 MB, y lo subo a ToDus.\n"
        "Verás el progreso aquí, con botón para cancelar."
    )


async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    cancel_flags[query.data.split(":", 1)[1]] = True
    await query.answer("Cancelando...")


class _Part:
    """Trozo de un flujo de origen, con tamano fijo (__len__) para que requests
    mande Content-Length real (MinIO rechaza chunked). Reporta progreso y cancela."""

    def __init__(self, reader, length, job_id, on_bytes):
        self._r, self._left, self._len = reader, length, length
        self._job_id, self._on_bytes = job_id, on_bytes

    def __len__(self):
        return self._len

    def read(self, size=-1):
        if cancel_flags.get(self._job_id):
            raise RuntimeError("CANCELLED")
        if self._left <= 0:
            return b""
        n = CHUNK if size is None or size < 0 else min(size, CHUNK)
        data = self._r.read(min(n, self._left))
        if not data:
            raise RuntimeError("La fuente se cortó antes de terminar la descarga.")
        self._left -= len(data)
        self._on_bytes(len(data))
        return data


def upload_stream_sync(reader, total, user_id, filename, job_id, progress_cb):
    """Sube 'reader' (total bytes) a ToDus; parte en trozos de 2 GB si hace falta."""
    done = 0

    def on_bytes(n):
        nonlocal done
        done += n
        progress_cb(done, total)

    n_parts = max(1, (total + SPLIT_SIZE - 1) // SPLIT_SIZE)
    urls = []
    for i in range(n_parts):
        size = min(SPLIT_SIZE, total - i * SPLIT_SIZE)
        name = filename if n_parts == 1 else f"{filename}.part{i+1:03d}"
        # Formato que persiste en el bucket: users/df/<id_numerico>/<archivo>
        url = f"{S3_BASE}/{S3_PREFIX}/df/{user_id}/{quote(name, safe='')}"
        resp = requests.put(
            url,
            data=_Part(reader, size, job_id, on_bytes),
            headers={"Content-Type": "application/octet-stream"},
            timeout=600,
        )
        resp.raise_for_status()
        # Comprobar que el objeto realmente quedó guardado (HEAD) antes de dar el enlace.
        try:
            head = requests.head(url, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            raise RuntimeError(f"No pude verificar la parte {i+1}: {e}")
        if head.status_code != 200:
            raise RuntimeError(
                f"ToDus respondió {resp.status_code} al subir la parte {i+1}, pero al "
                f"verificarla dio {head.status_code}: el archivo no quedó guardado."
            )
        remote = int(head.headers.get("Content-Length", -1))
        if remote != size:
            raise RuntimeError(
                f"La parte {i+1} quedó incompleta en ToDus ({remote} de {size} bytes)."
            )
        urls.append(url)
    return urls


def url_to_todus_sync(src_url, user_id, job_id, progress_cb):
    parsed = urlparse(src_url)
    filename = unquote(os.path.basename(parsed.path)) or f"{job_id}.bin"
    with requests.get(
        src_url, stream=True, timeout=60, headers={"Accept-Encoding": "identity"}
    ) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        if total <= 0:
            raise RuntimeError(
                "Ese servidor no informa el tamaño del archivo (Content-Length), "
                "y sin eso ToDus no acepta la subida en streaming."
            )
        urls = upload_stream_sync(r.raw, total, user_id, filename, job_id, progress_cb)
    return filename, urls


def bytes_to_todus_sync(data, user_id, filename, job_id, progress_cb):
    return upload_stream_sync(
        io.BytesIO(data), len(data), user_id, filename, job_id, progress_cb
    )


async def safe_edit(status_msg, text, job_id):
    try:
        await status_msg.edit_text(
            text, parse_mode="Markdown", reply_markup=cancel_keyboard(job_id)
        )
    except Exception:
        pass


def make_progress(status, job_id, label, loop):
    state = {"last": 0.0}

    def cb(done, total):
        now = time.time()
        if now - state["last"] < EDIT_EVERY and done != total:
            return
        state["last"] = now
        pct = done * 100 / total if total else 100
        text = (
            f"⬆️ {label}\n\n[{build_bar(pct)}] {pct:5.1f}%\n"
            f"{done/1048576:.1f} / {total/1048576:.1f} MB"
        )
        asyncio.run_coroutine_threadsafe(safe_edit(status, text, job_id), loop)

    return cb


async def finish(status, filename, urls):
    if len(urls) == 1:
        texto = f"✅ Subido correctamente\n`{filename}`\n\n`{urls[0]}`"
    else:
        enlaces = "\n".join(f"{i+1}. `{u}`" for i, u in enumerate(urls))
        texto = f"✅ Subido en {len(urls)} partes (> 2 GB)\n`{filename}`\n\n{enlaces}"
    try:
        await status.edit_text(texto, parse_mode="Markdown")
    except Exception:
        # Si Telegram no puede interpretar el Markdown, se manda como texto plano.
        await status.edit_text(texto.replace("`", ""))
    asyncio.create_task(recheck(status, texto, urls))


async def recheck(status, texto, urls):
    """A los 2 minutos vuelve a comprobar que ToDus no haya borrado el archivo."""
    await asyncio.sleep(120)
    try:
        estados = []
        for u in urls:
            h = await asyncio.to_thread(requests.head, u, timeout=30)
            estados.append(h.status_code)
        if all(c == 200 for c in estados):
            aviso = "\n\n🟢 Verificado a los 2 min: sigue en ToDus."
        else:
            aviso = f"\n\n🔴 A los 2 min ToDus ya lo borró (códigos {estados}). Hay que descargarlo enseguida o usar otro destino."
        try:
            await status.edit_text(texto + aviso, parse_mode="Markdown")
        except Exception:
            await status.edit_text((texto + aviso).replace("`", ""))
    except Exception:
        pass


async def report_error(status, e, filename=""):
    if isinstance(e, RuntimeError) and "CANCELLED" in str(e):
        await status.edit_text(f"❌ Cancelado.\n`{filename}`", parse_mode="Markdown")
    elif isinstance(e, requests.RequestException):
        await status.edit_text(f"⚠️ Error de red: {e}")
    else:
        await status.edit_text(f"⚠️ Error: {e}")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    tg_file = (
        msg.document or msg.video or msg.audio or msg.voice or msg.video_note
        or msg.sticker or (msg.photo[-1] if msg.photo else None)
    )
    if tg_file is None:
        await msg.reply_text("Envía un archivo o pega un enlace directo.")
        return

    job_id = tg_file.file_unique_id
    cancel_flags[job_id] = False
    filename = getattr(tg_file, "file_name", None)
    if not filename and msg.sticker is not None:
        ext = "webm" if msg.sticker.is_video else "tgs" if msg.sticker.is_animated else "webp"
        filename = f"sticker_{job_id}.{ext}"
    filename = filename or f"{job_id}.bin"

    status = await msg.reply_text(
        f"⬇️ Descargando desde Telegram...\n`{filename}`",
        parse_mode="Markdown",
        reply_markup=cancel_keyboard(job_id),
    )
    try:
        file_obj = await context.bot.get_file(tg_file.file_id)
        data = bytes(await file_obj.download_as_bytearray())
    except Exception as e:
        cancel_flags.pop(job_id, None)
        if "too big" in str(e).lower():
            await status.edit_text(
                "⚠️ Telegram solo deja descargar archivos de hasta ~20 MB a los bots.\n\n"
                "Si el archivo está en internet, mándame el *enlace* directo y "
                "lo paso a ToDus sin ese límite.",
                parse_mode="Markdown",
            )
        else:
            await status.edit_text(f"⚠️ No pude descargar desde Telegram: {e}")
        return

    loop = asyncio.get_running_loop()
    try:
        urls = await asyncio.to_thread(
            bytes_to_todus_sync, data, msg.chat_id, filename, job_id,
            make_progress(status, job_id, f"Subiendo a ToDus...\n`{filename}`", loop),
        )
        await finish(status, filename, urls)
    except Exception as e:
        await report_error(status, e, filename)
    finally:
        cancel_flags.pop(job_id, None)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = (msg.text or "").strip()
    if not URL_RE.match(text):
        await msg.reply_text("Mándame un archivo o un enlace http(s) directo de descarga.")
        return

    job_id = f"url_{msg.message_id}"
    cancel_flags[job_id] = False
    status = await msg.reply_text(
        f"⏳ Conectando...\n`{text}`",
        parse_mode="Markdown",
        reply_markup=cancel_keyboard(job_id),
    )
    loop = asyncio.get_running_loop()
    try:
        filename, urls = await asyncio.to_thread(
            url_to_todus_sync, text, msg.chat_id, job_id,
            make_progress(status, job_id, "Pasando el archivo a ToDus (streaming)...", loop),
        )
        await finish(status, filename, urls)
    except Exception as e:
        await report_error(status, e)
    finally:
        cancel_flags.pop(job_id, None)



def keepalive_server():
    """Render (Web Service) exige un puerto abierto: sirve un 'ok' en $PORT."""
    port = os.environ.get("PORT")
    if not port:
        return
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import threading

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        do_HEAD = do_GET

        def log_message(self, *a):
            pass

    threading.Thread(
        target=HTTPServer(("0.0.0.0", int(port)), H).serve_forever, daemon=True
    ).start()


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        sys.exit("Falta TELEGRAM_BOT_TOKEN.")
    keepalive_server()
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern=r"^cancel:"))
    app.add_handler(
        MessageHandler(
            filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO
            | filters.Sticker.ALL | filters.VOICE | filters.VIDEO_NOTE,
            handle_file,
        )
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Bot iniciado (versión ligera, sin disco ni servidor local).")
    app.run_polling()


if __name__ == "__main__":
    main()
