#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PUENTE TCP - Python 2.7.12


CONFIGURACIÓN:
- El script escucha en: <IP_PUENTE>:<PUERTO_PUENTE>
- Reenvía a: <IP_TARGET>:<PUERTO_TARGET>


USO CLIENTES: Conectarse a <IP_PUENTE>:<PUERTO_PUENTE>


Modos de uso:
- CLI (TCP):  python tcp_4guard.py <ip_puente> <puerto_puente> <ip_target> <puerto_target>
- CLI (MQTT): python tcp_4guard.py <ip_puente> <puerto_puente> <ip_target> <puerto_target> --mqtt --mqtt-broker <broker_ip> [--mqtt-port <port>] [--mqtt-topic <topic>] [--mqtt-user <user>] [--mqtt-pass <pass>] [--ota-topic <topic>]
- GUI:        python tcp_4guard.py  (sin argumentos) o con flag --gui

Dependencia MQTT (opcional): pip install paho-mqtt
"""

VERSION = '1.11.4'

import socket
import struct
import threading
import sys
import traceback
import time
import os
import sqlite3
import re
import datetime
import atexit
import signal
import json
import hashlib
try:
    import urllib2
except ImportError:
    urllib2 = None
try:
    import ConfigParser as configparser  # Python 2
except Exception:
    configparser = None
try:
    # Python 2
    import Tkinter as tk
    import ttk
except Exception:
    try:
        # Python 3
        import tkinter as tk
        from tkinter import ttk
    except Exception:
        tk = None
try:
    from Queue import Queue, Empty
except Exception:
    try:
        from queue import Queue, Empty
    except Exception:
        Queue = None
        Empty = Exception
try:
    import paho.mqtt.client as paho_mqtt
except Exception:
    paho_mqtt = None
try:
    import pymongo
except Exception:
    pymongo = None


def ensure_pymongo():
    """Instala pymongo via pip si no esta disponible. Compatible Python 2.7 y 3.x.
    En Python 2.7 usa pymongo==3.12.3 (ultima version con soporte Py2).
    Retorna (ok, msg)."""
    global pymongo
    if pymongo is not None:
        return (True, 'pymongo ya disponible')

    # Seleccionar version compatible segun Python
    if sys.version_info[0] < 3:
        # Python 2.7: ultima version compatible de pymongo
        package_spec = 'pymongo==3.12.3'
    else:
        # Python 3.x: cualquier version moderna
        package_spec = 'pymongo'

    def _try_pip(args_extra, label):
        """Ejecuta pip install con args_extra adicionales. Retorna (rc, out)."""
        import subprocess
        popen_kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
        }
        if os.name == 'nt':
            popen_kwargs['creationflags'] = 0x08000000
        cmd = [sys.executable, '-m', 'pip', 'install', '--user'] + list(args_extra) + [package_spec]
        try:
            proc = subprocess.Popen(cmd, **popen_kwargs)
            stdout, stderr = proc.communicate()
            rc = proc.returncode
            if isinstance(stderr, bytes):
                stderr = stderr.decode('utf-8', 'replace')
            if isinstance(stdout, bytes):
                stdout = stdout.decode('utf-8', 'replace')
            return rc, '{} {}'.format(stdout or '', stderr or '').strip()
        except Exception as e:
            return -1, '{} excepcion: {}'.format(label, e)

    def _refresh_and_import():
        """Limpia cache de imports y fuerza reimport de pymongo tras pip install."""
        global pymongo
        # 1. Agregar user site-packages al sys.path si no esta
        try:
            import site
            try:
                user_site = site.getusersitepackages()
            except Exception:
                user_site = None
            if user_site and user_site not in sys.path:
                sys.path.insert(0, user_site)
        except Exception:
            pass
        # 2. Invalidar cache de finders (Python 3.3+)
        try:
            import importlib
            if hasattr(importlib, 'invalidate_caches'):
                importlib.invalidate_caches()
        except Exception:
            pass
        # 3. Limpiar sys.modules de entradas previas (negative cache / parcial)
        for mod_name in list(sys.modules.keys()):
            if mod_name == 'pymongo' or mod_name.startswith('pymongo.'):
                try:
                    del sys.modules[mod_name]
                except Exception:
                    pass
        # 4. Intentar import ahora
        import pymongo as _pm
        pymongo = _pm

    # Intento 1: pip install normal (quiet)
    rc, out = _try_pip(['--quiet'], 'pip normal')
    if rc == 0:
        try:
            _refresh_and_import()
            return (True, 'pymongo {} instalado'.format(package_spec))
        except Exception as e:
            return (False, 'instalado pero no importa: {}'.format(e))

    # Intento 2: con trusted-host (para problemas SSL en Python 2.7 viejo)
    rc2, out2 = _try_pip(
        ['--trusted-host', 'pypi.org', '--trusted-host', 'files.pythonhosted.org'],
        'pip trusted-host')
    if rc2 == 0:
        try:
            _refresh_and_import()
            return (True, 'pymongo {} instalado (via trusted-host)'.format(package_spec))
        except Exception as e:
            return (False, 'instalado pero no importa: {}'.format(e))

    # Todo fallo: devolver el ultimo error
    return (False, 'pip install fallo (rc={}): {}'.format(
        rc2 if rc2 != 0 else rc, (out2 or out)[:400]))


# ========== Autostart Windows (Startup folder VBS, compat Win7/8/10/11) ==========
# Mecanismo primario: archivo .vbs en
#   %APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\PuenteDWRTD.vbs
# Compatible con Windows 7, 8, 10 y 11. No requiere admin.
# Mas visible que HKCU Run en Task Manager > Startup tab.
#
# Ademas migramos (desinstalamos) cualquier entry previa en HKCU Run que hayan
# creado versiones anteriores, para evitar doble arranque.

AUTOSTART_APP_NAME = 'PuenteDWRTD'
_AUTOSTART_REG_PATH = r'Software\Microsoft\Windows\CurrentVersion\Run'
_AUTOSTART_APPROVED_PATH = r'Software\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run'


def _autostart_pythonw_exe():
    """Retorna la ruta a pythonw.exe si existe, sino a python.exe."""
    exe = sys.executable
    pythonw = os.path.join(os.path.dirname(exe), 'pythonw.exe')
    if os.path.exists(pythonw):
        return pythonw
    return exe


def _autostart_vbs_path():
    """Retorna la ruta al archivo .vbs en la Startup folder del usuario actual."""
    if os.name != 'nt':
        return None
    appdata = os.environ.get('APPDATA')
    if not appdata:
        return None
    startup_dir = os.path.join(appdata, 'Microsoft', 'Windows',
                                'Start Menu', 'Programs', 'Startup')
    return os.path.join(startup_dir, AUTOSTART_APP_NAME + '.vbs')


def _autostart_vbs_content():
    """Genera el contenido del .vbs launcher (arranca pythonw.exe con el script, sin ventana)."""
    exe = _autostart_pythonw_exe()
    script = os.path.abspath(__file__)
    cwd = os.path.dirname(script)
    # Escapar comillas dobles duplicandolas (sintaxis VBS)
    exe_v = exe.replace('"', '""')
    script_v = script.replace('"', '""')
    cwd_v = cwd.replace('"', '""')
    # Chr(34) = " (comilla doble) para evitar peleas de escape
    return (
        "' Autostart launcher generado por tcp_4guard.py\n"
        "Set sh = CreateObject(\"WScript.Shell\")\n"
        "sh.CurrentDirectory = \"{cwd}\"\n"
        "sh.Run Chr(34) & \"{exe}\" & Chr(34) & \" \" & Chr(34) & \"{script}\" & Chr(34) & \" --gui\", 0, False\n"
    ).format(cwd=cwd_v, exe=exe_v, script=script_v)


def autostart_check():
    """Verifica si el .vbs existe y su contenido apunta al script actual.
    Retorna {exists, enabled, error, path, content_ok}."""
    if os.name != 'nt':
        return {'exists': False, 'enabled': False, 'error': 'solo Windows',
                'path': None, 'content_ok': False}
    path = _autostart_vbs_path()
    if not path:
        return {'exists': False, 'enabled': False, 'error': 'APPDATA no disponible',
                'path': None, 'content_ok': False}
    if not os.path.exists(path):
        return {'exists': False, 'enabled': False, 'error': None,
                'path': path, 'content_ok': False}
    try:
        with open(path, 'r') as f:
            content = f.read()
    except Exception as e:
        return {'exists': True, 'enabled': False, 'error': str(e),
                'path': path, 'content_ok': False}
    # Validar que apunte al script actual
    script = os.path.abspath(__file__)
    content_ok = (script in content) or (script.replace('\\', '\\\\') in content)
    return {'exists': True, 'enabled': content_ok, 'error': None,
            'path': path, 'content_ok': content_ok}


def autostart_install():
    """Crea o sobrescribe el .vbs en Startup folder. Tambien limpia el registry Run antiguo."""
    if os.name != 'nt':
        return (False, 'solo Windows')
    path = _autostart_vbs_path()
    if not path:
        return (False, 'No se pudo determinar la Startup folder (APPDATA missing)')
    try:
        # Asegurar que el directorio Startup existe (usualmente si, pero por si acaso)
        startup_dir = os.path.dirname(path)
        if not os.path.exists(startup_dir):
            try:
                os.makedirs(startup_dir)
            except Exception:
                pass
        content = _autostart_vbs_content()
        with open(path, 'w') as f:
            f.write(content)
        # Migracion: remover entry vieja de HKCU Run si existe (para evitar doble arranque)
        _autostart_registry_uninstall_silent()
        return (True, 'Autostart instalado: {}'.format(path))
    except Exception as e:
        return (False, 'Error escribiendo .vbs: {}'.format(e))


def autostart_enable():
    """Compat: re-instala el .vbs (equivalente a habilitar)."""
    return autostart_install()


def autostart_uninstall():
    """Elimina el .vbs y tambien limpia el registry Run antiguo."""
    if os.name != 'nt':
        return (False, 'solo Windows')
    path = _autostart_vbs_path()
    removed_vbs = False
    if path and os.path.exists(path):
        try:
            os.remove(path)
            removed_vbs = True
        except Exception as e:
            return (False, 'Error eliminando .vbs: {}'.format(e))
    # Limpiar tambien registry Run (por si habia entry antigua)
    _autostart_registry_uninstall_silent()
    if removed_vbs:
        return (True, 'Autostart desinstalado (.vbs eliminado)')
    return (True, 'No estaba instalado')


def _autostart_registry_uninstall_silent():
    """Remueve la entry vieja de HKCU Run (del mecanismo anterior). Best-effort silencioso."""
    if os.name != 'nt':
        return
    # v1.11.2: compat Py2 (_winreg) y Py3 (winreg). FileNotFoundError es Py3-only,
    # uso OSError que cubre WindowsError en Py2 y FileNotFoundError en Py3.
    try:
        import _winreg as winreg  # Python 2
    except Exception:
        try:
            import winreg  # Python 3
        except Exception:
            return
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _AUTOSTART_REG_PATH, 0,
                             winreg.KEY_SET_VALUE) as k:
            try:
                winreg.DeleteValue(k, AUTOSTART_APP_NAME)
            except OSError:
                pass
    except Exception:
        pass
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _AUTOSTART_APPROVED_PATH, 0,
                             winreg.KEY_SET_VALUE) as k:
            try:
                winreg.DeleteValue(k, AUTOSTART_APP_NAME)
            except OSError:
                pass
    except Exception:
        pass


def ensure_autostart():
    """Crea el .vbs si no existe, o lo regenera si el contenido cambio.
    Tambien limpia la entry vieja de HKCU Run (migracion). Idempotente."""
    if os.name != 'nt':
        return
    try:
        status = autostart_check()
        if not status['exists']:
            ok, msg = autostart_install()
            print('[Autostart] ' + msg)
        elif not status['content_ok']:
            # El .vbs existe pero apunta a otro script (ej: path movido) - regenerar
            ok, msg = autostart_install()
            print('[Autostart] Regenerado: ' + msg)
        else:
            print('[Autostart] Ya activo: {}'.format(status.get('path')))
        # Migracion silenciosa: limpiar HKCU Run aunque el .vbs ya este
        _autostart_registry_uninstall_silent()
    except Exception as e:
        print('[Autostart] Error: {}'.format(e))


# ========== Helpers para indicadores CLI con colores ANSI ==========

class CLIStatus(object):
    """Muestra una línea de estado actualizable en la terminal (modo CLI)."""
    # Códigos ANSI para colores
    RESET = '\033[0m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    # Símbolos para indicadores
    LED_ON = '\xe2\x97\x8f'   # ● (círculo lleno) - UTF-8 encoded para Python 2
    LED_OFF = '\xe2\x97\x8b'  # ○ (círculo vacío) - UTF-8 encoded para Python 2

    def __init__(self, bridge):
        self.bridge = bridge
        self._stop_event = threading.Event()
        self._thread = None
        self._last_line_len = 0

    def _colorize(self, text, color):
        """Aplica color ANSI al texto."""
        return '{}{}{}'.format(color, text, self.RESET)

    def _led(self, on, label):
        """Genera un indicador LED con etiqueta."""
        if on:
            return '{}{}{} {}'.format(self.GREEN, self.LED_ON, self.RESET, label)
        else:
            return '{}{}{} {}'.format(self.RED, self.LED_OFF, self.RESET, label)

    def _format_bytes(self, b):
        """Formatea bytes en KB/MB para legibilidad."""
        if b < 1024:
            return '{}B'.format(b)
        elif b < 1024 * 1024:
            return '{:.1f}KB'.format(b / 1024.0)
        else:
            return '{:.1f}MB'.format(b / (1024.0 * 1024.0))

    def _build_status_line(self):
        """Construye la línea de estado con indicadores."""
        try:
            st = self.bridge.get_status()
            rx_on = st.get('rx_active', False)
            client_on = st.get('client_count', 0) > 0
            lat_ms = st.get('latency_ms', -1)
            lat_ok = st.get('latency_ok', True)
            backlog_n = st.get('backlog_count', 0)
            backlog_b = st.get('backlog_bytes', 0)

            # Construir partes
            parts = []
            parts.append(self._led(rx_on, 'Rx'))
            parts.append(self._led(client_on, 'Cliente:{}'.format(st.get('client_count', 0))))

            # Latencia con color según estado
            if lat_ms < 0:
                lat_str = '--ms'
                parts.append(self._led(False, 'Lat:{}'.format(lat_str)))
            else:
                lat_str = '{:.0f}ms'.format(lat_ms)
                if lat_ok:
                    parts.append(self._led(True, 'Lat:{}'.format(lat_str)))
                else:
                    parts.append(self._led(False, 'Lat:{} PAUSADO'.format(lat_str)))

            # Backlog
            backlog_str = 'Backlog:{}msgs/{}'.format(backlog_n, self._format_bytes(backlog_b))
            if backlog_n > 0:
                parts.append(self._colorize(backlog_str, self.YELLOW))
            else:
                parts.append(backlog_str)

            return ' | '.join(parts)
        except Exception:
            return 'Estado: error obteniendo datos'

    def _clear_line(self):
        """Limpia la línea actual."""
        sys.stdout.write('\r' + ' ' * self._last_line_len + '\r')
        sys.stdout.flush()

    def _write_status(self):
        """Escribe la línea de estado."""
        line = self._build_status_line()
        # Limpiar línea anterior si era más larga
        clear_str = ' ' * max(0, self._last_line_len - len(line))
        sys.stdout.write('\r' + line + clear_str)
        sys.stdout.flush()
        self._last_line_len = len(line)

    def _loop(self):
        """Bucle principal que actualiza la línea de estado."""
        while not self._stop_event.is_set() and self.bridge.running:
            try:
                self._write_status()
            except Exception:
                pass
            self._stop_event.wait(1.0)  # Actualizar cada segundo
        # Limpiar al salir
        try:
            self._clear_line()
        except Exception:
            pass

    def start(self):
        """Inicia el hilo de actualización de estado."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        """Detiene el hilo de actualización."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        # Asegurar que la línea quede limpia
        try:
            self._clear_line()
        except Exception:
            pass

    def print_above(self, msg):
        """Imprime un mensaje arriba de la línea de estado (para logs)."""
        try:
            self._clear_line()
            sys.stdout.write(msg + '\n')
            sys.stdout.flush()
            self._write_status()
        except Exception:
            pass


# Variable global para el indicador CLI (usado por cli_print)
_cli_status_instance = None


def cli_print(msg):
    """Imprime un mensaje preservando la línea de estado CLI si está activa."""
    global _cli_status_instance
    if _cli_status_instance is not None:
        _cli_status_instance.print_above(msg)
    else:
        print(msg)


# ========== Helpers para encabezado TS por frame ==========

# Regex para detectar tramas completas WITS en un blob: && ... !!
FRAME_RX = re.compile(br'&&.*?!!', re.DOTALL)

# Regex para extraer item 0101 (Well ID / identificador del pozo) de una trama WITS.
# Formato: lineas de 4 digitos (RRII) seguidas del valor. Ej: "0101WELL_ABC"
ITEM_0101_RX = re.compile(br'(?:^|\n)\s*0101([^\r\n]+)', re.MULTILINE)

def iso8601(ts_epoch):
    """
    Devuelve un ISO-8601 UTC con milisegundos y 'Z' para un epoch en segundos (float).
    Compatible con Python 2.7.
    """
    dt = datetime.datetime.utcfromtimestamp(ts_epoch)
    ms = int(round((ts_epoch - int(ts_epoch)) * 1000.0))
    return "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, ms)

def add_ts_header_to_frames(blob_bytes, ts_epoch):
    """
    Inserta un encabezado 'TS:<ISO>\\n' por cada trama completa '&& ... !!' en el blob.
    Si no hay tramas completas en el blob, envuelve todo el blob como una sola trama con TS (fallback).
    """
    try:
        iso = iso8601(ts_epoch)
        iso_b = iso.encode('ascii')
    except Exception:
        iso_b = b'1970-01-01T00:00:00.000Z'

    out = bytearray()
    found = False
    for m in FRAME_RX.finditer(blob_bytes or b''):
        frame = m.group(0)  # b'&& ... !!'
        inner = frame[2:-2]  # quitar '&&' y '!!'
        out += b'&&TS:' + iso_b + b'\n' + inner + b'!!'
        found = True

    if not found:
        # Fallback: si no detectamos ninguna trama completa, envolvemos el blob entero como 1 frame
        out = b'&&TS:' + iso_b + b'\n' + (blob_bytes or b'') + b'!!'

    return bytes(out)


# ========== Tuning de sockets para redes intermitentes ==========
def tune_socket(sock, server=False, snd_buf=262144, rcv_buf=262144,
                nodelay=True, keepalive=True, keepidle=10, keepintvl=3, keepcnt=3):
    """Ajusta opciones del socket para mejorar resiliencia y latencia.
    - server=True evita aplicar TCP_NODELAY en sockets de escucha.
    - Maneja plataformas sin constantes específicas con try/except.
    """
    try:
        # Buffers de socket más grandes
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, int(snd_buf))
    except Exception:
        pass
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, int(rcv_buf))
    except Exception:
        pass

    if keepalive:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except Exception:
            pass
        # Windows: configurar tiempos de keepalive con ioctl si está disponible
        try:
            if hasattr(socket, 'SIO_KEEPALIVE_VALS'):
                sock.ioctl(socket.SIO_KEEPALIVE_VALS, struct.pack('=III', 1, int(keepidle*1000), int(keepintvl*1000)))
        except Exception:
            pass
        # Afinar tiempos de keepalive cuando esté disponible (Linux/Unix)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, int(keepidle))
        except Exception:
            pass
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(keepintvl))
        except Exception:
            pass
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(keepcnt))
        except Exception:
            pass

    # Deshabilitar Nagle para reducir latencia en tramas pequeñas
    if nodelay and not server:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass


# ========== Publisher MQTT ==========

class MQTTPublisher(object):
    """Publica frames WITS a un broker MQTT remoto."""
    def __init__(self, broker_ip, broker_port=1883, topic='puente/wits',
                 username=None, password=None):
        self.broker_ip = broker_ip
        self.broker_port = int(broker_port)
        self.topic = topic
        self.username = username
        self.password = password
        self._client = None
        self._connected = False
        self._lock = threading.Lock()
        # Watchdog de salud del MQTT
        self._maintain_stop = threading.Event()
        self._maintain_thread = None

    def _build_client(self):
        """Crea y configura un nuevo cliente paho-mqtt. No conecta todavia."""
        client = paho_mqtt.Client()
        if self.username:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=60)
        except Exception:
            pass
        return client

    def connect(self):
        if paho_mqtt is None:
            cli_print('[MQTT] ERROR: paho-mqtt no instalado. Instale con: pip install paho-mqtt')
            return False
        try:
            self._client = self._build_client()
            try:
                self._client.connect_async(self.broker_ip, self.broker_port, keepalive=30)
            except AttributeError:
                self._client.connect(self.broker_ip, self.broker_port, keepalive=30)
            self._client.loop_start()
            cli_print('[MQTT] Conectando a {}:{}  topic={}'.format(
                self.broker_ip, self.broker_port, self.topic))
            # Iniciar watchdog de salud
            self._maintain_stop.clear()
            self._maintain_thread = threading.Thread(target=self._maintain_loop)
            self._maintain_thread.daemon = True
            self._maintain_thread.start()
            return True
        except Exception as e:
            cli_print('[MQTT] Error conectando al broker: {}'.format(e))
            return False

    def _recreate_client(self):
        """Opcion nuclear: destruye el cliente actual y crea uno nuevo desde cero.
        Se usa cuando reconnect() simple no logra restablecer conexion."""
        try:
            old = self._client
            if old:
                try:
                    old.loop_stop()
                except Exception:
                    pass
                try:
                    old.disconnect()
                except Exception:
                    pass
            with self._lock:
                self._connected = False
            # Cliente nuevo
            self._client = self._build_client()
            try:
                self._client.connect_async(self.broker_ip, self.broker_port, keepalive=30)
            except AttributeError:
                self._client.connect(self.broker_ip, self.broker_port, keepalive=30)
            self._client.loop_start()
            cli_print('[MQTT] Cliente recreado, reintentando conexion...')
        except Exception as e:
            cli_print('[MQTT] Error recreando cliente: {}'.format(e))

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            with self._lock:
                self._connected = True
            try:
                client.subscribe  # no-op para compatibilidad
            except Exception:
                pass
            cli_print('[MQTT] Conectado al broker {}:{}'.format(self.broker_ip, self.broker_port))
        else:
            cli_print('[MQTT] Fallo conexion al broker, rc={} ({})'.format(rc, self._rc_to_str(rc)))

    def _on_disconnect(self, client, userdata, rc):
        with self._lock:
            self._connected = False
        if rc != 0:
            cli_print('[MQTT] Desconectado del broker (rc={}). Auto-reconnect activo...'.format(rc))

    @staticmethod
    def _rc_to_str(rc):
        mapping = {
            0: 'Conectado',
            1: 'version protocolo incorrecta',
            2: 'client_id invalido',
            3: 'servidor no disponible',
            4: 'usuario/password incorrecto',
            5: 'no autorizado',
        }
        return mapping.get(rc, 'desconocido')

    def is_connected(self):
        # Preferir el estado nativo de paho-mqtt (mas confiable)
        if self._client:
            try:
                return self._client.is_connected()
            except Exception:
                pass
        with self._lock:
            return self._connected

    def _maintain_loop(self):
        """Watchdog escalonado:
         - Tras 10s desconectado: intenta reconnect()
         - Tras 3 reintentos fallidos: destruye cliente y crea uno nuevo (opcion nuclear)
        """
        disconnected_since = 0.0
        reconnect_attempts = 0
        while not self._maintain_stop.is_set():
            try:
                if self._client is None:
                    break
                if not self.is_connected():
                    now = time.time()
                    if disconnected_since == 0.0:
                        disconnected_since = now
                        reconnect_attempts = 0
                    elif (now - disconnected_since) > 10.0:
                        reconnect_attempts += 1
                        if reconnect_attempts <= 3:
                            cli_print('[MQTT] Reintento #{} tras {:.0f}s desconectado'.format(
                                reconnect_attempts, now - disconnected_since))
                            try:
                                self._client.reconnect()
                            except Exception as e:
                                cli_print('[MQTT] reconnect() fallo: {}'.format(e))
                        else:
                            cli_print('[MQTT] {} reintentos fallidos, recreando cliente...'.format(reconnect_attempts - 1))
                            self._recreate_client()
                            reconnect_attempts = 0
                        disconnected_since = now  # reset timer para proximo intento
                else:
                    if disconnected_since > 0.0:
                        cli_print('[MQTT] Conexion restablecida')
                    disconnected_since = 0.0
                    reconnect_attempts = 0
            except Exception as e:
                cli_print('[MQTT] maintain error: {}'.format(e))
            self._maintain_stop.wait(5.0)

    def publish(self, payload):
        """Publica payload al topico configurado. Retorna True si se encoló."""
        if self._client is None:
            return False
        # Verificar conexion antes de publicar para evitar acumular en buffer interno
        # del cliente MQTT si el broker esta caido
        if not self.is_connected():
            return False
        try:
            # Asegurar que payload sea str (Python 2 paho-mqtt requiere str, no buffer)
            if not isinstance(payload, str):
                try:
                    payload = payload.decode('utf-8', 'replace')
                except Exception:
                    payload = str(payload)
            info = self._client.publish(self.topic, payload, qos=1)
            return info.rc == 0
        except Exception as e:
            cli_print('[MQTT] Error publicando: {}'.format(e))
            return False

    def disconnect(self):
        try:
            self._maintain_stop.set()
        except Exception:
            pass
        try:
            if self._client:
                self._client.loop_stop()
                self._client.disconnect()
        except Exception:
            pass
        with self._lock:
            self._connected = False
        self._client = None


# ========== Actualizacion OTA via MQTT ==========

class OTAUpdater(object):
    """Escucha comandos OTA via MQTT y actualiza el script remotamente.

    Mensaje esperado en el topic (JSON):
        {"version": "1.1.0", "url": "http://servidor/tcp_4guard.py", "sha256": "abc123..."}

    Flujo:
        1. Recibe mensaje en topic OTA
        2. Compara version (si ya esta instalada, ignora)
        3. Descarga el .py desde la URL
        4. Verifica SHA256
        5. Crea backup del script actual (.bak)
        6. Reemplaza el script
        7. Reinicia el proceso
    """
    def __init__(self, broker_ip, broker_port=1883, topic='puente/ota',
                 username=None, password=None, bridge=None,
                 test_comment_topic='puente/cmd/test_comment'):
        self.broker_ip = broker_ip
        self.broker_port = int(broker_port)
        self.topic = topic
        self.test_comment_topic = test_comment_topic
        self.username = username
        self.password = password
        self.bridge = bridge  # referencia al TCPBridge para dispatch de comandos
        self._client = None
        self._connected = False
        self._lock = threading.Lock()
        self._updating = False
        self._maintain_stop = threading.Event()
        self._maintain_thread = None

    def _build_client(self):
        """Crea y configura un nuevo cliente paho-mqtt para OTA."""
        client = paho_mqtt.Client(client_id='ota_{}'.format(os.getpid()))
        if self.username:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=60)
        except Exception:
            pass
        return client

    def start(self):
        """Conecta al broker MQTT y se suscribe al topic OTA."""
        if paho_mqtt is None:
            cli_print('[OTA] ERROR: paho-mqtt no instalado. pip install paho-mqtt')
            return False
        if not self.broker_ip:
            cli_print('[OTA] Sin broker configurado, OTA desactivado')
            return False
        try:
            self._client = self._build_client()
            try:
                self._client.connect_async(self.broker_ip, self.broker_port, keepalive=30)
            except AttributeError:
                self._client.connect(self.broker_ip, self.broker_port, keepalive=30)
            self._client.loop_start()
            cli_print('[OTA] Conectando a {}:{} topic={}'.format(
                self.broker_ip, self.broker_port, self.topic))
            # Iniciar watchdog
            self._maintain_stop.clear()
            self._maintain_thread = threading.Thread(target=self._maintain_loop)
            self._maintain_thread.daemon = True
            self._maintain_thread.start()
            return True
        except Exception as e:
            cli_print('[OTA] Error conectando: {}'.format(e))
            return False

    def _recreate_client(self):
        """Destruye el cliente OTA y crea uno nuevo desde cero."""
        try:
            old = self._client
            if old:
                try:
                    old.loop_stop()
                except Exception:
                    pass
                try:
                    old.disconnect()
                except Exception:
                    pass
            with self._lock:
                self._connected = False
            self._client = self._build_client()
            try:
                self._client.connect_async(self.broker_ip, self.broker_port, keepalive=30)
            except AttributeError:
                self._client.connect(self.broker_ip, self.broker_port, keepalive=30)
            self._client.loop_start()
            cli_print('[OTA] Cliente recreado, reintentando conexion...')
        except Exception as e:
            cli_print('[OTA] Error recreando cliente: {}'.format(e))

    def _maintain_loop(self):
        """Watchdog escalonado: reconnect() -> recrear cliente tras 3 fallos."""
        disconnected_since = 0.0
        reconnect_attempts = 0
        while not self._maintain_stop.is_set():
            try:
                if self._client is None:
                    break
                if not self.is_connected():
                    now = time.time()
                    if disconnected_since == 0.0:
                        disconnected_since = now
                        reconnect_attempts = 0
                    elif (now - disconnected_since) > 10.0:
                        reconnect_attempts += 1
                        if reconnect_attempts <= 3:
                            cli_print('[OTA] Reintento #{} tras {:.0f}s desconectado'.format(
                                reconnect_attempts, now - disconnected_since))
                            try:
                                self._client.reconnect()
                            except Exception as e:
                                cli_print('[OTA] reconnect() fallo: {}'.format(e))
                        else:
                            cli_print('[OTA] {} reintentos fallidos, recreando cliente...'.format(reconnect_attempts - 1))
                            self._recreate_client()
                            reconnect_attempts = 0
                        disconnected_since = now
                else:
                    if disconnected_since > 0.0:
                        cli_print('[OTA] Conexion restablecida')
                    disconnected_since = 0.0
                    reconnect_attempts = 0
            except Exception as e:
                cli_print('[OTA] maintain error: {}'.format(e))
            self._maintain_stop.wait(5.0)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            with self._lock:
                self._connected = True
            client.subscribe(self.topic, qos=1)
            cli_print('[OTA] Suscrito a {} (version actual={})'.format(self.topic, VERSION))
            # Suscribir tambien al topic de comandos de prueba (test_comment)
            try:
                if self.test_comment_topic and self.bridge is not None:
                    client.subscribe(self.test_comment_topic, qos=1)
                    cli_print('[CMD] Suscrito a {} (para inyectar comentarios de prueba)'.format(
                        self.test_comment_topic))
            except Exception:
                pass
        else:
            cli_print('[OTA] Fallo conexion al broker, rc={}'.format(rc))

    def _on_disconnect(self, client, userdata, rc):
        with self._lock:
            self._connected = False
        if rc != 0:
            cli_print('[OTA] Desconectado (rc={}). Auto-reconnect activo...'.format(rc))

    def _on_message(self, client, userdata, msg):
        """Despacha mensajes MQTT segun el topic: OTA vs comando de prueba."""
        try:
            payload = msg.payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8', 'replace')
            topic = getattr(msg, 'topic', '')
            if topic == self.test_comment_topic:
                # Comando de prueba: inyecta un SPARE500
                self._handle_test_comment(payload)
                return
            # Por defecto, tratar como comando OTA
            cli_print('[OTA] Comando recibido: {}'.format(payload[:300]))
            cmd = json.loads(payload)
            t = threading.Thread(target=self._process_update, args=(cmd,))
            t.daemon = True
            t.start()
        except Exception as e:
            cli_print('[OTA] Error parseando mensaje: {}'.format(e))

    def _handle_test_comment(self, payload):
        """Procesa un comando de test_comment. Acepta texto plano o JSON {text, author}."""
        if self.bridge is None:
            cli_print('[CMD] Sin bridge referenciado, ignorando test_comment')
            return
        try:
            text = None
            author = 'OTA-test'
            # Intentar parsear como JSON
            try:
                obj = json.loads(payload)
                if isinstance(obj, dict):
                    text = obj.get('text') or obj.get('message') or ''
                    author = obj.get('author') or author
                else:
                    text = str(obj)
            except Exception:
                text = payload  # texto plano
            if not text:
                cli_print('[CMD] test_comment sin texto, ignorando')
                return
            cli_print('[CMD] test_comment recibido, inyectando...')
            self.bridge.inject_test_comment(text, author=author)
        except Exception as e:
            cli_print('[CMD] Error manejando test_comment: {}'.format(e))

    def _process_update(self, cmd):
        """Ejecuta el flujo completo de actualizacion OTA."""
        if self._updating:
            cli_print('[OTA] Actualizacion ya en curso, ignorando')
            return
        self._updating = True
        try:
            version = cmd.get('version', '')
            url = cmd.get('url', '')
            sha256_expected = cmd.get('sha256', '').lower().strip()

            if not url:
                cli_print('[OTA] Comando sin URL, ignorando')
                return

            if version and version == VERSION:
                cli_print('[OTA] Version {} ya instalada, ignorando'.format(version))
                return

            cli_print('[OTA] === Iniciando actualizacion a v{} ==='.format(version or '?'))
            cli_print('[OTA] URL: {}'.format(url))

            # 1. Descargar
            cli_print('[OTA] Descargando...')
            try:
                new_code = self._download(url)
            except Exception as e:
                cli_print('[OTA] ERROR descargando: {}'.format(e))
                return
            cli_print('[OTA] Descargado: {} bytes'.format(len(new_code)))

            # 2. Verificar SHA256
            if sha256_expected:
                actual_hash = hashlib.sha256(new_code).hexdigest()
                if actual_hash != sha256_expected:
                    cli_print('[OTA] ERROR: SHA256 no coincide!')
                    cli_print('[OTA]   Esperado: {}'.format(sha256_expected))
                    cli_print('[OTA]   Actual:   {}'.format(actual_hash))
                    return
                cli_print('[OTA] SHA256 verificado OK')
            else:
                cli_print('[OTA] ADVERTENCIA: sin SHA256, omitiendo verificacion')

            # 3. Validacion basica: el archivo descargado debe ser Python valido
            try:
                compile(new_code, '<ota_update>', 'exec')
                cli_print('[OTA] Sintaxis Python verificada OK')
            except SyntaxError as e:
                cli_print('[OTA] ERROR: el archivo descargado tiene error de sintaxis: {}'.format(e))
                return

            # 4. Backup del script actual
            script_path = os.path.abspath(__file__)
            backup_path = script_path + '.bak'
            try:
                import shutil
                shutil.copy2(script_path, backup_path)
                cli_print('[OTA] Backup creado: {}'.format(backup_path))
            except Exception as e:
                cli_print('[OTA] ERROR creando backup: {}'.format(e))
                return

            # 5. Reemplazar script
            try:
                with open(script_path, 'wb') as f:
                    f.write(new_code)
                cli_print('[OTA] Script reemplazado exitosamente')
            except Exception as e:
                cli_print('[OTA] ERROR escribiendo script: {}'.format(e))
                # Rollback
                try:
                    shutil.copy2(backup_path, script_path)
                    cli_print('[OTA] Rollback ejecutado desde backup')
                except Exception:
                    cli_print('[OTA] ERROR CRITICO: no se pudo hacer rollback!')
                return

            # 6. Reiniciar proceso
            cli_print('[OTA] === Reiniciando proceso en 2 segundos... ===')
            time.sleep(2.0)
            try:
                _cleanup_all_bridges()
            except Exception:
                pass
            try:
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception:
                # Fallback para Windows
                try:
                    import subprocess
                    subprocess.Popen([sys.executable] + sys.argv)
                    os._exit(0)
                except Exception as e:
                    cli_print('[OTA] ERROR reiniciando: {}'.format(e))
                    cli_print('[OTA] Reinicie manualmente para aplicar la actualizacion')
        finally:
            self._updating = False

    def _download(self, url):
        """Descarga contenido desde URL. Compatible Python 2/3."""
        try:
            import urllib2 as _url_mod
        except ImportError:
            import urllib.request as _url_mod
        req = _url_mod.Request(url)
        resp = _url_mod.urlopen(req, timeout=60)
        data = resp.read()
        resp.close()
        return data

    def stop(self):
        """Desconecta del broker MQTT."""
        try:
            self._maintain_stop.set()
        except Exception:
            pass
        try:
            if self._client:
                self._client.loop_stop()
                self._client.disconnect()
        except Exception:
            pass
        with self._lock:
            self._connected = False
        self._client = None

    def is_connected(self):
        # Preferir el estado nativo de paho-mqtt
        if self._client:
            try:
                return self._client.is_connected()
            except Exception:
                pass
        with self._lock:
            return self._connected


# ========== Poller de comentarios externos (MongoDB -> MQTT) ==========

class ExternalCommentsPoller(object):
    """Polling a MongoDB externa para leer comentarios y retransmitirlos como tramas WITS.

    Flujo por tick:
      1. Lee last_ts persistido en SQLite (o now - backfill_hours si no existe)
      2. Consulta mongo: find({ts: {$gt: last_ts}}).sort(ts).limit(batch)
      3. Para cada doc: construye trama WITS con 0101 (well_id) + SPARE500 (comentario)
         y la encola en el backlog principal, compartiendo el mismo canal que los
         datos WITS normales (se entrega via TCP o MQTT segun el modo del bridge)
      4. Actualiza last_ts y persiste en SQLite
    """
    def __init__(self, mongo_cfg, mqtt_publisher, store,
                 field_ts='ts', field_text='text', field_author='author',
                 poll_interval=5.0, batch_size=500, backfill_hours=1,
                 db_getter=None):
        self.mongo_cfg = mongo_cfg  # dict: host, port, user, password, authdb, db, collection
        self.mqtt_publisher = mqtt_publisher  # no usado directamente; mantenido por compat
        self.store = store
        self.field_ts = field_ts
        self.field_text = field_text
        self.field_author = field_author
        self.poll_interval = float(poll_interval)
        self.batch_size = int(batch_size)
        self.backfill_hours = int(backfill_hours)
        # Callable que retorna el well ID detectado de item 0101. Tambien se usa como DB de Mongo.
        self.db_getter = db_getter

        self._stop_event = threading.Event()
        self._thread = None
        self._client = None
        self._collection = None
        # Backoff en caso de error conectando mongo
        self._backoff = 5.0
        self._backoff_max = 60.0
        # Metricas
        self._published_count = 0
        self._last_tick_docs = 0
        self._last_error = None

    def _resolve_db_name(self):
        """Retorna el nombre de DB: priorizar db_getter (well ID detectado); fallback a cfg."""
        if self.db_getter:
            try:
                dyn = self.db_getter()
                if dyn:
                    return str(dyn)
            except Exception:
                pass
        return self.mongo_cfg.get('db', '')

    @property
    def source_key(self):
        """Clave unica para identificar esta fuente en la tabla poller_state."""
        return 'mongo:{}:{}:{}/{}'.format(
            self.mongo_cfg.get('host', ''),
            self.mongo_cfg.get('port', 27017),
            self._resolve_db_name(),
            self.mongo_cfg.get('collection', ''))

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        if pymongo is None:
            cli_print('[ExtComments] ERROR: pymongo no instalado. pip install pymongo')
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        try:
            if self._client:
                self._client.close()
        except Exception:
            pass

    def _build_mongo_uri(self):
        cfg = self.mongo_cfg
        host = cfg.get('host', 'localhost')
        port = int(cfg.get('port', 27017))
        user = cfg.get('user')
        password = cfg.get('password')
        authdb = cfg.get('authdb', 'admin')
        db_name = self._resolve_db_name()
        if user:
            try:
                from urllib.parse import quote_plus as _quote_plus  # Python 3
            except ImportError:
                from urllib import quote_plus as _quote_plus  # Python 2
            user_enc = _quote_plus(user)
            pass_enc = _quote_plus(password or '')
            uri = 'mongodb://{}:{}@{}:{}/{}?authSource={}'.format(
                user_enc, pass_enc, host, port, db_name, authdb)
        else:
            uri = 'mongodb://{}:{}/{}'.format(host, port, db_name)
        return uri

    def _connect_mongo(self):
        """Establece conexion a Mongo. Retorna True si ok."""
        try:
            db_name = self._resolve_db_name()
            if not db_name:
                # Aun no tenemos well ID detectado; esperar
                cli_print('[ExtComments] Esperando Well ID (item 0101) de las tramas WITS...')
                return False
            uri = self._build_mongo_uri()
            self._client = pymongo.MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=15000)
            # Forzar validacion
            self._client.admin.command('ping')
            db = self._client[db_name]
            self._collection = db[self.mongo_cfg.get('collection', '')]
            cli_print('[ExtComments] Conectado a Mongo {}:{}/{}.{}'.format(
                self.mongo_cfg.get('host'), self.mongo_cfg.get('port'),
                db_name, self.mongo_cfg.get('collection')))
            self._backoff = 5.0
            return True
        except Exception as e:
            self._last_error = str(e)
            cli_print('[ExtComments] Error conectando a Mongo: {}'.format(e))
            try:
                if self._client:
                    self._client.close()
            except Exception:
                pass
            self._client = None
            self._collection = None
            return False

    def _get_initial_last_ts(self):
        """Obtiene last_ts persistido o lo inicializa (now - backfill_hours)."""
        persisted = self.store.get_last_ts(self.source_key)
        if persisted:
            return persisted
        # Backfill inicial
        start = datetime.datetime.utcnow() - datetime.timedelta(hours=self.backfill_hours)
        iso = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        self.store.set_last_ts(self.source_key, iso)
        cli_print('[ExtComments] Iniciando backfill desde {} ({}h atras)'.format(iso, self.backfill_hours))
        return iso

    def _ts_to_iso(self, ts_value):
        """Convierte un valor ts de Mongo a ISO 8601 UTC. Compat Python 2.7 y 3.x."""
        if isinstance(ts_value, datetime.datetime):
            if ts_value.tzinfo is None:
                return ts_value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            # tz-aware: convertir a UTC manualmente (datetime.timezone no existe en Py2.7)
            try:
                offset = ts_value.utcoffset()
            except Exception:
                offset = None
            if offset is not None:
                ts_utc_naive = ts_value.replace(tzinfo=None) - offset
            else:
                ts_utc_naive = ts_value.replace(tzinfo=None)
            return ts_utc_naive.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return str(ts_value)

    def _iso_to_datetime(self, ts_iso):
        """Parse ISO string a datetime UTC."""
        try:
            # Manejar sufijo Z
            s = ts_iso.rstrip('Z')
            if '.' in s:
                return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f')
            return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    def _build_wits_frame(self, well_id, ts_iso, text, author):
        """Construye una trama WITS minima con 0101 + SPARE500.

        Formato:
            &&
            0101<well_id>
            SPARE500<ts>|<author>|<text>
            !!
        """
        safe_text = str(text).replace('\r', ' ').replace('\n', ' ')
        safe_author = str(author or 'external').replace('|', '/').replace('\r', ' ').replace('\n', ' ')
        payload = '{}|{}|{}'.format(ts_iso, safe_author, safe_text)
        frame = '&&\n0101{}\nSPARE500{}\n!!'.format(well_id, payload)
        return frame.encode('utf-8')

    def _inject_comment(self, doc):
        """Convierte doc de Mongo en trama WITS y la encola en el backlog principal."""
        try:
            ts_raw = doc.get(self.field_ts)
            text = doc.get(self.field_text)
            author = doc.get(self.field_author)
            if ts_raw is None:
                cli_print('[ExtComments] Doc sin campo {}, skip. Keys: {}'.format(
                    self.field_ts, list(doc.keys())[:10]))
                return None
            if not text:
                cli_print('[ExtComments] Doc sin campo {} o vacio, skip. Keys: {}'.format(
                    self.field_text, list(doc.keys())[:10]))
                return None
            # Obtener well_id (item 0101) detectado del stream WITS
            well_id = ''
            if self.db_getter:
                try:
                    well_id = self.db_getter() or ''
                except Exception:
                    pass
            if not well_id:
                well_id = self.mongo_cfg.get('db', '') or 'UNKNOWN'
            ts_iso = self._ts_to_iso(ts_raw)
            frame_bytes = self._build_wits_frame(well_id, ts_iso, text, author)
            # Encolar en el mismo backlog que las tramas WITS
            self.store.enqueue(frame_bytes, time.time())
            self._published_count += 1
            # Log del frame inyectado (truncado)
            try:
                frame_preview = frame_bytes.decode('utf-8', 'replace').replace('\n', ' | ')[:200]
                cli_print('[ExtComments] Inyectado: {}'.format(frame_preview))
            except Exception:
                pass
            return ts_iso
        except Exception as e:
            cli_print('[ExtComments] Error procesando doc: {} (ts={}, text={})'.format(
                e, repr(doc.get(self.field_ts))[:50], repr(doc.get(self.field_text))[:50]))
            return None

    def _run(self):
        cli_print('[ExtComments] Poller iniciado (interval={}s, inyeccion como WITS SPARE500)'.format(
            self.poll_interval))
        current_db_name = None
        while not self._stop_event.is_set():
            # Detectar cambio de well ID: si cambia, forzar reconexion
            resolved = self._resolve_db_name()
            if current_db_name and resolved and resolved != current_db_name:
                cli_print('[ExtComments] Well ID cambio ({} -> {}), reconectando Mongo'.format(
                    current_db_name, resolved))
                try:
                    if self._client:
                        self._client.close()
                except Exception:
                    pass
                self._client = None
                self._collection = None
                current_db_name = None
            # Intentar (re)conectar si no hay conexion
            if self._collection is None:
                if not self._connect_mongo():
                    # Backoff exponencial
                    wait = self._backoff
                    self._backoff = min(self._backoff * 2, self._backoff_max)
                    if self._stop_event.wait(wait):
                        break
                    continue
                current_db_name = self._resolve_db_name()
            # Query Mongo
            try:
                last_ts_iso = self._get_initial_last_ts()
                last_ts_dt = self._iso_to_datetime(last_ts_iso)
                cursor = self._collection.find(
                    {self.field_ts: {'$gt': last_ts_dt}}
                ).sort(self.field_ts, 1).limit(self.batch_size)
                docs = list(cursor)
                self._last_tick_docs = len(docs)
                if docs:
                    newest_ts = last_ts_iso
                    injected = 0
                    for doc in docs:
                        if self._stop_event.is_set():
                            break
                        ts_iso = self._inject_comment(doc)
                        if ts_iso:
                            newest_ts = ts_iso
                            injected += 1
                    if newest_ts != last_ts_iso:
                        self.store.set_last_ts(self.source_key, newest_ts)
                    cli_print('[ExtComments] Tick: {} doc(s) encontrados, {} inyectado(s) como WITS SPARE500'.format(
                        len(docs), injected))
            except Exception as e:
                self._last_error = str(e)
                cli_print('[ExtComments] Error en tick ({}): {}'.format(
                    type(e).__name__, e))
                # Romper conexion para reconectar
                try:
                    if self._client:
                        self._client.close()
                except Exception:
                    pass
                self._client = None
                self._collection = None
                self._backoff = 5.0
            # Esperar siguiente tick
            if self._stop_event.wait(self.poll_interval):
                break
        cli_print('[ExtComments] Poller detenido')

    def get_status(self):
        return {
            'running': bool(self._thread and self._thread.is_alive()),
            'connected': self._collection is not None,
            'injected_count': self._published_count,
            'last_tick_docs': self._last_tick_docs,
            'last_error': self._last_error,
            'last_ts': self.store.get_last_ts(self.source_key) if self.store else None,
        }

    def test_connection(self):
        """Prueba la conexion Mongo y retorna info. Usado por GUI."""
        if pymongo is None:
            return (False, 'pymongo no instalado')
        try:
            uri = self._build_mongo_uri()
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000)
            client.admin.command('ping')
            db = client[self.mongo_cfg.get('db', '')]
            col = db[self.mongo_cfg.get('collection', '')]
            n = col.count_documents({}, limit=1)
            sample = col.find_one({}, sort=[(self.field_ts, -1)])
            client.close()
            msg = 'OK. Coleccion tiene al menos {} doc. Ultimo ts: {}'.format(
                n, sample.get(self.field_ts) if sample else 'N/A')
            return (True, msg)
        except Exception as e:
            return (False, str(e))


# ========== Núcleo del puente TCP ==========

# Registro global de bridges activos para apagado limpio en salida del proceso
_ACTIVE_BRIDGES = set()
_ACTIVE_BRIDGES_LOCK = threading.Lock()

def _cleanup_all_bridges():
    """Detiene todos los bridges activos. Llamado en salida del proceso."""
    try:
        with _ACTIVE_BRIDGES_LOCK:
            bridges = list(_ACTIVE_BRIDGES)
        for b in bridges:
            try:
                b.stop()
            except Exception:
                pass
    except Exception:
        pass

# Registrar limpieza en salida
atexit.register(_cleanup_all_bridges)

class TCPBridge:
    def __init__(self, listen_ip, listen_port, forward_ip, forward_port, source_ip=None,
                 delivery_mode='tcp', mqtt_broker_ip=None, mqtt_broker_port=1883,
                 mqtt_topic='puente/wits', mqtt_user=None, mqtt_pass=None,
                 status_api_url=None, status_api_interval=60, status_api_key=None,
                 ota_topic='puente/ota',
                 ext_comments_enabled=False, ext_comments_cfg=None):
        # ESCUCHA (donde los clientes se conectan)
        self.listen_ip = listen_ip
        self.listen_port = listen_port

        # DESTINO (a donde se reenvía)
        self.forward_ip = forward_ip
        self.forward_port = forward_port
        # IP local para salida (opcional). Si se define, se usará para bind() antes de connect().
        self.source_ip = source_ip

        # Modo de entrega: 'tcp' o 'mqtt'
        self.delivery_mode = delivery_mode
        self.mqtt_broker_ip = mqtt_broker_ip
        self.mqtt_broker_port = int(mqtt_broker_port) if mqtt_broker_port else 1883
        self.mqtt_topic = mqtt_topic or 'puente/wits'
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        self._mqtt_publisher = None
        self._mqtt_drain_thread = None
        self._mqtt_drain_stop = threading.Event()

        # Status API reporting
        self.status_api_url = status_api_url
        self.status_api_interval = int(status_api_interval) if status_api_interval else 60
        self.status_api_key = status_api_key
        self._status_report_thread = None
        self._status_report_stop = threading.Event()

        # OTA
        self.ota_topic = ota_topic or 'puente/ota'
        self._ota_updater = None

        # External comments poller
        self.ext_comments_enabled = bool(ext_comments_enabled)
        self.ext_comments_cfg = ext_comments_cfg or {}
        self._ext_comments_poller = None

        # Well ID detectado desde item 0101 de las tramas WITS recibidas
        self._detected_well_id = None
        self._detected_well_id_lock = threading.Lock()

        self.running = True
        self.server_socket = None
        # Control de clientes y recolector
        self._clients_lock = threading.Lock()
        self._client_count = 0
        self._collector_thread = None
        self._collector_stop = threading.Event()
        # Almacenamiento de backlog
        self.store = DataStore()
        # Estado de actividad Rx desde el TARGET
        self._last_rx_time = 0.0
        # Estado global del TARGET para evitar spam de logs entre clientes
        self._target_state_up = None  # None desconocido, True arriba, False caído
        self._last_target_log = 0.0
        # Estado de conexión del recolector al TARGET
        self._collector_connected = False
        # Reproducción de backlog configurables
        self.replay_batch_size = 100  # número de tramas a enviar por lote
        self.replay_batch_pause = 0.05  # pausa entre lotes (segundos)
        # Control de latencia
        self.latency_threshold_ms = 600  # umbral en ms para pausar entrega
        self._current_latency_ms = 0.0  # última latencia medida
        self._latency_ok = True  # True si latencia está bajo el umbral
        self._latency_lock = threading.Lock()
        self._latency_thread = None
        self._latency_stop = threading.Event()
        self._latency_probe_interval = 5.0  # segundos entre mediciones
        # Estado: ¿hay entrega activa al/los clientes?
        self._delivery_active = False
        # Sensibilidad a estancamiento/desconexión del cliente
        self.client_stall_timeout = 1.5  # TARGET->CLIENTE: segundos sin progreso para considerar cliente no receptivo
        # Estancamiento permitido para CLIENTE->TARGET (más laxo para no forzar reconexión del TARGET)
        self.target_stall_timeout = 15.0
        self.forward_max_pending = 256 * 1024  # 256KB máximos en buffer por dirección
        self.dest_timeout = 3.0  # timeout general de sockets hacia TARGET

    def _measure_latency(self):
        """Mide latencia al TARGET usando TCP connect time. Retorna ms o -1 si falla."""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)  # timeout máximo para el probe
            if self.source_ip:
                try:
                    sock.bind((self.source_ip, 0))
                except Exception:
                    pass
            t0 = time.time()
            sock.connect((self.forward_ip, self.forward_port))
            t1 = time.time()
            latency_ms = (t1 - t0) * 1000.0
            return latency_ms
        except Exception:
            return -1.0
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    def _latency_probe_loop(self):
        """Hilo que mide latencia periódicamente y actualiza el estado."""
        print('[LATENCY] Monitor iniciado (umbral={}ms, intervalo={}s)'.format(
            self.latency_threshold_ms, self._latency_probe_interval))
        while self.running and not self._latency_stop.is_set():
            try:
                latency = self._measure_latency()
                with self._latency_lock:
                    if latency < 0:
                        # No se pudo medir (TARGET inalcanzable)
                        self._current_latency_ms = -1.0
                        self._latency_ok = False
                    else:
                        self._current_latency_ms = latency
                        old_ok = self._latency_ok
                        self._latency_ok = (latency < self.latency_threshold_ms)
                        # Log solo en transiciones
                        if old_ok and not self._latency_ok:
                            cli_print('[LATENCY] Alta latencia detectada: {:.0f}ms >= {}ms - PAUSANDO entrega'.format(
                                latency, self.latency_threshold_ms))
                        elif not old_ok and self._latency_ok:
                            cli_print('[LATENCY] Latencia normalizada: {:.0f}ms < {}ms - REANUDANDO entrega'.format(
                                latency, self.latency_threshold_ms))
            except Exception:
                pass
            # Esperar hasta el próximo probe
            self._latency_stop.wait(self._latency_probe_interval)
        print('[LATENCY] Monitor detenido')

    def _start_latency_monitor(self):
        """Inicia el hilo monitor de latencia."""
        if self._latency_thread and self._latency_thread.is_alive():
            return
        self._latency_stop.clear()
        self._latency_thread = threading.Thread(target=self._latency_probe_loop)
        self._latency_thread.daemon = True
        self._latency_thread.start()

    def _stop_latency_monitor(self):
        """Detiene el hilo monitor de latencia."""
        try:
            self._latency_stop.set()
        except Exception:
            pass

    def is_latency_ok(self):
        """Retorna True si la latencia está bajo el umbral."""
        with self._latency_lock:
            return self._latency_ok

    def get_current_latency(self):
        """Retorna la última latencia medida en ms (-1 si no disponible)."""
        with self._latency_lock:
            return self._current_latency_ms

    # ---- MQTT helpers ----

    def _start_mqtt(self):
        """Inicia publisher MQTT y el hilo de drenaje del backlog."""
        if self.delivery_mode != 'mqtt':
            return
        if paho_mqtt is None:
            cli_print('[MQTT] ERROR: paho-mqtt no instalado. Instale con: pip install paho-mqtt')
            cli_print('[MQTT] Cayendo a modo TCP.')
            self.delivery_mode = 'tcp'
            return
        if not self.mqtt_broker_ip:
            cli_print('[MQTT] ERROR: No se configuró broker MQTT. Cayendo a modo TCP.')
            self.delivery_mode = 'tcp'
            return
        self._mqtt_publisher = MQTTPublisher(
            self.mqtt_broker_ip, self.mqtt_broker_port, self.mqtt_topic,
            self.mqtt_user, self.mqtt_pass)
        if not self._mqtt_publisher.connect():
            cli_print('[MQTT] No se pudo conectar al broker. Cayendo a modo TCP.')
            self._mqtt_publisher = None
            self.delivery_mode = 'tcp'
            return
        # Hilo de drenaje: publica frames del backlog SQLite via MQTT
        self._mqtt_drain_stop.clear()
        self._mqtt_drain_thread = threading.Thread(target=self._mqtt_drain_loop)
        self._mqtt_drain_thread.daemon = True
        self._mqtt_drain_thread.start()

    def _stop_mqtt(self):
        """Detiene publisher MQTT y el hilo de drenaje."""
        try:
            self._mqtt_drain_stop.set()
        except Exception:
            pass
        if self._mqtt_publisher:
            self._mqtt_publisher.disconnect()
            self._mqtt_publisher = None

    def _mqtt_drain_loop(self):
        """Lee frames del backlog SQLite y los publica via MQTT."""
        cli_print('[MQTT-DRAIN] Hilo de drenaje iniciado')
        last_warn_time = 0.0
        while self.running and not self._mqtt_drain_stop.is_set():
            try:
                # Si el broker no esta conectado, esperar sin tocar el backlog
                if self._mqtt_publisher and not self._mqtt_publisher.is_connected():
                    now = time.time()
                    if (now - last_warn_time) > 10.0:
                        try:
                            pending = self.store.count()
                        except Exception:
                            pending = -1
                        cli_print('[MQTT-DRAIN] Broker desconectado, acumulando en backlog (pendientes={})'.format(pending))
                        last_warn_time = now
                    self._mqtt_drain_stop.wait(2.0)
                    continue
                batch = self.store.dequeue_batch(limit=self.replay_batch_size)
                if not batch:
                    self._mqtt_drain_stop.wait(0.1)
                    continue
                ids_to_delete = []
                for row_id, ts, data in batch:
                    if not self.running or self._mqtt_drain_stop.is_set():
                        break
                    try:
                        if self._mqtt_publisher and self._mqtt_publisher.publish(data):
                            ids_to_delete.append(row_id)
                        else:
                            # Broker no disponible; esperar y reintentar
                            time.sleep(1.0)
                            break
                    except Exception as e:
                        cli_print('[MQTT-DRAIN] Error publicando frame: {}'.format(e))
                        time.sleep(1.0)
                        break
                if ids_to_delete:
                    self.store.delete_ids(ids_to_delete)
                    try:
                        pending = self.store.count()
                    except Exception:
                        pending = -1
                    cli_print('[MQTT-DRAIN] +{} frame(s) publicado(s) (pendientes={})'.format(
                        len(ids_to_delete), pending))
                time.sleep(self.replay_batch_pause)
            except Exception as e:
                cli_print('[MQTT-DRAIN] Error: {}'.format(e))
                time.sleep(1.0)
        cli_print('[MQTT-DRAIN] Hilo de drenaje detenido')

    # ---------- Status API reporter ----------

    def _start_status_reporter(self):
        """Inicia el hilo que reporta status al API externo."""
        if not self.status_api_url:
            return
        if urllib2 is None:
            cli_print('[STATUS-API] ERROR: urllib2 no disponible, reporte desactivado')
            return
        if self._status_report_thread and self._status_report_thread.is_alive():
            return
        self._status_report_stop.clear()
        self._status_report_thread = threading.Thread(target=self._status_reporter_loop)
        self._status_report_thread.daemon = True
        self._status_report_thread.start()

    def _stop_status_reporter(self):
        """Detiene el hilo de reporte de status."""
        try:
            self._status_report_stop.set()
        except Exception:
            pass

    def _status_reporter_loop(self):
        """Envía status del bridge al API externo cada N segundos."""
        cli_print('[STATUS-API] Reporte iniciado -> {}'.format(self.status_api_url))
        while self.running and not self._status_report_stop.is_set():
            try:
                status = self.get_status()
                status['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                status['forward_ip'] = self.forward_ip
                status['forward_port'] = self.forward_port
                status['listen_ip'] = self.listen_ip
                status['listen_port'] = self.listen_port
                body = json.dumps(status)
                if isinstance(body, bytes):
                    pass
                else:
                    body = body.encode('utf-8')
                req = urllib2.Request(self.status_api_url, body)
                req.add_header('Content-Type', 'application/json')
                if self.status_api_key:
                    req.add_header('Authorization', 'Bearer {}'.format(self.status_api_key))
                resp = urllib2.urlopen(req, timeout=10)
                resp.read()
                resp.close()
            except Exception as e:
                cli_print('[STATUS-API] Error enviando status: {}'.format(e))
            self._status_report_stop.wait(self.status_api_interval)
        cli_print('[STATUS-API] Reporte detenido')

    # ---- OTA helpers ----

    def _start_ota(self):
        """Inicia el actualizador OTA si hay broker MQTT configurado."""
        broker = self.mqtt_broker_ip
        if not broker:
            return
        self._ota_updater = OTAUpdater(
            broker_ip=broker,
            broker_port=self.mqtt_broker_port,
            topic=self.ota_topic,
            username=self.mqtt_user,
            password=self.mqtt_pass,
            bridge=self)  # para que pueda inyectar comentarios de prueba
        self._ota_updater.start()

    def _stop_ota(self):
        """Detiene el actualizador OTA."""
        if self._ota_updater:
            self._ota_updater.stop()
            self._ota_updater = None

    def is_ota_connected(self):
        """Retorna True si el OTA esta conectado al broker."""
        if self._ota_updater:
            return self._ota_updater.is_connected()
        return False

    def get_detected_well_id(self):
        """Retorna el Well ID extraido del item 0101 de las tramas WITS, o None."""
        with self._detected_well_id_lock:
            return self._detected_well_id

    def inject_test_comment(self, text, author='OTA-test'):
        """Inyecta una trama WITS con SPARE500 como si viniera de Mongo.
        Util para pruebas remotas via MQTT o boton en GUI."""
        well_id = self.get_detected_well_id() or 'TEST_WELL'
        ts_iso = iso8601(time.time())
        safe_text = str(text).replace('\r', ' ').replace('\n', ' ')[:500]
        safe_author = str(author or 'test').replace('|', '/').replace('\r', ' ').replace('\n', ' ')
        payload = '{}|{}|{}'.format(ts_iso, safe_author, safe_text)
        frame = '&&\n0101{}\nSPARE500{}\n!!'.format(well_id, payload)
        try:
            self.store.enqueue(frame.encode('utf-8'), time.time())
            cli_print('[TEST] Comentario de prueba inyectado: well_id={} author={} text={}'.format(
                well_id, safe_author, safe_text[:60]))
            return True
        except Exception as e:
            cli_print('[TEST] Error inyectando: {}'.format(e))
            return False

    def _try_extract_well_id(self, frame_bytes):
        """Extrae el item 0101 de una trama WITS y lo guarda si es nuevo/valido."""
        try:
            m = ITEM_0101_RX.search(frame_bytes)
            if not m:
                return
            raw = m.group(1).strip()
            if not raw:
                return
            try:
                well_id = raw.decode('ascii', 'replace').strip()
            except Exception:
                well_id = str(raw).strip()
            if not well_id:
                return
            with self._detected_well_id_lock:
                if self._detected_well_id != well_id:
                    old = self._detected_well_id
                    self._detected_well_id = well_id
                    if old is None:
                        cli_print('[COLLECTOR] Well ID detectado (item 0101): {}'.format(well_id))
                    else:
                        cli_print('[COLLECTOR] Well ID cambio: {} -> {}'.format(old, well_id))
        except Exception:
            pass

    # ---- External comments helpers ----

    def _start_ext_comments(self):
        """Inicia el poller de comentarios externos si esta habilitado."""
        if not self.ext_comments_enabled:
            return
        if pymongo is None:
            cli_print('[ExtComments] pymongo no disponible, intentando instalar automaticamente...')
            ok, msg = ensure_pymongo()
            cli_print('[ExtComments] ' + msg)
            if not ok:
                cli_print('[ExtComments] Instale manualmente: pip install pymongo')
                return
        cfg = self.ext_comments_cfg
        mongo_cfg = {
            'host': cfg.get('host', 'localhost'),
            'port': int(cfg.get('port', 27017)),
            'user': cfg.get('user'),
            'password': cfg.get('password'),
            'authdb': cfg.get('authdb', 'admin'),
            'db': cfg.get('db'),
            'collection': cfg.get('collection'),
        }
        self._ext_comments_poller = ExternalCommentsPoller(
            mongo_cfg=mongo_cfg,
            mqtt_publisher=self._mqtt_publisher,
            store=self.store,
            field_ts=cfg.get('field_ts', 'ts'),
            field_text=cfg.get('field_text', 'text'),
            field_author=cfg.get('field_author', 'author'),
            poll_interval=float(cfg.get('interval', 5.0)),
            backfill_hours=int(cfg.get('backfill_hours', 1)),
            db_getter=self.get_detected_well_id)
        self._ext_comments_poller.start()

    def _stop_ext_comments(self):
        if self._ext_comments_poller:
            try:
                self._ext_comments_poller.stop()
            except Exception:
                pass
            self._ext_comments_poller = None

    def is_mqtt_connected(self):
        """Retorna True si el publisher MQTT está conectado."""
        if self._mqtt_publisher:
            return self._mqtt_publisher.is_connected()
        return False

    def start(self):
        """Inicia el servidor puente"""
        try:
            # Registrar en conjunto global
            try:
                with _ACTIVE_BRIDGES_LOCK:
                    _ACTIVE_BRIDGES.add(self)
            except Exception:
                pass
            # Crear socket servidor solo en modo TCP
            if self.delivery_mode != 'mqtt':
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    tune_socket(self.server_socket, server=True)
                except Exception:
                    pass
                self.server_socket.bind((self.listen_ip, self.listen_port))
                self.server_socket.listen(100)
                try:
                    self.server_socket.settimeout(1.0)
                except Exception:
                    pass

            print("=" * 60)
            print("PUENTE WITS INICIADO")
            print("=" * 60)
            print("MODO DE ENTREGA: {}".format(self.delivery_mode.upper()))
            if self.delivery_mode == 'mqtt':
                print("MQTT BROKER: {}:{}  TOPIC: {}".format(
                    self.mqtt_broker_ip, self.mqtt_broker_port, self.mqtt_topic))
            print("FUENTE WITS: {}:{}".format(self.forward_ip, self.forward_port))
            if self.delivery_mode == 'tcp':
                print("ESCUCHANDO CLIENTES EN: {}:{}".format(self.listen_ip, self.listen_port))
            else:
                print("ENTREGA VIA MQTT al topic: {}".format(self.mqtt_topic))
            print("=" * 60)
            print("Presiona Ctrl+C para detener...")
            print("")
            # Iniciar recolector (no hay clientes al inicio)
            self._start_collector()
            # Iniciar MQTT si corresponde
            self._start_mqtt()
            # Iniciar reporte de status al API externo
            self._start_status_reporter()
            # Iniciar OTA
            self._start_ota()
            # Iniciar poller de comentarios externos
            self._start_ext_comments()
            # NOTA: Monitor de latencia desactivado por defecto para evitar interferencia
            # con servidores que solo aceptan una conexión. La latencia se estima ahora
            # desde el tiempo de respuesta del recolector.
            # self._start_latency_monitor()

            if self.delivery_mode == 'mqtt':
                # En modo MQTT no aceptamos clientes TCP, solo esperamos
                while self.running:
                    time.sleep(1.0)
            else:
                while self.running:
                    try:
                        # Aceptar conexión entrante de CLIENTE
                        client_socket = None
                        try:
                            client_socket, client_addr = self.server_socket.accept()
                            try:
                                tune_socket(client_socket)
                            except Exception:
                                pass
                            try:
                                client_socket.settimeout(3.0)
                            except Exception:
                                pass
                            # Modo 1-cliente: rechazar conexiones adicionales si ya hay un cliente activo
                            deny = False
                            try:
                                with self._clients_lock:
                                    if self._client_count >= 1:
                                        deny = True
                            except Exception:
                                pass
                            if deny:
                                # Silenciar log y no enviar nada al cliente adicional; cerrar de inmediato
                                pass
                                try:
                                    client_socket.shutdown(socket.SHUT_RDWR)
                                except Exception:
                                    pass
                                try:
                                    client_socket.close()
                                except Exception:
                                    pass
                                client_socket = None
                                continue
                            # Reducir buffer de envío hacia el CLIENTE para detectar más rápido falta de lectura
                            try:
                                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 16384)
                            except Exception:
                                pass
                            cli_print("[+] Cliente conectado: {}:{}".format(client_addr[0], client_addr[1]))
                            cli_print("    Intentando conectar al target: {}:{}".format(self.forward_ip, self.forward_port))

                            # Manejar cliente en hilo separado
                            client_thread = threading.Thread(
                                target=self.handle_client,
                                args=(client_socket, client_addr)
                            )
                            client_thread.daemon = True
                            client_thread.start()
                            # Transferimos propiedad del socket al hilo
                            client_socket = None
                        finally:
                            # Si ocurrió un error antes de arrancar el hilo, cerrar el socket aceptado
                            if client_socket is not None:
                                try:
                                    client_socket.shutdown(socket.SHUT_RDWR)
                                except Exception:
                                    pass
                                try:
                                    client_socket.close()
                                except Exception:
                                    pass

                    except socket.timeout:
                        continue
                    except Exception as e:
                        if self.running:
                            print("[-] Error aceptando cliente: {}".format(e))

        except Exception as e:
            print("[-] Error iniciando puente: {}".format(e))
            traceback.print_exc()
        finally:
            # Cerrar socket servidor
            try:
                if self.server_socket:
                    self.server_socket.close()
            except Exception:
                pass
            self._stop_collector()
            self._stop_ext_comments()
            self._stop_mqtt()
            self._stop_ota()
            self._stop_latency_monitor()

    def handle_client(self, client_socket, client_addr):
        """Maneja cada conexión de cliente"""
        dest_socket = None

        try:
            # Registrar cliente conectado
            with self._clients_lock:
                self._client_count += 1
                # Ya no detenemos el recolector; quedará ocioso si hay entrega activa

            # Modo 'acumular primero': este handler NO se conecta al TARGET.
            # Solo entrega al cliente los datos acumulados en SQLite por el recolector.
            client_socket.settimeout(1.0)
            dest_socket = None

            # Helper de envío con reintentos para manejar estancamientos temporales
            def _send_all_resilient(sock, data_bytes, stall_timeout=5.0):
                view = memoryview(data_bytes)
                total = 0
                last_progress = time.time()
                while self.running and total < len(view):
                    try:
                        sent = sock.send(view[total:])
                        if sent is None:
                            sent = 0
                        if sent == 0:
                            # Socket posiblemente cerrado
                            raise Exception('socket_cerrado')
                        total += sent
                        if sent > 0:
                            last_progress = time.time()
                    except socket.timeout:
                        # Backoff breve y reintentar; no perder datos
                        time.sleep(0.1)
                    # Detectar estancamiento prolongado
                    if (time.time() - last_progress) > stall_timeout:
                        raise Exception('send_stall_timeout')
                return total

            def _client_is_alive(sock):
                try:
                    try:
                        peek = sock.recv(1, socket.MSG_PEEK)
                        if peek == b'':
                            return False
                    except socket.timeout:
                        pass
                    return True
                except Exception:
                    return False

            # Entrega continua desde el backlog (incluye backlog histórico y frames en tiempo real)
            try:
                try:
                    self._delivery_active = True
                except Exception:
                    pass
                total_sent = 0
                idle_loops = 0
                while self.running and _client_is_alive(client_socket):
                    # Verificar latencia antes de entregar
                    if not self.is_latency_ok():
                        # Latencia alta: pausar entrega, acumular en backlog
                        time.sleep(0.5)
                        continue
                    batch = self.store.dequeue_batch(limit=self.replay_batch_size)
                    if not batch:
                        idle_loops += 1
                        time.sleep(0.05)
                        continue
                    ids_to_delete = []
                    for row_id, ts, data in batch:
                        try:
                            payload = add_ts_header_to_frames(data, ts)
                            _ = _send_all_resilient(client_socket, payload, stall_timeout=self.client_stall_timeout)
                            total_sent += len(payload)
                            ids_to_delete.append(row_id)
                        except Exception:
                            cli_print("[-] Cliente desconectado durante entrega de backlog")
                            raise
                    if ids_to_delete:
                        self.store.delete_ids(ids_to_delete)
                        # Log en GUI: indicar cuántos frames fueron enviados en este lote y cuántos quedan
                        try:
                            pending = self.store.count()
                        except Exception:
                            pending = -1
                        try:
                            cli_print('[DELIVERY] +{} frame(s) enviado(s) (pendientes={})'.format(len(ids_to_delete), pending))
                        except Exception:
                            pass
                    # Pausa pequeña para no monopolizar CPU ni bloquear al recolector
                    time.sleep(self.replay_batch_pause)
                if total_sent:
                    cli_print("[DELIVERY] Enviados {} bytes desde backlog".format(total_sent))
            except Exception:
                raise

            try:
                self._delivery_active = False
            except Exception:
                pass
            cli_print("[-] Conexión cliente finalizada: {}:{}".format(client_addr[0], client_addr[1]))

        except socket.timeout:
            print("[-] Timeout entregando datos al cliente {}:{}".format(client_addr[0], client_addr[1]))
        except socket.error as e:
            print("[-] Error de socket con cliente {}:{} - {}".format(client_addr[0], client_addr[1], e))
        except Exception as e:
            print("[-] Error manejando cliente: {}".format(e))
        finally:
            # Cerrar sockets
            try:
                try:
                    client_socket.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                client_socket.close()
            except:
                pass
            try:
                if dest_socket:
                    try:
                        dest_socket.shutdown(socket.SHUT_RDWR)
                    except Exception:
                        pass
                    dest_socket.close()
            except:
                pass
            # Registrar cliente desconectado
            with self._clients_lock:
                if self._client_count > 0:
                    self._client_count -= 1
                # Si no hay clientes, reanudar recolector
                if self._client_count == 0 and self.running:
                    self._start_collector()

    def stop(self):
        """Detiene el puente limpiamente"""
        self.running = False
        # Cerrar socket de escucha para desbloquear accept()
        try:
            if self.server_socket:
                self.server_socket.close()
        except Exception:
            pass
        # Detener recolector
        self._stop_collector()
        # Detener MQTT
        self._stop_mqtt()
        # Detener reporte de status
        self._stop_status_reporter()
        # Detener OTA
        self._stop_ota()
        # Detener poller de comentarios externos
        self._stop_ext_comments()
        # Detener monitor de latencia
        self._stop_latency_monitor()
        # Desregistrar de conjunto global
        try:
            with _ACTIVE_BRIDGES_LOCK:
                if self in _ACTIVE_BRIDGES:
                    _ACTIVE_BRIDGES.discard(self)
        except Exception:
            pass

    def _start_collector(self):
        """Inicia el hilo recolector que toma datos del TARGET cuando no hay clientes"""
        if self._collector_thread and self._collector_thread.is_alive():
            return
        self._collector_stop.clear()
        self._collector_thread = threading.Thread(target=self._collector_loop)
        self._collector_thread.daemon = True
        self._collector_thread.start()

    def _stop_collector(self):
        """Detiene el hilo recolector"""
        try:
            self._collector_stop.set()
        except Exception:
            pass
        # No unimos aquí para no bloquear; se apagará solo

    def _collector_loop(self):
        """Conecta al target y guarda datos en SQLite (siempre activo).

        Incluye watchdog para detectar conexiones colgadas silenciosamente:
        si no llega data en STALE_RX_TIMEOUT segundos, fuerza reconexion.
        """
        STALE_RX_TIMEOUT = 30.0  # segundos sin data -> forzar reconexion
        RECONNECT_BACKOFF_MAX = 30.0  # max espera entre intentos de reconexion
        print('[COLLECTOR] Iniciado (acumulando desde TARGET siempre)')
        sock = None
        buf = bytearray()  # buffer para ensamblar tramas completas '&& ... !!'
        last_data_time = time.time()
        reconnect_attempts = 0

        def _close_and_reset():
            nonlocal_dict['sock'] = None
            nonlocal_dict['buf'] = bytearray()
            try:
                self._collector_connected = False
            except Exception:
                pass

        # Workaround para nonlocal en Python 2/3 compat
        nonlocal_dict = {'sock': sock, 'buf': buf}

        try:
            while self.running and not self._collector_stop.is_set():
                sock = nonlocal_dict['sock']
                buf = nonlocal_dict['buf']
                try:
                    if sock is None:
                        # Backoff exponencial con tope para no saturar logs/CPU en caidas prolongadas
                        backoff = min(float(reconnect_attempts), RECONNECT_BACKOFF_MAX)
                        if backoff > 0:
                            cli_print('[COLLECTOR] Reintentando conexion en {:.0f}s (intento #{})'.format(
                                backoff, reconnect_attempts))
                            for _ in range(int(backoff * 10)):
                                if not self.running or self._collector_stop.is_set():
                                    break
                                time.sleep(0.1)
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        try:
                            tune_socket(sock)
                        except Exception:
                            pass
                        try:
                            if self.source_ip:
                                sock.bind((self.source_ip, 0))
                        except Exception:
                            pass
                        sock.connect((self.forward_ip, self.forward_port))
                        try:
                            sock.settimeout(1.0)
                        except Exception:
                            pass
                        nonlocal_dict['sock'] = sock
                        cli_print('[COLLECTOR] Conectado a {}:{} (colector activo)'.format(self.forward_ip, self.forward_port))
                        try:
                            self._collector_connected = True
                            # Considerar actividad en el instante de conexión
                            self._last_rx_time = time.time()
                            last_data_time = time.time()
                        except Exception:
                            pass
                        reconnect_attempts = 0
                    try:
                        data = sock.recv(4096)
                        if not data:
                            # Conexion cerrada por el TARGET: reconectar
                            cli_print('[COLLECTOR] TARGET cerro la conexion, reconectando...')
                            try:
                                sock.close()
                            except Exception:
                                pass
                            _close_and_reset()
                            reconnect_attempts = min(reconnect_attempts + 1, int(RECONNECT_BACKOFF_MAX))
                            time.sleep(0.5)
                            continue
                        # Marca de actividad Rx inmediata (cualquier dato)
                        last_data_time = time.time()
                        try:
                            self._last_rx_time = last_data_time
                        except Exception:
                            pass
                        # Acumular y extraer tramas completas con su propia marca de tiempo
                        buf.extend(data)
                        nonlocal_dict['buf'] = buf
                        bview = bytes(buf)
                        processed_end = 0
                        frames_enqueued = 0
                        for m in FRAME_RX.finditer(bview):
                            frame = m.group(0)  # incluye '&&' y '!!'
                            processed_end = m.end()
                            ts_frame = time.time()  # instante de detección de frame completo
                            # Encolar frame individual con su propia huella temporal
                            self.store.enqueue(frame, ts_frame)
                            frames_enqueued += 1
                            # Intentar extraer Well ID (item 0101) de la trama
                            self._try_extract_well_id(frame)
                            try:
                                self._last_rx_time = ts_frame
                            except Exception:
                                pass
                        if processed_end:
                            # Descartar bytes ya procesados
                            del buf[:processed_end]
                            nonlocal_dict['buf'] = buf
                        # Evitar crecimiento ilimitado si no llegan delimitadores
                        if len(buf) > 1024 * 1024:
                            # conservar último 1MB como seguridad
                            buf = bytearray(buf[-1024 * 1024:])
                            nonlocal_dict['buf'] = buf
                        if frames_enqueued:
                            cli_print('[COLLECTOR] +{} frame(s) acumulado(s) (pendientes={})'.format(
                                frames_enqueued, self.store.count()))
                    except socket.timeout:
                        # Watchdog: si no llega data por mucho tiempo, la red puede estar caida silenciosamente
                        if (time.time() - last_data_time) > STALE_RX_TIMEOUT:
                            cli_print('[COLLECTOR] Sin data por {:.0f}s, conexion posiblemente colgada, reconectando...'.format(
                                time.time() - last_data_time))
                            try:
                                sock.shutdown(socket.SHUT_RDWR)
                            except Exception:
                                pass
                            try:
                                sock.close()
                            except Exception:
                                pass
                            _close_and_reset()
                            reconnect_attempts = min(reconnect_attempts + 1, int(RECONNECT_BACKOFF_MAX))
                            last_data_time = time.time()  # reset para proximo intento
                            continue
                        continue
                except Exception as e:
                    # Problema conectando/recibiendo; reintentar con backoff
                    cli_print('[COLLECTOR] Error: {}, reconectando...'.format(e))
                    try:
                        if sock:
                            sock.close()
                    except Exception:
                        pass
                    _close_and_reset()
                    reconnect_attempts = min(reconnect_attempts + 1, int(RECONNECT_BACKOFF_MAX))
                    last_data_time = time.time()
                    time.sleep(1.0)
        finally:
            try:
                if sock:
                    sock.close()
            except Exception:
                pass
            try:
                self._collector_connected = False
            except Exception:
                pass
            print('[COLLECTOR] Detenido')

    def get_status(self):
        """Devuelve estado para la GUI: clientes conectados y actividad Rx reciente"""
        with self._clients_lock:
            cc = self._client_count
        try:
            last = self._last_rx_time
        except Exception:
            last = 0.0
        now = time.time()
        # LED Rx verde si el recolector está conectado o si hubo actividad reciente
        conn = False
        try:
            conn = bool(self._collector_connected)
        except Exception:
            conn = False
        rx_active = conn or (last > 0 and (now - last) < 10.0)
        # ¿Recolector activo?
        collector_running = False
        try:
            collector_running = (self._collector_thread is not None and self._collector_thread.is_alive())
        except Exception:
            pass
        # Latencia
        latency_ms = self.get_current_latency()
        latency_ok = self.is_latency_ok()
        return {
            'client_count': cc,
            'rx_active': rx_active,
            'collector_running': collector_running,
            'backlog_count': self.store.count(),
            'backlog_bytes': self.store.size_bytes(),
            'latency_ms': latency_ms,
            'latency_ok': latency_ok,
            'delivery_mode': self.delivery_mode,
            'mqtt_connected': self.is_mqtt_connected(),
            'ota_connected': self.is_ota_connected(),
            'version': VERSION,
            'ext_comments': self._ext_comments_poller.get_status() if self._ext_comments_poller else None,
        }


class _ThreadedBridge(object):
    """Envuelve TCPBridge en un hilo para control desde GUI"""
    def __init__(self, listen_ip, listen_port, forward_ip, forward_port, source_ip=None,
                 delivery_mode='tcp', mqtt_broker_ip=None, mqtt_broker_port=1883,
                 mqtt_topic='puente/wits', mqtt_user=None, mqtt_pass=None,
                 status_api_url=None, status_api_interval=60, status_api_key=None,
                 ota_topic='puente/ota',
                 ext_comments_enabled=False, ext_comments_cfg=None):
        self.bridge = TCPBridge(listen_ip, listen_port, forward_ip, forward_port,
                                source_ip=source_ip, delivery_mode=delivery_mode,
                                mqtt_broker_ip=mqtt_broker_ip, mqtt_broker_port=mqtt_broker_port,
                                mqtt_topic=mqtt_topic, mqtt_user=mqtt_user, mqtt_pass=mqtt_pass,
                                status_api_url=status_api_url, status_api_interval=status_api_interval,
                                status_api_key=status_api_key,
                                ota_topic=ota_topic,
                                ext_comments_enabled=ext_comments_enabled,
                                ext_comments_cfg=ext_comments_cfg)
        self.thread = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.bridge.running = True
        self.thread = threading.Thread(target=self.bridge.start)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.bridge:
            self.bridge.stop()
        if self.thread:
            # Esperar un poco a que termine
            for _ in range(20):
                if not self.thread.is_alive():
                    break
                time.sleep(0.1)


class _QueueWriter(object):
    """Redirige prints a una cola para consumo desde GUI"""
    def __init__(self, q, stream_name):
        self.q = q
        self.stream_name = stream_name
        try:
            self.encoding = sys.stdout.encoding
        except Exception:
            self.encoding = 'utf-8'

    def write(self, msg):
        if msg is None:
            return
        try:
            self.q.put((self.stream_name, msg))
        except Exception:
            pass

    def flush(self):
        pass


# ========== Persistencia SQLite del backlog ==========

class DataStore(object):
    """Cola persistente con SQLite para backlog TARGET->CLIENT"""
    def __init__(self, db_path=None):
        if db_path is None:
            db_path = os.path.join(os.path.dirname(__file__), 'tcp_4guard.db')
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.text_factory = bytes  # almacenar bytes tal cual
        self._init_schema()

    def _init_schema(self):
        with self._lock:
            c = self._conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS backlog (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          ts REAL NOT NULL,
                          data BLOB NOT NULL
                        )''')
            c.execute('''CREATE TABLE IF NOT EXISTS poller_state (
                          source_key TEXT PRIMARY KEY,
                          last_ts    TEXT NOT NULL,
                          updated_at TEXT NOT NULL
                        )''')
            c.execute('''CREATE TABLE IF NOT EXISTS comments_backlog (
                          id          INTEGER PRIMARY KEY AUTOINCREMENT,
                          config_id   TEXT NOT NULL,
                          payload     TEXT NOT NULL,
                          created_at  TEXT NOT NULL,
                          attempts    INTEGER DEFAULT 0
                        )''')
            self._conn.commit()

    # ---- helpers poller ----

    def get_last_ts(self, source_key):
        """Retorna el last_ts persistido para una fuente, o None."""
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT last_ts FROM poller_state WHERE source_key = ?', (source_key,))
            row = c.fetchone()
            if row is None:
                return None
            val = row[0]
            if isinstance(val, bytes):
                val = val.decode('utf-8', 'replace')
            return val

    def set_last_ts(self, source_key, ts_iso):
        """Actualiza (upsert) el last_ts de una fuente.
        v1.11.3: usar INSERT OR REPLACE (compat SQLite < 3.24, ej Python 2.7 Windows)."""
        now_iso = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        with self._lock:
            c = self._conn.cursor()
            c.execute('''INSERT OR REPLACE INTO poller_state (source_key, last_ts, updated_at)
                         VALUES (?, ?, ?)''',
                      (source_key, ts_iso, now_iso))
            self._conn.commit()

    def enqueue_comment(self, config_id, payload_str):
        """Guarda un comentario pendiente en comments_backlog."""
        now_iso = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        with self._lock:
            c = self._conn.cursor()
            c.execute('''INSERT INTO comments_backlog (config_id, payload, created_at, attempts)
                         VALUES (?, ?, ?, 0)''',
                      (str(config_id), payload_str, now_iso))
            self._conn.commit()

    def dequeue_comments_batch(self, limit=100):
        """Lee lote de comentarios pendientes del backlog."""
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT id, config_id, payload FROM comments_backlog ORDER BY id ASC LIMIT ?', (int(limit),))
            rows = c.fetchall()
            out = []
            for (row_id, cid, payload) in rows:
                if isinstance(cid, bytes):
                    cid = cid.decode('utf-8', 'replace')
                if isinstance(payload, bytes):
                    payload = payload.decode('utf-8', 'replace')
                out.append((row_id, cid, payload))
            return out

    def delete_comment_ids(self, ids):
        if not ids:
            return
        with self._lock:
            c = self._conn.cursor()
            q_marks = ','.join(['?'] * len(ids))
            c.execute('DELETE FROM comments_backlog WHERE id IN ({})'.format(q_marks), ids)
            self._conn.commit()

    def increment_comment_attempts(self, ids):
        if not ids:
            return
        with self._lock:
            c = self._conn.cursor()
            q_marks = ','.join(['?'] * len(ids))
            c.execute('UPDATE comments_backlog SET attempts = attempts + 1 WHERE id IN ({})'.format(q_marks), ids)
            self._conn.commit()

    def count_comments_backlog(self):
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT COUNT(1) FROM comments_backlog')
            (n,) = c.fetchone()
            return int(n)

    def enqueue(self, data, ts=None):
        # Permite especificar la huella temporal de recepción para precisión por-trama
        ts = time.time() if ts is None else float(ts)
        with self._lock:
            c = self._conn.cursor()
            c.execute('INSERT INTO backlog (ts, data) VALUES (?, ?)', (ts, sqlite3.Binary(data)))
            self._conn.commit()

    def dequeue_batch(self, limit=200):
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT id, ts, data FROM backlog ORDER BY id ASC LIMIT ?', (int(limit),))
            rows = c.fetchall()
            # No eliminamos aquí; se borran tras enviar
            return rows

    def delete_ids(self, ids):
        if not ids:
            return
        with self._lock:
            c = self._conn.cursor()
            q_marks = ','.join(['?'] * len(ids))
            c.execute('DELETE FROM backlog WHERE id IN ({})'.format(q_marks), ids)
            self._conn.commit()

    def count(self):
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT COUNT(1) FROM backlog')
            (n,) = c.fetchone()
            return int(n)

    def size_bytes(self):
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT COALESCE(SUM(LENGTH(data)), 0) FROM backlog')
            (b,) = c.fetchone()
            try:
                return int(b)
            except Exception:
                return 0


# ========== Config y GUI ==========

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'tcp_4guard.ini')

def load_settings():
    """Carga la configuración desde archivo INI si existe"""
    if configparser is None:
        return None
    cfg = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        cfg.read(CONFIG_FILE)
        if not cfg.has_section('bridge'):
            return None
        out = {
            'listen_ip': cfg.get('bridge', 'listen_ip'),
            'listen_port': cfg.get('bridge', 'listen_port'),
            'forward_ip': cfg.get('bridge', 'forward_ip'),
            'forward_port': cfg.get('bridge', 'forward_port')
        }
        try:
            if cfg.has_option('bridge', 'source_ip'):
                out['source_ip'] = cfg.get('bridge', 'source_ip')
        except Exception:
            pass
        # MQTT + Status API settings
        for key in ('delivery_mode', 'mqtt_broker_ip', 'mqtt_broker_port',
                     'mqtt_topic', 'mqtt_user', 'mqtt_pass',
                     'status_api_url', 'status_api_interval', 'status_api_key',
                     'ota_topic', 'auto_start'):
            try:
                if cfg.has_option('bridge', key):
                    out[key] = cfg.get('bridge', key)
            except Exception:
                pass
        # External comments (seccion aparte)
        if cfg.has_section('external_comments'):
            for key in ('ec_enabled', 'ec_host', 'ec_port', 'ec_user', 'ec_pass',
                        'ec_authdb', 'ec_db', 'ec_col', 'ec_config_id',
                        'ec_field_ts', 'ec_field_text', 'ec_field_author',
                        'ec_interval', 'ec_backfill'):
                try:
                    if cfg.has_option('external_comments', key):
                        out[key] = cfg.get('external_comments', key)
                except Exception:
                    pass
        return out
    except Exception:
        return None

def save_settings(listen_ip, listen_port, forward_ip, forward_port, source_ip=None,
                  delivery_mode='tcp', mqtt_broker_ip=None, mqtt_broker_port=1883,
                  mqtt_topic='puente/wits', mqtt_user=None, mqtt_pass=None,
                  status_api_url=None, status_api_interval=60, status_api_key=None,
                  ota_topic='puente/ota',
                  ec_enabled=False, ec_cfg=None,
                  auto_start=True):
    """Guarda la configuración en un archivo INI"""
    if configparser is None:
        return
    cfg = configparser.ConfigParser()
    cfg.add_section('bridge')
    cfg.set('bridge', 'listen_ip', str(listen_ip))
    cfg.set('bridge', 'listen_port', str(listen_port))
    cfg.set('bridge', 'forward_ip', str(forward_ip))
    cfg.set('bridge', 'forward_port', str(forward_port))
    try:
        if source_ip:
            cfg.set('bridge', 'source_ip', str(source_ip))
    except Exception:
        pass
    # MQTT settings
    cfg.set('bridge', 'delivery_mode', str(delivery_mode or 'tcp'))
    if mqtt_broker_ip:
        cfg.set('bridge', 'mqtt_broker_ip', str(mqtt_broker_ip))
    cfg.set('bridge', 'mqtt_broker_port', str(mqtt_broker_port or 1883))
    cfg.set('bridge', 'mqtt_topic', str(mqtt_topic or 'puente/wits'))
    if mqtt_user:
        cfg.set('bridge', 'mqtt_user', str(mqtt_user))
    if mqtt_pass:
        cfg.set('bridge', 'mqtt_pass', str(mqtt_pass))
    # Status API settings
    if status_api_url:
        cfg.set('bridge', 'status_api_url', str(status_api_url))
    cfg.set('bridge', 'status_api_interval', str(status_api_interval or 60))
    if status_api_key:
        cfg.set('bridge', 'status_api_key', str(status_api_key))
    # OTA
    cfg.set('bridge', 'ota_topic', str(ota_topic or 'puente/ota'))
    # Auto start
    cfg.set('bridge', 'auto_start', '1' if auto_start else '0')
    # External comments
    if ec_cfg is None:
        ec_cfg = {}
    cfg.add_section('external_comments')
    cfg.set('external_comments', 'ec_enabled', '1' if ec_enabled else '0')
    if ec_cfg.get('host'):
        cfg.set('external_comments', 'ec_host', str(ec_cfg.get('host')))
    if ec_cfg.get('port'):
        cfg.set('external_comments', 'ec_port', str(ec_cfg.get('port')))
    if ec_cfg.get('user'):
        cfg.set('external_comments', 'ec_user', str(ec_cfg.get('user')))
    if ec_cfg.get('password'):
        cfg.set('external_comments', 'ec_pass', str(ec_cfg.get('password')))
    if ec_cfg.get('authdb'):
        cfg.set('external_comments', 'ec_authdb', str(ec_cfg.get('authdb')))
    if ec_cfg.get('db'):
        cfg.set('external_comments', 'ec_db', str(ec_cfg.get('db')))
    if ec_cfg.get('collection'):
        cfg.set('external_comments', 'ec_col', str(ec_cfg.get('collection')))
    if ec_cfg.get('config_id'):
        cfg.set('external_comments', 'ec_config_id', str(ec_cfg.get('config_id')))
    if ec_cfg.get('field_ts'):
        cfg.set('external_comments', 'ec_field_ts', str(ec_cfg.get('field_ts')))
    if ec_cfg.get('field_text'):
        cfg.set('external_comments', 'ec_field_text', str(ec_cfg.get('field_text')))
    if ec_cfg.get('field_author'):
        cfg.set('external_comments', 'ec_field_author', str(ec_cfg.get('field_author')))
    if ec_cfg.get('interval'):
        cfg.set('external_comments', 'ec_interval', str(ec_cfg.get('interval')))
    if ec_cfg.get('backfill_hours'):
        cfg.set('external_comments', 'ec_backfill', str(ec_cfg.get('backfill_hours')))
    try:
        with open(CONFIG_FILE, 'w') as f:
            cfg.write(f)
    except Exception:
        pass


class BridgeGUI(object):
    def __init__(self, root):
        self.root = root
        self.root.title('TCP Bridge (Python 2.7)')

        # Estados
        self.worker = None
        self.log_queue = Queue() if Queue else None
        # Acceso a métricas de backlog
        self._store_gui = DataStore()

        # Variables UI (con valores por defecto)
        self.listen_ip_var = tk.StringVar(value='0.0.0.0')
        self.listen_port_var = tk.StringVar(value='2724')
        self.forward_ip_var = tk.StringVar(value='167.175.223.23')
        self.forward_port_var = tk.StringVar(value='2725')
        # IP local de salida (opcional)
        self.source_ip_var = tk.StringVar(value='')
        # MQTT variables
        self.delivery_mode_var = tk.StringVar(value='tcp')
        self.mqtt_broker_ip_var = tk.StringVar(value='')
        self.mqtt_broker_port_var = tk.StringVar(value='1883')
        self.mqtt_topic_var = tk.StringVar(value='puente/wits')
        self.mqtt_user_var = tk.StringVar(value='')
        self.mqtt_pass_var = tk.StringVar(value='')
        # Status API variables
        self.status_api_url_var = tk.StringVar(value='')
        self.status_api_interval_var = tk.StringVar(value='60')
        self.status_api_key_var = tk.StringVar(value='')
        # OTA variable
        self.ota_topic_var = tk.StringVar(value='puente/ota')
        # Auto-iniciar bridge al abrir la app
        self.auto_start_var = tk.BooleanVar(value=True)
        # External comments variables
        # v1.11.0: force-on — el feature queda encendido obligatoriamente.
        self.ec_enabled_var = tk.BooleanVar(value=True)
        # v1.11.1: force-on — host Mongo siempre 192.168.0.10 (override .ini).
        self.ec_host_var = tk.StringVar(value='192.168.0.10')
        self.ec_port_var = tk.StringVar(value='27017')
        self.ec_user_var = tk.StringVar(value='dwcore')
        self.ec_pass_var = tk.StringVar(value='qwerty')
        self.ec_authdb_var = tk.StringVar(value='admin')
        self.ec_db_var = tk.StringVar(value='')
        self.ec_col_var = tk.StringVar(value='timedata')
        # v1.11.4: force-on — campo timestamp Mongo siempre 'createdAt' (default Mongoose).
        self.ec_field_ts_var = tk.StringVar(value='createdAt')
        self.ec_field_text_var = tk.StringVar(value='text')
        self.ec_field_author_var = tk.StringVar(value='author')
        self.ec_interval_var = tk.StringVar(value='5')
        self.ec_backfill_var = tk.StringVar(value='1')

        # Cargar configuración persistente si existe
        try:
            loaded = load_settings()
            if loaded:
                if loaded.get('listen_ip'):
                    self.listen_ip_var.set(loaded.get('listen_ip'))
                if loaded.get('listen_port'):
                    self.listen_port_var.set(loaded.get('listen_port'))
                if loaded.get('forward_ip'):
                    self.forward_ip_var.set(loaded.get('forward_ip'))
                if loaded.get('forward_port'):
                    self.forward_port_var.set(loaded.get('forward_port'))
                if loaded.get('source_ip'):
                    self.source_ip_var.set(loaded.get('source_ip'))
                if loaded.get('delivery_mode'):
                    self.delivery_mode_var.set(loaded.get('delivery_mode'))
                if loaded.get('mqtt_broker_ip'):
                    self.mqtt_broker_ip_var.set(loaded.get('mqtt_broker_ip'))
                if loaded.get('mqtt_broker_port'):
                    self.mqtt_broker_port_var.set(loaded.get('mqtt_broker_port'))
                if loaded.get('mqtt_topic'):
                    self.mqtt_topic_var.set(loaded.get('mqtt_topic'))
                if loaded.get('mqtt_user'):
                    self.mqtt_user_var.set(loaded.get('mqtt_user'))
                if loaded.get('mqtt_pass'):
                    self.mqtt_pass_var.set(loaded.get('mqtt_pass'))
                if loaded.get('status_api_url'):
                    self.status_api_url_var.set(loaded.get('status_api_url'))
                if loaded.get('status_api_interval'):
                    self.status_api_interval_var.set(loaded.get('status_api_interval'))
                if loaded.get('status_api_key'):
                    self.status_api_key_var.set(loaded.get('status_api_key'))
                if loaded.get('ota_topic'):
                    self.ota_topic_var.set(loaded.get('ota_topic'))
                if loaded.get('auto_start') is not None:
                    try:
                        val = loaded.get('auto_start')
                        self.auto_start_var.set(str(val).lower() in ('1', 'true', 'yes', 'on'))
                    except Exception:
                        pass
                # External comments
                # v1.11.0: ignorar ec_enabled del .ini — el feature va forzado a True.
                # v1.11.1: ignorar ec_host del .ini — host forzado a 192.168.0.10.
                # v1.11.4: ignorar ec_field_ts del .ini — campo forzado a 'createdAt'.
                for key, var in (('ec_port', self.ec_port_var),
                                 ('ec_user', self.ec_user_var), ('ec_pass', self.ec_pass_var),
                                 ('ec_authdb', self.ec_authdb_var), ('ec_db', self.ec_db_var),
                                 ('ec_col', self.ec_col_var),
                                 ('ec_field_text', self.ec_field_text_var),
                                 ('ec_field_author', self.ec_field_author_var),
                                 ('ec_interval', self.ec_interval_var),
                                 ('ec_backfill', self.ec_backfill_var)):
                    if loaded.get(key) is not None:
                        try:
                            var.set(loaded.get(key))
                        except Exception:
                            pass
        except Exception:
            pass

        # Layout
        self._build_form()
        self._build_log()
        self._build_buttons()
        self._build_status()

        # Redirección de stdout/stderr
        if self.log_queue is not None:
            self._orig_stdout = sys.stdout
            self._orig_stderr = sys.stderr
            sys.stdout = _QueueWriter(self.log_queue, 'STDOUT')
            sys.stderr = _QueueWriter(self.log_queue, 'STDERR')

        # Iniciar loop de refresco de logs y métricas
        self._poll_logs()
        self._poll_stats()
        # Actualizar estado de autostart al arrancar
        try:
            self.root.after(500, self._refresh_autostart_status)
        except Exception:
            pass
        # Auto-instalar pymongo en background si falta (para no bloquear arranque)
        try:
            self.root.after(300, self._auto_install_pymongo_if_missing)
        except Exception:
            pass
        # Auto-iniciar el bridge si esta habilitado en la config
        try:
            if self.auto_start_var.get():
                self.root.after(1000, self._auto_start_if_needed)
        except Exception:
            pass

        # Manejar cierre de ventana
        # Deshabilitar boton de cerrar ventana (X)
        self.root.protocol('WM_DELETE_WINDOW', self._ignore_close)
        # Deshabilitar visualmente el boton X despues de que la ventana se renderice
        self.root.after(100, self._disable_close_button)

        # Atajo oculto Ctrl+Shift+Q para cerrar
        self.root.bind('<Control-Shift-Q>', lambda e: self.on_close())
        self.root.bind('<Control-Shift-q>', lambda e: self.on_close())

    def _build_form(self):
        # Notebook con 3 pestanas: Principal / OTA / Comentarios Externos
        self._notebook = ttk.Notebook(self.root)
        self._notebook.pack(fill='x', padx=8, pady=8)

        # === TAB 1: Principal ===
        tab_main = tk.Frame(self._notebook, padx=8, pady=8)
        self._notebook.add(tab_main, text='Principal')

        # --- Campos TCP (escucha de clientes) - se ocultan en modo MQTT ---
        self._lbl_listen_ip = tk.Label(tab_main, text='Escuchar IP:')
        self._lbl_listen_ip.grid(row=0, column=0, sticky='e')
        self._listen_ip_entry = tk.Entry(tab_main, textvariable=self.listen_ip_var, width=18)
        self._listen_ip_entry.grid(row=0, column=1, padx=4)
        self._lbl_listen_port = tk.Label(tab_main, text='Puerto:')
        self._lbl_listen_port.grid(row=0, column=2, sticky='e')
        self._listen_port_entry = tk.Entry(tab_main, textvariable=self.listen_port_var, width=8)
        self._listen_port_entry.grid(row=0, column=3, padx=4)

        # --- Fuente WITS (siempre requerida) ---
        tk.Label(tab_main, text='Fuente WITS IP:').grid(row=1, column=0, sticky='e')
        tk.Entry(tab_main, textvariable=self.forward_ip_var, width=18).grid(row=1, column=1, padx=4)
        tk.Label(tab_main, text='Puerto:').grid(row=1, column=2, sticky='e')
        tk.Entry(tab_main, textvariable=self.forward_port_var, width=8).grid(row=1, column=3, padx=4)

        # Fila para IP local de salida (opcional) - se oculta en modo MQTT
        self._lbl_source_ip = tk.Label(tab_main, text='IP local (salida):')
        self._lbl_source_ip.grid(row=2, column=0, sticky='e')
        self._source_ip_entry = tk.Entry(tab_main, textvariable=self.source_ip_var, width=18)
        self._source_ip_entry.grid(row=2, column=1, padx=4)
        self._lbl_source_opt = tk.Label(tab_main, text='(opcional)')
        self._lbl_source_opt.grid(row=2, column=2, sticky='w')

        # Separador visual
        ttk.Separator(tab_main, orient='horizontal').grid(row=3, column=0, columnspan=4, sticky='ew', pady=6)

        # Modo de entrega
        tk.Label(tab_main, text='Entrega:').grid(row=4, column=0, sticky='e')
        mode_frm = tk.Frame(tab_main)
        mode_frm.grid(row=4, column=1, columnspan=3, sticky='w', padx=4)
        tk.Radiobutton(mode_frm, text='TCP', variable=self.delivery_mode_var,
                       value='tcp', command=self._on_mode_change).pack(side='left')
        tk.Radiobutton(mode_frm, text='MQTT', variable=self.delivery_mode_var,
                       value='mqtt', command=self._on_mode_change).pack(side='left', padx=(8, 0))

        # Campos MQTT
        tk.Label(tab_main, text='MQTT Broker IP:').grid(row=5, column=0, sticky='e')
        self._mqtt_broker_entry = tk.Entry(tab_main, textvariable=self.mqtt_broker_ip_var, width=18)
        self._mqtt_broker_entry.grid(row=5, column=1, padx=4)
        tk.Label(tab_main, text='Puerto:').grid(row=5, column=2, sticky='e')
        self._mqtt_port_entry = tk.Entry(tab_main, textvariable=self.mqtt_broker_port_var, width=8)
        self._mqtt_port_entry.grid(row=5, column=3, padx=4)

        tk.Label(tab_main, text='MQTT Topic:').grid(row=6, column=0, sticky='e')
        self._mqtt_topic_entry = tk.Entry(tab_main, textvariable=self.mqtt_topic_var, width=18)
        self._mqtt_topic_entry.grid(row=6, column=1, padx=4)

        tk.Label(tab_main, text='MQTT Usuario:').grid(row=7, column=0, sticky='e')
        self._mqtt_user_entry = tk.Entry(tab_main, textvariable=self.mqtt_user_var, width=18)
        self._mqtt_user_entry.grid(row=7, column=1, padx=4)
        tk.Label(tab_main, text='Password:').grid(row=7, column=2, sticky='e')
        self._mqtt_pass_entry = tk.Entry(tab_main, textvariable=self.mqtt_pass_var, width=18, show='*')
        self._mqtt_pass_entry.grid(row=7, column=3, padx=4)

        self._mqtt_widgets = [self._mqtt_broker_entry, self._mqtt_port_entry,
                              self._mqtt_topic_entry, self._mqtt_user_entry,
                              self._mqtt_pass_entry]

        self._tcp_only_widgets = [self._lbl_listen_ip, self._listen_ip_entry,
                                  self._lbl_listen_port, self._listen_port_entry,
                                  self._lbl_source_ip, self._source_ip_entry,
                                  self._lbl_source_opt]

        # Separador antes de Status API
        ttk.Separator(tab_main, orient='horizontal').grid(row=8, column=0, columnspan=4, sticky='ew', pady=6)

        tk.Label(tab_main, text='Status API URL:').grid(row=9, column=0, sticky='e')
        tk.Entry(tab_main, textvariable=self.status_api_url_var, width=40).grid(row=9, column=1, columnspan=3, sticky='ew', padx=4)

        tk.Label(tab_main, text='Intervalo (s):').grid(row=10, column=0, sticky='e')
        tk.Entry(tab_main, textvariable=self.status_api_interval_var, width=8).grid(row=10, column=1, padx=4, sticky='w')
        tk.Label(tab_main, text='API Key:').grid(row=10, column=2, sticky='e')
        tk.Entry(tab_main, textvariable=self.status_api_key_var, width=18, show='*').grid(row=10, column=3, padx=4)

        # Separador + seccion Autostart Windows
        ttk.Separator(tab_main, orient='horizontal').grid(row=11, column=0, columnspan=4, sticky='ew', pady=6)
        tk.Label(tab_main, text='Inicio automatico (Windows):', font=('Arial', 9, 'bold')).grid(
            row=12, column=0, columnspan=2, sticky='w', padx=4)
        self._autostart_status_lbl = tk.Label(tab_main, text='Verificando...', fg='gray40')
        self._autostart_status_lbl.grid(row=12, column=2, columnspan=2, sticky='w', padx=4)
        self._autostart_install_btn = tk.Button(tab_main, text='Instalar/Reparar',
                                                 command=self._on_autostart_install)
        self._autostart_install_btn.grid(row=13, column=0, padx=4, pady=4, sticky='w')
        self._autostart_uninstall_btn = tk.Button(tab_main, text='Desinstalar',
                                                   command=self._on_autostart_uninstall)
        self._autostart_uninstall_btn.grid(row=13, column=1, padx=4, pady=4, sticky='w')
        self._autostart_refresh_btn = tk.Button(tab_main, text='Actualizar estado',
                                                 command=self._refresh_autostart_status)
        self._autostart_refresh_btn.grid(row=13, column=2, padx=4, pady=4, sticky='w')

        for i in range(4):
            tab_main.grid_columnconfigure(i, weight=1)

        # === TAB 2: OTA ===
        tab_ota = tk.Frame(self._notebook, padx=8, pady=8)
        self._notebook.add(tab_ota, text='OTA')

        tk.Label(tab_ota, text='Version actual:', font=('Arial', 10, 'bold')).grid(row=0, column=0, sticky='e')
        tk.Label(tab_ota, text='v{}'.format(VERSION)).grid(row=0, column=1, sticky='w', padx=4)

        tk.Label(tab_ota, text='OTA Topic:').grid(row=1, column=0, sticky='e', pady=(8, 0))
        self._ota_topic_entry = tk.Entry(tab_ota, textvariable=self.ota_topic_var, width=25)
        self._ota_topic_entry.grid(row=1, column=1, padx=4, pady=(8, 0), sticky='w')
        tk.Label(tab_ota, text='(usa broker MQTT configurado en Principal)').grid(row=1, column=2, columnspan=2, sticky='w')

        ttk.Separator(tab_ota, orient='horizontal').grid(row=2, column=0, columnspan=4, sticky='ew', pady=10)

        info_text = (
            'Flujo OTA:\n'
            '1. Recibe comando JSON en el topic configurado\n'
            '2. Descarga script desde la URL indicada\n'
            '3. Verifica SHA256\n'
            '4. Valida sintaxis Python\n'
            '5. Crea backup (.bak) y reemplaza\n'
            '6. Reinicia proceso automaticamente\n\n'
            'Ejemplo de payload:\n'
            '  {"version": "1.1.0",\n'
            '   "url": "http://servidor/tcp_4guard.py",\n'
            '   "sha256": "abc123..."}'
        )
        tk.Label(tab_ota, text=info_text, justify='left', fg='gray30').grid(
            row=3, column=0, columnspan=4, sticky='w', padx=4, pady=4)

        for i in range(4):
            tab_ota.grid_columnconfigure(i, weight=1)

        # === TAB 3: Comentarios Externos ===
        tab_ec = tk.Frame(self._notebook, padx=8, pady=8)
        self._notebook.add(tab_ec, text='Comentarios Externos')

        self._ec_checkbox = tk.Checkbutton(tab_ec, text='Habilitar Comentarios Externos (Mongo -> MQTT)',
                                           variable=self.ec_enabled_var,
                                           command=self._on_ec_toggle)
        self._ec_checkbox.grid(row=0, column=0, columnspan=4, sticky='w', padx=4)

        ttk.Separator(tab_ec, orient='horizontal').grid(row=1, column=0, columnspan=4, sticky='ew', pady=6)

        tk.Label(tab_ec, text='Mongo Host:').grid(row=2, column=0, sticky='e')
        self._ec_host_entry = tk.Entry(tab_ec, textvariable=self.ec_host_var, width=20)
        self._ec_host_entry.grid(row=2, column=1, padx=4)
        tk.Label(tab_ec, text='Puerto:').grid(row=2, column=2, sticky='e')
        self._ec_port_entry = tk.Entry(tab_ec, textvariable=self.ec_port_var, width=8)
        self._ec_port_entry.grid(row=2, column=3, padx=4)

        tk.Label(tab_ec, text='Mongo Usuario:').grid(row=3, column=0, sticky='e')
        self._ec_user_entry = tk.Entry(tab_ec, textvariable=self.ec_user_var, width=20, show='*')
        self._ec_user_entry.grid(row=3, column=1, padx=4)
        tk.Label(tab_ec, text='Password:').grid(row=3, column=2, sticky='e')
        self._ec_pass_entry = tk.Entry(tab_ec, textvariable=self.ec_pass_var, width=20, show='*')
        self._ec_pass_entry.grid(row=3, column=3, padx=4)

        tk.Label(tab_ec, text='Auth DB:').grid(row=4, column=0, sticky='e')
        self._ec_authdb_entry = tk.Entry(tab_ec, textvariable=self.ec_authdb_var, width=20)
        self._ec_authdb_entry.grid(row=4, column=1, padx=4)
        tk.Label(tab_ec, text='Database:').grid(row=4, column=2, sticky='e')
        self._ec_db_detected_lbl = tk.Label(tab_ec, text='(auto-detectado del WITS item 0101)',
                                             fg='gray50', anchor='w')
        self._ec_db_detected_lbl.grid(row=4, column=3, padx=4, sticky='w')

        tk.Label(tab_ec, text='Coleccion:').grid(row=5, column=0, sticky='e')
        self._ec_col_entry = tk.Entry(tab_ec, textvariable=self.ec_col_var, width=20)
        self._ec_col_entry.grid(row=5, column=1, padx=4)
        tk.Label(tab_ec, text='(comentarios se inyectan como WITS SPARE500)',
                 fg='gray40').grid(row=5, column=2, columnspan=2, sticky='w', padx=4)

        ttk.Separator(tab_ec, orient='horizontal').grid(row=6, column=0, columnspan=4, sticky='ew', pady=6)

        tk.Label(tab_ec, text='Campo ts:').grid(row=7, column=0, sticky='e')
        self._ec_field_ts_entry = tk.Entry(tab_ec, textvariable=self.ec_field_ts_var, width=12)
        self._ec_field_ts_entry.grid(row=7, column=1, padx=4, sticky='w')
        tk.Label(tab_ec, text='Campo text:').grid(row=7, column=2, sticky='e')
        self._ec_field_text_entry = tk.Entry(tab_ec, textvariable=self.ec_field_text_var, width=12)
        self._ec_field_text_entry.grid(row=7, column=3, padx=4, sticky='w')

        tk.Label(tab_ec, text='Campo author:').grid(row=8, column=0, sticky='e')
        self._ec_field_author_entry = tk.Entry(tab_ec, textvariable=self.ec_field_author_var, width=12)
        self._ec_field_author_entry.grid(row=8, column=1, padx=4, sticky='w')
        tk.Label(tab_ec, text='Intervalo (s):').grid(row=8, column=2, sticky='e')
        self._ec_interval_entry = tk.Entry(tab_ec, textvariable=self.ec_interval_var, width=6)
        self._ec_interval_entry.grid(row=8, column=3, padx=4, sticky='w')

        tk.Label(tab_ec, text='Backfill (h):').grid(row=9, column=0, sticky='e')
        self._ec_backfill_entry = tk.Entry(tab_ec, textvariable=self.ec_backfill_var, width=6)
        self._ec_backfill_entry.grid(row=9, column=1, padx=4, sticky='w')
        self._ec_test_btn = tk.Button(tab_ec, text='Probar conexion Mongo',
                                       command=self._on_ec_test)
        self._ec_test_btn.grid(row=9, column=2, padx=4, pady=(8, 0), sticky='w')
        self._ec_install_btn = tk.Button(tab_ec, text='Instalar pymongo',
                                          command=self._on_ec_install_pymongo)
        self._ec_install_btn.grid(row=9, column=3, padx=4, pady=(8, 0), sticky='w')
        self._ec_send_test_btn = tk.Button(tab_ec, text='Enviar comentario de prueba',
                                            command=self._on_send_test_comment)
        self._ec_send_test_btn.grid(row=10, column=0, columnspan=2, padx=4, pady=(4, 0), sticky='w')

        self._ec_widgets = [self._ec_host_entry, self._ec_port_entry,
                            self._ec_user_entry, self._ec_pass_entry,
                            self._ec_authdb_entry,
                            self._ec_col_entry,
                            self._ec_field_ts_entry, self._ec_field_text_entry,
                            self._ec_field_author_entry, self._ec_interval_entry,
                            self._ec_backfill_entry, self._ec_test_btn]

        for i in range(4):
            tab_ec.grid_columnconfigure(i, weight=1)

        # Aplicar estado inicial de los campos MQTT y EC
        self._on_mode_change()
        self._on_ec_toggle()

    def _build_log(self):
        frm = tk.Frame(self.root, padx=8, pady=4)
        frm.pack(fill='both', expand=True)
        self.txt = tk.Text(frm, height=18, wrap='word')
        self.txt.pack(fill='both', expand=True)
        self.txt.configure(state='disabled')

    def _build_buttons(self):
        frm = tk.Frame(self.root, padx=8, pady=8)
        frm.pack(fill='x')
        self.btn_start = tk.Button(frm, text='Iniciar', command=self.on_start)
        self.btn_stop = tk.Button(frm, text='Detener', command=self.on_stop, state='disabled')
        self.btn_start.pack(side='left')
        self.btn_stop.pack(side='left', padx=(8, 0))
        tk.Checkbutton(frm, text='Iniciar automaticamente al abrir',
                       variable=self.auto_start_var).pack(side='left', padx=(20, 0))

    def _build_status(self):
        frm = tk.Frame(self.root, padx=8, pady=4)
        frm.pack(fill='x')
        # LEDs de estado
        self.led_rx = tk.Canvas(frm, width=16, height=16, highlightthickness=0)
        self.led_rx.create_oval(2, 2, 14, 14, fill='red', outline='black', tags=('dot',))
        tk.Label(frm, text='Rx').pack(side='left', padx=(0, 4))
        self.led_rx.pack(side='left')

        self.led_client = tk.Canvas(frm, width=16, height=16, highlightthickness=0)
        self.led_client.create_oval(2, 2, 14, 14, fill='red', outline='black', tags=('dot',))
        tk.Label(frm, text='Cliente').pack(side='left', padx=(12, 4))
        self.led_client.pack(side='left')

        # LED de latencia
        self.led_latency = tk.Canvas(frm, width=16, height=16, highlightthickness=0)
        self.led_latency.create_oval(2, 2, 14, 14, fill='gray', outline='black', tags=('dot',))
        tk.Label(frm, text='Latencia').pack(side='left', padx=(12, 4))
        self.led_latency.pack(side='left')
        self.lbl_latency = tk.Label(frm, text='--ms')
        self.lbl_latency.pack(side='left', padx=(4, 0))

        # LED MQTT
        self.led_mqtt = tk.Canvas(frm, width=16, height=16, highlightthickness=0)
        self.led_mqtt.create_oval(2, 2, 14, 14, fill='gray', outline='black', tags=('dot',))
        self.lbl_mqtt_tag = tk.Label(frm, text='MQTT')
        self.lbl_mqtt_tag.pack(side='left', padx=(12, 4))
        self.led_mqtt.pack(side='left')

        # LED OTA
        self.led_ota = tk.Canvas(frm, width=16, height=16, highlightthickness=0)
        self.led_ota.create_oval(2, 2, 14, 14, fill='gray', outline='black', tags=('dot',))
        self.lbl_ota_tag = tk.Label(frm, text='OTA')
        self.lbl_ota_tag.pack(side='left', padx=(12, 4))
        self.led_ota.pack(side='left')

        # Métricas backlog
        self.lbl_msgs = tk.Label(frm, text='Pendientes: 0 mensajes')
        self.lbl_bytes = tk.Label(frm, text='Bytes: 0')
        self.lbl_msgs.pack(side='left', padx=(16, 0))
        self.lbl_bytes.pack(side='left', padx=(16, 0))

    def _on_mode_change(self):
        """Habilita/deshabilita campos MQTT y TCP-only según el modo seleccionado."""
        is_mqtt = (self.delivery_mode_var.get() == 'mqtt')
        # Campos MQTT: habilitados solo en modo MQTT
        mqtt_state = 'normal' if is_mqtt else 'disabled'
        for w in self._mqtt_widgets:
            try:
                w.configure(state=mqtt_state)
            except Exception:
                pass
        # Campos TCP-only (escucha de clientes): ocultos en modo MQTT
        for w in self._tcp_only_widgets:
            try:
                if is_mqtt:
                    w.grid_remove()
                else:
                    w.grid()
            except Exception:
                pass

    def _auto_install_pymongo_if_missing(self):
        """Si pymongo no esta instalado, lo instala en background al abrir la GUI.
        Tras instalar exitosamente, arranca la prueba de conexion Mongo."""
        if pymongo is not None:
            # Ya esta: si ext_comments esta habilitado, hacer test directamente
            if self.ec_enabled_var.get():
                try:
                    self.root.after(500, self._on_ec_test)
                except Exception:
                    pass
            return
        def _worker():
            try:
                self._append_log('[ExtComments] pymongo no detectado, instalando automaticamente...\n')
            except Exception:
                pass
            ok, msg = ensure_pymongo()
            try:
                self._append_log('[ExtComments] ' + msg + '\n')
            except Exception:
                pass
            if ok:
                # Tras instalacion exitosa, lanzar prueba de conexion Mongo
                try:
                    self._append_log('[ExtComments] Iniciando prueba de conexion Mongo...\n')
                except Exception:
                    pass
                try:
                    # Ejecutar el test en el hilo principal de Tk
                    self.root.after(0, self._on_ec_test)
                except Exception:
                    # Fallback: ejecutar directo (funciona porque _on_ec_test no toca widgets criticos)
                    try:
                        self._on_ec_test()
                    except Exception:
                        pass
        t = threading.Thread(target=_worker)
        t.daemon = True
        t.start()

    def _auto_start_if_needed(self):
        """Inicia el bridge automaticamente si esta habilitado y no esta ya corriendo."""
        if self.worker is None and self.auto_start_var.get():
            self._append_log('[GUI] Auto-iniciando bridge...\n')
            self.on_start()

    def _refresh_autostart_status(self):
        """Actualiza el label de estado del autostart."""
        try:
            st = autostart_check()
            if st.get('error'):
                self._autostart_status_lbl.configure(text=st['error'], fg='gray50')
            elif not st['exists']:
                self._autostart_status_lbl.configure(text='No instalado', fg='red')
            elif not st.get('content_ok', True):
                self._autostart_status_lbl.configure(
                    text='VBS existe pero apunta a otro path', fg='orange')
            else:
                self._autostart_status_lbl.configure(text='Activo (.vbs)', fg='green')
        except Exception as e:
            try:
                self._autostart_status_lbl.configure(text='Error: {}'.format(e), fg='gray50')
            except Exception:
                pass

    def _on_autostart_install(self):
        ok, msg = autostart_install()
        self._append_log('[Autostart] ' + msg + '\n')
        self._refresh_autostart_status()

    def _on_autostart_uninstall(self):
        ok, msg = autostart_uninstall()
        self._append_log('[Autostart] ' + msg + '\n')
        self._refresh_autostart_status()

    def _on_ec_toggle(self):
        """Habilita/deshabilita campos de Comentarios Externos segun checkbox."""
        state = 'normal' if self.ec_enabled_var.get() else 'disabled'
        for w in self._ec_widgets:
            try:
                w.configure(state=state)
            except Exception:
                pass

    def _on_send_test_comment(self):
        """Inyecta un comentario de prueba local (boton en GUI)."""
        if self.worker is None or not hasattr(self.worker, 'bridge'):
            self._append_log('[TEST] Error: el bridge no esta corriendo. Primero inicia.\n')
            return
        text = 'Comentario de prueba desde GUI - {}'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ok = self.worker.bridge.inject_test_comment(text, author='GUI-test')
        if ok:
            self._append_log('[TEST] Comentario inyectado en el backlog\n')
        else:
            self._append_log('[TEST] Fallo al inyectar\n')

    def _on_ec_install_pymongo(self):
        """Instala pymongo en un hilo separado para no bloquear la GUI."""
        def _worker():
            self._append_log('[ExtComments] Instalando pymongo (pip install --user)...\n')
            ok, msg = ensure_pymongo()
            self._append_log('[ExtComments] ' + msg + '\n')
        t = threading.Thread(target=_worker)
        t.daemon = True
        t.start()

    def _on_ec_test(self):
        """Prueba la conexion a Mongo usando los valores actuales del formulario."""
        if pymongo is None:
            self._append_log('[ExtComments] ERROR: pymongo no instalado\n')
            return
        # Obtener well ID detectado del bridge en ejecucion, o del field var
        detected_db = None
        if self.worker is not None and hasattr(self.worker, 'bridge'):
            try:
                detected_db = self.worker.bridge.get_detected_well_id()
            except Exception:
                pass
        db_name = detected_db or self.ec_db_var.get().strip()
        mongo_cfg = {
            'host': self.ec_host_var.get().strip() or 'localhost',
            'port': int(self.ec_port_var.get().strip() or '27017'),
            'user': self.ec_user_var.get().strip() or None,
            'password': self.ec_pass_var.get() or None,
            'authdb': self.ec_authdb_var.get().strip() or 'admin',
            'db': db_name,
            'collection': self.ec_col_var.get().strip(),
        }
        if not mongo_cfg['db']:
            self._append_log('[ExtComments] ERROR: no hay Well ID detectado. Inicie el bridge y espere tramas WITS, o configure --ec-db manualmente\n')
            return
        if not mongo_cfg['collection']:
            self._append_log('[ExtComments] ERROR: completar Coleccion\n')
            return
        poller = ExternalCommentsPoller(mongo_cfg, None, None,
                                         field_ts=self.ec_field_ts_var.get().strip() or 'ts')
        ok, msg = poller.test_connection()
        prefix = '[ExtComments] Test OK (db={}): '.format(db_name) if ok else '[ExtComments] Test FALLO (db={}): '.format(db_name)
        self._append_log(prefix + msg + '\n')

    def _set_led(self, canvas_widget, on):
        color = 'green' if on else 'red'
        try:
            canvas_widget.itemconfigure('dot', fill=color)
        except Exception:
            pass

    def _append_log(self, text):
        self.txt.configure(state='normal')
        self.txt.insert('end', text)
        self.txt.see('end')
        self.txt.configure(state='disabled')

    def _poll_logs(self):
        try:
            if self.log_queue is not None:
                while True:
                    try:
                        _, msg = self.log_queue.get_nowait()
                        self._append_log(msg)
                    except Empty:
                        break
        finally:
            # Repetir cada 100ms
            self.root.after(100, self._poll_logs)

    def _poll_stats(self):
        try:
            n = self._store_gui.count()
            b = self._store_gui.size_bytes()
            self.lbl_msgs.configure(text='Pendientes: {} mensajes'.format(n))
            self.lbl_bytes.configure(text='Bytes: {}'.format(b))
            # LEDs: si hay worker, leer estado del bridge
            if self.worker is not None and hasattr(self.worker, 'bridge'):
                try:
                    # Actualizar label del well ID detectado
                    try:
                        wid = self.worker.bridge.get_detected_well_id()
                        if wid:
                            self._ec_db_detected_lbl.configure(
                                text='Detectado: {}'.format(wid), fg='green')
                        else:
                            self._ec_db_detected_lbl.configure(
                                text='(esperando tramas WITS...)', fg='orange')
                    except Exception:
                        pass
                    st = self.worker.bridge.get_status()
                    self._set_led(self.led_rx, bool(st.get('rx_active')))
                    self._set_led(self.led_client, (st.get('client_count', 0) > 0))
                    # Latencia
                    lat = st.get('latency_ms', -1)
                    lat_ok = st.get('latency_ok', True)
                    if lat < 0:
                        self.lbl_latency.configure(text='--ms')
                        self._set_led(self.led_latency, False)
                    else:
                        self.lbl_latency.configure(text='{:.0f}ms'.format(lat))
                        self._set_led(self.led_latency, lat_ok)
                    # MQTT
                    if st.get('delivery_mode') == 'mqtt':
                        self._set_led(self.led_mqtt, bool(st.get('mqtt_connected')))
                    else:
                        try:
                            self.led_mqtt.itemconfigure('dot', fill='gray')
                        except Exception:
                            pass
                    # OTA
                    if st.get('ota_connected'):
                        self._set_led(self.led_ota, True)
                    else:
                        try:
                            self.led_ota.itemconfigure('dot', fill='gray')
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                # Sin worker: todos en rojo
                self._set_led(self.led_rx, False)
                self._set_led(self.led_client, False)
                self._set_led(self.led_latency, False)
                self.lbl_latency.configure(text='--ms')
                try:
                    self.led_mqtt.itemconfigure('dot', fill='gray')
                except Exception:
                    pass
                try:
                    self.led_ota.itemconfigure('dot', fill='gray')
                except Exception:
                    pass
                try:
                    self._ec_db_detected_lbl.configure(
                        text='(auto-detectado del WITS item 0101)', fg='gray50')
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            # Actualizar cada 1s
            self.root.after(1000, self._poll_stats)

    def on_start(self):
        if self.worker is not None:
            return
        delivery_mode = self.delivery_mode_var.get().strip() or 'tcp'
        try:
            forward_ip = self.forward_ip_var.get().strip()
            forward_port = int(self.forward_port_var.get().strip())
            source_ip = self.source_ip_var.get().strip() or None
            if delivery_mode == 'mqtt':
                listen_ip = '0.0.0.0'
                listen_port = 0
            else:
                listen_ip = self.listen_ip_var.get().strip()
                listen_port = int(self.listen_port_var.get().strip())
        except Exception:
            self._append_log('[GUI] Error: puertos deben ser números\n')
            return

        mqtt_broker_ip = self.mqtt_broker_ip_var.get().strip() or None
        mqtt_broker_port = 1883
        try:
            mqtt_broker_port = int(self.mqtt_broker_port_var.get().strip())
        except Exception:
            pass
        mqtt_topic = self.mqtt_topic_var.get().strip() or 'puente/wits'
        mqtt_user = self.mqtt_user_var.get().strip() or None
        mqtt_pass = self.mqtt_pass_var.get().strip() or None

        status_api_url = self.status_api_url_var.get().strip() or None
        status_api_interval = 60
        try:
            status_api_interval = int(self.status_api_interval_var.get().strip())
        except Exception:
            pass
        status_api_key = self.status_api_key_var.get().strip() or None

        if delivery_mode == 'mqtt' and not mqtt_broker_ip:
            self._append_log('[GUI] Error: debe configurar la IP del broker MQTT\n')
            return

        ota_topic = self.ota_topic_var.get().strip() or 'puente/ota'

        # External comments
        # v1.11.0: force-on — siempre True aunque el usuario desmarque el check.
        ec_enabled = True
        if not self.ec_enabled_var.get():
            self.ec_enabled_var.set(True)
            self._append_log('[v1.11.0] Comentarios externos force-on (override)\n')
        # v1.11.1: force host Mongo a 192.168.0.10 aunque el usuario edite el campo.
        EC_FORCED_HOST = '192.168.0.10'
        if self.ec_host_var.get().strip() != EC_FORCED_HOST:
            self.ec_host_var.set(EC_FORCED_HOST)
            self._append_log('[v1.11.1] Mongo host forzado a {} (override)\n'.format(EC_FORCED_HOST))
        # v1.11.4: force campo ts a 'createdAt' (default Mongoose).
        EC_FORCED_FIELD_TS = 'createdAt'
        if self.ec_field_ts_var.get().strip() != EC_FORCED_FIELD_TS:
            self.ec_field_ts_var.set(EC_FORCED_FIELD_TS)
            self._append_log('[v1.11.4] Campo ts Mongo forzado a {} (override)\n'.format(EC_FORCED_FIELD_TS))
        ec_cfg = {
            'host': EC_FORCED_HOST,
            'port': int(self.ec_port_var.get().strip() or '27017'),
            'user': self.ec_user_var.get().strip() or None,
            'password': self.ec_pass_var.get() or None,
            'authdb': self.ec_authdb_var.get().strip() or 'admin',
            'db': self.ec_db_var.get().strip(),
            'collection': self.ec_col_var.get().strip(),
            'field_ts': EC_FORCED_FIELD_TS,
            'field_text': self.ec_field_text_var.get().strip() or 'text',
            'field_author': self.ec_field_author_var.get().strip() or 'author',
            'interval': float(self.ec_interval_var.get().strip() or '5'),
            'backfill_hours': int(self.ec_backfill_var.get().strip() or '1'),
        }
        if ec_enabled and not ec_cfg['collection']:
            self._append_log('[GUI] Error: Comentarios Externos requiere Coleccion\n')
            return

        # Guardar configuración al iniciar
        save_settings(listen_ip, listen_port, forward_ip, forward_port, source_ip,
                      delivery_mode, mqtt_broker_ip, mqtt_broker_port,
                      mqtt_topic, mqtt_user, mqtt_pass,
                      status_api_url, status_api_interval, status_api_key,
                      ota_topic=ota_topic,
                      ec_enabled=ec_enabled, ec_cfg=ec_cfg,
                      auto_start=bool(self.auto_start_var.get()))

        self.worker = _ThreadedBridge(listen_ip, listen_port, forward_ip, forward_port,
                                      source_ip=source_ip, delivery_mode=delivery_mode,
                                      mqtt_broker_ip=mqtt_broker_ip, mqtt_broker_port=mqtt_broker_port,
                                      mqtt_topic=mqtt_topic, mqtt_user=mqtt_user, mqtt_pass=mqtt_pass,
                                      status_api_url=status_api_url, status_api_interval=status_api_interval,
                                      status_api_key=status_api_key,
                                      ota_topic=ota_topic,
                                      ext_comments_enabled=ec_enabled,
                                      ext_comments_cfg=ec_cfg)
        print('=' * 60)
        print('Iniciando puente desde GUI...')
        self.worker.start()
        self.btn_start.configure(state='disabled')
        self.btn_stop.configure(state='normal')

    def on_stop(self):
        if self.worker is None:
            return
        print('[GUI] Solicitando detener puente...')
        self.worker.stop()
        self.worker = None
        print('[GUI] Puente detenido')
        self.btn_start.configure(state='normal')
        self.btn_stop.configure(state='disabled')

    def _ignore_close(self):
        """Ignora el intento de cerrar la ventana con la X."""
        pass

    def _disable_close_button(self):
        """Deshabilita visualmente el boton X en Windows."""
        try:
            import ctypes
            import ctypes.wintypes
            GWL_STYLE = -16
            WS_SYSMENU = 0x00080000
            SWP_FRAMECHANGED = 0x0020
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            hwnd = ctypes.windll.user32.GetParent(self.root.winfo_id())
            style = ctypes.windll.user32.GetWindowLongW(hwnd, GWL_STYLE)
            style = style & ~WS_SYSMENU
            ctypes.windll.user32.SetWindowLongW(hwnd, GWL_STYLE, style)
            # Forzar redibujado del frame de la ventana
            ctypes.windll.user32.SetWindowPos(
                hwnd, 0, 0, 0, 0, 0,
                SWP_FRAMECHANGED | SWP_NOMOVE | SWP_NOSIZE)
        except Exception:
            pass

    def on_close(self):
        try:
            # Guardar última configuración visible
            try:
                mqtt_port = 1883
                try:
                    mqtt_port = int(self.mqtt_broker_port_var.get().strip())
                except Exception:
                    pass
                status_interval = 60
                try:
                    status_interval = int(self.status_api_interval_var.get().strip())
                except Exception:
                    pass
                ec_cfg_close = {
                    'host': self.ec_host_var.get().strip() or 'localhost',
                    'port': int(self.ec_port_var.get().strip() or '27017'),
                    'user': self.ec_user_var.get().strip() or None,
                    'password': self.ec_pass_var.get() or None,
                    'authdb': self.ec_authdb_var.get().strip() or 'admin',
                    'db': self.ec_db_var.get().strip(),
                    'collection': self.ec_col_var.get().strip(),
                    'field_ts': self.ec_field_ts_var.get().strip() or 'ts',
                    'field_text': self.ec_field_text_var.get().strip() or 'text',
                    'field_author': self.ec_field_author_var.get().strip() or 'author',
                    'interval': self.ec_interval_var.get().strip() or '5',
                    'backfill_hours': self.ec_backfill_var.get().strip() or '1',
                }
                save_settings(self.listen_ip_var.get().strip(),
                              self.listen_port_var.get().strip(),
                              self.forward_ip_var.get().strip(),
                              self.forward_port_var.get().strip(),
                              self.source_ip_var.get().strip() or None,
                              self.delivery_mode_var.get().strip() or 'tcp',
                              self.mqtt_broker_ip_var.get().strip() or None,
                              mqtt_port,
                              self.mqtt_topic_var.get().strip() or 'puente/wits',
                              self.mqtt_user_var.get().strip() or None,
                              self.mqtt_pass_var.get().strip() or None,
                              self.status_api_url_var.get().strip() or None,
                              status_interval,
                              self.status_api_key_var.get().strip() or None,
                              ota_topic=self.ota_topic_var.get().strip() or 'puente/ota',
                              ec_enabled=bool(self.ec_enabled_var.get()),
                              ec_cfg=ec_cfg_close,
                              auto_start=bool(self.auto_start_var.get()))
            except Exception:
                pass
            if self.worker is not None:
                self.worker.stop()
        finally:
            # Restaurar stdout/stderr
            try:
                if hasattr(self, '_orig_stdout') and self._orig_stdout:
                    sys.stdout = self._orig_stdout
                if hasattr(self, '_orig_stderr') and self._orig_stderr:
                    sys.stderr = self._orig_stderr
            except Exception:
                pass
            self.root.destroy()


# ========== Entradas CLI/GUI ==========

def main():
    # Instalar manejadores de señales para apagado limpio
    def _sig_handler(signum, frame):
        try:
            print('\n[!] Señal recibida ({}). Deteniendo puentes...'.format(signum))
        except Exception:
            pass
        try:
            _cleanup_all_bridges()
        finally:
            try:
                # Salida inmediata
                os._exit(0)
            except Exception:
                pass

    try:
        signal.signal(signal.SIGINT, _sig_handler)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    # Autostart: crear/verificar tarea programada al inicio de sesion
    if '--uninstall-autostart' in sys.argv:
        ok, msg = autostart_uninstall()
        print('[Autostart] ' + msg)
        return
    if '--no-autostart' not in sys.argv:
        ensure_autostart()

    # Helper para extraer flag con valor: --flag valor
    def _get_flag(flag_name, default=None):
        for i, a in enumerate(sys.argv):
            if a == flag_name and i + 1 < len(sys.argv):
                return sys.argv[i + 1]
        return default

    def _has_flag(flag_name):
        return flag_name in sys.argv

    # Detectar modo CLI: al menos 4 args posicionales (ip_puente puerto ip_target puerto)
    # Filtrar flags conocidos para contar solo posicionales
    known_flags_with_val = ('--mqtt-broker', '--mqtt-port', '--mqtt-topic', '--mqtt-user', '--mqtt-pass',
                            '--status-api-url', '--status-api-interval', '--status-api-key',
                            '--ota-topic',
                            '--ec-host', '--ec-port', '--ec-user', '--ec-pass', '--ec-authdb',
                            '--ec-db', '--ec-col',
                            '--ec-field-ts', '--ec-field-text', '--ec-field-author',
                            '--ec-interval', '--ec-backfill-hours')
    known_flags_solo = ('--gui', '--mqtt', '--ext-comments', '--no-autostart', '--uninstall-autostart')
    positional = []
    skip_next = False
    for i, a in enumerate(sys.argv[1:], 1):
        if skip_next:
            skip_next = False
            continue
        if a in known_flags_with_val:
            skip_next = True
            continue
        if a in known_flags_solo:
            continue
        positional.append(a)

    is_cli = len(positional) >= 4 and not _has_flag('--gui')

    if is_cli:
        global _cli_status_instance
        listen_ip = positional[0]
        listen_port = int(positional[1])
        forward_ip = positional[2]
        forward_port = int(positional[3])
        source_ip = positional[4] if len(positional) >= 5 else None

        # MQTT flags
        delivery_mode = 'mqtt' if _has_flag('--mqtt') else 'tcp'
        mqtt_broker_ip = _get_flag('--mqtt-broker')
        mqtt_broker_port = int(_get_flag('--mqtt-port', '1883'))
        mqtt_topic = _get_flag('--mqtt-topic', 'puente/wits')
        mqtt_user = _get_flag('--mqtt-user')
        mqtt_pass = _get_flag('--mqtt-pass')

        # Status API flags
        status_api_url = _get_flag('--status-api-url')
        status_api_interval = int(_get_flag('--status-api-interval', '60'))
        status_api_key = _get_flag('--status-api-key')

        # OTA flags
        ota_topic = _get_flag('--ota-topic', 'puente/ota')

        # External comments flags
        ext_comments_enabled = _has_flag('--ext-comments')
        ext_comments_cfg = {}
        if ext_comments_enabled:
            ext_comments_cfg = {
                'host': _get_flag('--ec-host', 'localhost'),
                'port': int(_get_flag('--ec-port', '27017')),
                'user': _get_flag('--ec-user'),
                'password': _get_flag('--ec-pass'),
                'authdb': _get_flag('--ec-authdb', 'admin'),
                'db': _get_flag('--ec-db'),
                'collection': _get_flag('--ec-col', 'timedata'),
                'field_ts': _get_flag('--ec-field-ts', 'ts'),
                'field_text': _get_flag('--ec-field-text', 'text'),
                'field_author': _get_flag('--ec-field-author', 'author'),
                'interval': float(_get_flag('--ec-interval', '5')),
                'backfill_hours': int(_get_flag('--ec-backfill-hours', '1')),
            }
            if not ext_comments_cfg['collection']:
                print('ERROR: --ext-comments requiere --ec-col')
                return

        if delivery_mode == 'mqtt' and not mqtt_broker_ip:
            print('ERROR: modo --mqtt requiere --mqtt-broker <ip>')
            print('Ejemplo: python {} {} {} {} {} --mqtt --mqtt-broker 192.168.1.100 --mqtt-topic wits/data'.format(
                sys.argv[0], listen_ip, listen_port, forward_ip, forward_port))
            return

        bridge = TCPBridge(listen_ip, listen_port, forward_ip, forward_port,
                           source_ip=source_ip, delivery_mode=delivery_mode,
                           mqtt_broker_ip=mqtt_broker_ip, mqtt_broker_port=mqtt_broker_port,
                           mqtt_topic=mqtt_topic, mqtt_user=mqtt_user, mqtt_pass=mqtt_pass,
                           status_api_url=status_api_url, status_api_interval=status_api_interval,
                           status_api_key=status_api_key,
                           ota_topic=ota_topic,
                           ext_comments_enabled=ext_comments_enabled,
                           ext_comments_cfg=ext_comments_cfg)
        # Crear indicador de estado CLI
        cli_status = CLIStatus(bridge)
        _cli_status_instance = cli_status
        try:
            # Iniciar bridge en hilo separado para poder mostrar estado
            bridge_thread = threading.Thread(target=bridge.start)
            bridge_thread.daemon = True
            bridge_thread.start()
            # Esperar un momento para que el bridge inicie
            time.sleep(0.5)
            # Iniciar indicador de estado CLI
            cli_status.start()
            # Esperar hasta que el bridge termine o se interrumpa
            while bridge.running and bridge_thread.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            cli_status.stop()
            print("\n[+] Cerrando puente...")
            bridge.stop()
        except Exception as e:
            cli_status.stop()
            print("[-] Error: {}".format(e))
            bridge.stop()
        finally:
            cli_status.stop()
            _cli_status_instance = None
        return

    # Caso contrario, abrir GUI (si está disponible)
    if tk is None:
        print('Tkinter no disponible. Use modo CLI:')
        print("Uso: python {} <ip_puente> <puerto_puente> <ip_target> <puerto_target> [source_ip]".format(sys.argv[0]))
        print("     Flags MQTT: --mqtt --mqtt-broker <ip> [--mqtt-port <port>] [--mqtt-topic <topic>]")
        print("     Flag OTA:   --ota-topic <topic>  (default: puente/ota, requiere --mqtt-broker)")
        return

    root = tk.Tk()
    app = BridgeGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()