# Plan de trabajo: Recepción de comentarios externos

Añadir al puente `tcp_4guard.py` la capacidad de leer comentarios desde una
base de datos MongoDB externa y publicarlos vía MQTT a un topic dedicado,
para que la app RTDriller los consuma y los muestre en las tendencias.

## Contexto

- El puente ya tiene publisher MQTT (clase `MQTTPublisher`), SQLite backlog
  (`tcp_4guard.db`), hilos de trabajo, GUI Tkinter y modo CLI con argparse.
- La app RTDriller actualmente sincroniza comentarios haciendo polling directo
  desde el backend Node hacia la Mongo del cliente
  (`backend/src/services/externalCommentsService.js`). Esto obliga a abrir la
  Mongo del cliente al backend y gestionar credenciales ahí.
- Moviendo esa responsabilidad al puente (que ya está en la red del cliente
  y ya habla MQTT con el backend), se simplifica la arquitectura y se reutiliza
  la infraestructura de mensajería existente.

## Diseño

```
┌──────────────────┐        ┌───────────────────┐       ┌──────────────────┐
│ Mongo externa    │ poll   │ tcp_4guard.py     │ MQTT  │ RTDriller backend│
│ (comentarios)    │◄───────│ ExternalComments  │──────►│ MQTT subscriber  │
│                  │        │ Poller            │       │ → Comment model  │
└──────────────────┘        └───────────────────┘       └──────────────────┘
```

### Topic MQTT

- `dw/comments/<configId>` — configId de la transmisión RTDriller destino.
- Payload JSON por mensaje:
  ```json
  {
    "ts": "2026-04-17T13:00:00.000Z",
    "text": "Comentario del operador",
    "author": "Juan",
    "source": "dwcore"
  }
  ```
- QoS 1, retained false.

### Dependencias nuevas

- `pymongo` — `pip install pymongo`
- (Ya se tiene `paho-mqtt`, `sqlite3`, `threading`).

## Componentes a crear en tcp_4guard.py

### 1. Clase `ExternalCommentsPoller`

Archivo: misma `tcp_4guard.py` (o módulo separado si se quiere limpiar).

Responsabilidades:
- Conectar a Mongo externa con `MongoClient(uri)` usando host/port/user/pass/authdb.
- Mantener `last_ts` (fecha ISO) del último comentario procesado. Al iniciar,
  leer de SQLite `poller_state` la última fecha; si no existe, usar `now() - 1h`.
- Cada `POLL_INTERVAL_SEC` (default 5s), consultar
  `db[collection].find({ts: {"$gt": last_ts}}).sort("ts", 1).limit(500)`.
- Por cada documento:
  - Extraer `ts`, `text`, `author` (configurable los nombres de campo).
  - Validar que `ts` sea Date/ISO y `text` no vacío.
  - Publicar al topic MQTT vía una referencia al `MQTTPublisher` existente
    (añadir método `publish_comment(configId, payload_dict)` que serializa JSON).
  - Actualizar `last_ts` al `ts` de este documento.
  - Persistir `last_ts` en SQLite `poller_state` (una fila por fuente).
- Si la publicación MQTT falla, guardar el documento en una nueva tabla
  SQLite `comments_backlog` y reintentar en el siguiente tick.

Interfaz:
```python
class ExternalCommentsPoller(threading.Thread):
    def __init__(self, mongo_cfg, mqtt_publisher, config_id,
                 field_ts='ts', field_text='text', field_author='author',
                 poll_interval=5.0, db_path='tcp_4guard.db'):
        ...
    def run(self):
        ...
    def stop(self):
        ...
```

### 2. Extender SQLite (en `tcp_4guard.db`)

Dos tablas nuevas:

```sql
CREATE TABLE IF NOT EXISTS poller_state (
  source_key TEXT PRIMARY KEY,
  last_ts    TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS comments_backlog (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  config_id   TEXT NOT NULL,
  payload     TEXT NOT NULL,     -- JSON serializado
  created_at  TEXT NOT NULL,
  attempts    INTEGER DEFAULT 0
);
```

Crear helpers `get_last_ts(source_key)`, `set_last_ts(source_key, ts)`,
`enqueue_comment(config_id, payload)`, `drain_comments_backlog(publisher)`.

### 3. Extender `MQTTPublisher`

Añadir método:
```python
def publish_comment(self, config_id, payload_dict):
    topic = 'dw/comments/{}'.format(config_id)
    payload_str = json.dumps(payload_dict, ensure_ascii=False, default=str)
    info = self._client.publish(topic, payload_str, qos=1)
    return info.rc == 0
```

### 4. CLI / argparse

Añadir flags:

```
--ext-comments              (flag para activar la feature)
--ec-host <host>
--ec-port <port>            (default 27017)
--ec-user <user>
--ec-pass <pass>
--ec-authdb <db>            (default 'admin')
--ec-db <db>
--ec-col <collection>
--ec-config-id <id>         (configId de la transmisión destino en RTDriller)
--ec-field-ts <name>        (default 'ts')
--ec-field-text <name>      (default 'text')
--ec-field-author <name>    (default 'author')
--ec-interval <sec>         (default 5)
```

### 5. GUI Tkinter

Nueva pestaña **"Comentarios externos"** con:
- Checkbox "Habilitar"
- Campos: host, port, user, pass, authdb, database, collection
- Campo: configId RTDriller
- Campos opcionales: nombre de los campos ts/text/author
- Slider/input: intervalo de polling (1-60s)
- Indicador: último ts procesado, contador de publicados, backlog actual
- Botón "Probar conexión" que hace un `find().limit(1)` y muestra el resultado

Persistir configuración en el `.ini` ya usado por el bridge
(`configparser`), sección `[external_comments]`.

### 6. Ciclo de vida

- Al arrancar el bridge (CLI o GUI), si `--ext-comments` está activo,
  instanciar `ExternalCommentsPoller` y lanzarlo como hilo daemon después de
  que el `MQTTPublisher` esté conectado.
- Al cerrar el bridge (signal handler existente), llamar `.stop()` y `.join()`
  con timeout 5s.
- Al reconectar MQTT (lógica ya existente en `MQTTPublisher.maintain`),
  drenar `comments_backlog` automáticamente.

### 7. Logging y diagnóstico

Prefijo `[ExtComments]` en todos los logs. Log al menos:
- Inicio/fin del poller
- Cada tick: cuántos docs encontrados, cuántos publicados, cuántos al backlog
- Errores de conexión Mongo o MQTT
- Estado en CLI (integrar con `CLIStatus`)

## Del lado RTDriller backend

Cambios necesarios en `/opt/rtdriller/backend/src/`:

### 1. Nuevo suscriptor MQTT

En `services/mqttService.js` añadir suscripción al patrón `dw/comments/+`:

```js
this.client.subscribe('dw/comments/+', { qos: 1 });
```

Y en el handler de mensajes:

```js
if (topic.startsWith('dw/comments/')) {
  const configId = topic.split('/').pop();
  const payload = JSON.parse(message.toString());
  await handleExternalCommentFromMqtt(configId, payload);
  return;
}
```

### 2. `handleExternalCommentFromMqtt(configId, payload)`

Crear un nuevo servicio `services/commentsIngestService.js`:

```js
async function handleExternalCommentFromMqtt(configId, payload) {
  const { ts, text, author, source } = payload || {};
  if (!ts || !text) return;
  const tsDate = new Date(ts);
  if (isNaN(tsDate.getTime())) return;
  // Dedup por (transmissionId + ts + text)
  const existing = await Comment.findOne({
    transmissionId: configId,
    ts: tsDate,
    text: String(text).slice(0, 2000),
  });
  if (existing) return;
  await Comment.create({
    transmissionId: configId,
    ts: tsDate,
    text: String(text).slice(0, 2000),
    author: author || source || 'external',
  });
  // Publicar vía GraphQL subscription para que TrendVertical reciba en vivo
  pubsub.publish('COMMENT_ADDED', { commentAdded: { ... } });
}
```

### 3. Deprecar `externalCommentsService.js` (polling)

Desactivar su inicio en `startRealtimeConnection` cuando la transmisión tenga
flag `externalComments.mode: 'mqtt'`. Mantener el modo polling como legacy
para las transmisiones que aún lo usen.

Añadir campo al modelo `TransmissionConfig`:
```js
externalComments: {
  enabled: Boolean,
  mode: { type: String, enum: ['polling', 'mqtt'], default: 'polling' },
  ...
}
```

Cuando `mode === 'mqtt'`, no se ejecuta el polling desde el backend — los
comentarios llegan vía MQTT desde el puente.

## Orden de implementación sugerido

1. **Backend RTDriller**: agregar suscriptor MQTT a `dw/comments/+` y
   servicio de ingesta con dedup. Probarlo publicando manualmente con
   `mosquitto_pub` un mensaje al topic.
2. **tcp_4guard.py**:
   - Extender SQLite con las nuevas tablas.
   - Añadir método `publish_comment` al `MQTTPublisher`.
   - Implementar clase `ExternalCommentsPoller` y probarla con CLI.
3. **Integrar con GUI Tkinter** y persistencia en `.ini`.
4. **Pruebas end-to-end**: configurar fuente externa, generar un comentario,
   verificar que aparezca en la tendencia de RTDriller.
5. **Migrar transmisiones existentes** de modo `polling` a `mqtt` cambiando
   el flag en MongoDB.

## Consideraciones operativas

- **Una instancia del puente por fuente Mongo**. Si hay varias fuentes
  (varios taladros con Mongos distintas), instanciar varios `ExternalCommentsPoller`
  dentro del mismo proceso (permitir repetir `--ext-comments` en CLI con IDs
  distintos, o lista en la GUI).
- **Relojes**: asegurar que el `ts` en Mongo externa esté en UTC. Si viene
  en hora local, aplicar la TZ configurada en el puente antes de publicar.
- **Backfill**: al habilitar la feature por primera vez en un cliente con
  datos históricos en su Mongo, el `last_ts` inicial puede retroceder para
  recuperar N horas/días (flag `--ec-backfill-hours`).
- **Batch size**: límite de 500 por query para no saturar MQTT; si quedan más,
  se procesan en el siguiente tick.
- **Resiliencia**: si MongoDB externa se cae, el poller reintenta con
  backoff exponencial (5s → 10s → 30s → 60s, cap).

## Archivos a tocar

En `D:\PuenteDWRTD\`:
- `tcp_4guard.py` — agregar clase, extender MQTTPublisher, CLI, GUI.
- `PLAN_COMENTARIOS_EXTERNOS.md` (este archivo).
- Opcional: `external_comments_poller.py` como módulo separado si prefieres
  no crecer `tcp_4guard.py`.

En la app RTDriller (servidor):
- `backend/src/services/mqttService.js`
- `backend/src/services/commentsIngestService.js` (nuevo)
- `backend/src/models/TransmissionConfig.js` (agregar campo `mode`)
- `backend/src/resolvers/transmissionConfig.js` (no iniciar polling si mode=mqtt)
