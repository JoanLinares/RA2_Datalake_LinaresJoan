"""
Gaming Warehouse Loader
Carga mercados de videojuegos/esports desde Delta Lake a NeonDB (PostgreSQL).
Esquema dimensional gaming en espaÃ±ol:
  - dim_fecha          â†’ DimensiÃ³n temporal
  - dim_videojuego     â†’ Tipos de juego (DOTA, Valorant, CS:GO, etc.)
  - dim_mercado_gaming â†’ Preguntas/mercados de apuestas gaming
  - fact_metricas_gaming â†’ Volumen, liquidez, precios

Uso:
    python src/warehouse/loader_NeonDB.py
"""
import os
import sys
import io
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from deltalake import DeltaTable

# Consola UTF-8 en Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Agregar src al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.transformer_data import DataTransformer

# Cargar variables de entorno
load_dotenv()

# ConfiguraciÃ³n de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ConfiguraciÃ³n
DATABASE_URL = os.getenv('DATABASE_URL')
BASE_PATH = Path("datalake/raw")


class WarehouseLoader:
    """Cargador de datos gaming hacia NeonDB (modelo dimensional en espaÃ±ol)"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        self.cursor = None

    # ------------------------------------------------------------------
    # Conexión
    # ------------------------------------------------------------------
    def connect(self):
        """Conecta a NeonDB"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor()
            logger.info("Conectado a NeonDB")
        except psycopg2.Error as e:
            logger.error(f"Error de conexion: {e}")
            raise

    # ------------------------------------------------------------------
    # Schema gaming
    # ------------------------------------------------------------------
    def create_schema_gaming(self):
        """Crea (o recrea) el modelo dimensional gaming completo"""
        try:
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_evento_tag_gaming CASCADE;
                DROP TABLE IF EXISTS fact_mercado_evento_gaming CASCADE;
                DROP TABLE IF EXISTS fact_metricas_gaming CASCADE;
                DROP TABLE IF EXISTS dim_mercado_gaming CASCADE;
                DROP TABLE IF EXISTS dim_evento_gaming CASCADE;
                DROP TABLE IF EXISTS dim_tag_gaming CASCADE;
                DROP TABLE IF EXISTS dim_serie_gaming CASCADE;
                DROP TABLE IF EXISTS dim_videojuego CASCADE;
                DROP TABLE IF EXISTS dim_fecha CASCADE;
            """)

            # dim_fecha
            self.cursor.execute("""
                CREATE TABLE dim_fecha (
                    fecha_id     SERIAL PRIMARY KEY,
                    fecha        DATE NOT NULL UNIQUE,
                    anio         INT  NOT NULL,
                    mes          INT  NOT NULL,
                    dia          INT  NOT NULL,
                    trimestre    INT  NOT NULL,
                    dia_semana   INT  NOT NULL,
                    es_finde     BOOLEAN NOT NULL
                );
            """)
            logger.info("  dim_fecha creada")

            # dim_videojuego
            self.cursor.execute("""
                CREATE TABLE dim_videojuego (
                    videojuego_id  SERIAL PRIMARY KEY,
                    nombre_juego   VARCHAR(100) NOT NULL UNIQUE,
                    genero         VARCHAR(100),
                    es_esports     BOOLEAN DEFAULT FALSE
                );
            """)
            # Catálogo de juegos esports competitivos
            juegos = [
                ('DOTA',             'MOBA',          True),
                ('Valorant',         'FPS Táctico',   True),
                ('CS:GO',            'FPS Táctico',   True),
                ('League of Legends','MOBA',          True),
                ('Fortnite',         'Battle Royale', True),
                ('Overwatch',        'Hero Shooter',  True),
                ('Apex Legends',     'Battle Royale', True),
                ('Call of Duty',     'FPS',           True),
                ('Rocket League',    'Deportes',      True),
                ('Hearthstone',      'Cartas',        True),
                ('StarCraft',        'RTS',           True),
                ('Rainbow Six',      'FPS Táctico',   True),
                ('Esports General',  'Esports',       True),
            ]
            execute_values(
                self.cursor,
                "INSERT INTO dim_videojuego (nombre_juego, genero, es_esports) VALUES %s",
                juegos
            )
            logger.info("  dim_videojuego creada y poblada")

            # dim_serie_gaming
            self.cursor.execute("""
                CREATE TABLE dim_serie_gaming (
                    serie_id     VARCHAR(50) PRIMARY KEY,
                    serie_slug   VARCHAR(500),
                    titulo       VARCHAR(2048),
                    descripcion  TEXT
                );
            """)
            logger.info("  dim_serie_gaming creada")

            # dim_evento_gaming
            self.cursor.execute("""
                CREATE TABLE dim_evento_gaming (
                    evento_id        VARCHAR(50) PRIMARY KEY,
                    titulo           VARCHAR(2048),
                    categoria        VARCHAR(200),
                    subcategoria     VARCHAR(200),
                    ticker           VARCHAR(500),
                    slug             VARCHAR(500),
                    es_activo        BOOLEAN,
                    es_cerrado       BOOLEAN,
                    es_destacado     BOOLEAN,
                    fecha_creacion   TIMESTAMP,
                    fecha_inicio     TIMESTAMP,
                    fecha_fin        TIMESTAMP,
                    fuente_resolucion VARCHAR(500),
                    serie_id         VARCHAR(50) REFERENCES dim_serie_gaming(serie_id)
                );
                CREATE INDEX idx_evento_categoria ON dim_evento_gaming(categoria);
                CREATE INDEX idx_evento_ticker ON dim_evento_gaming(ticker);
                CREATE INDEX idx_evento_serie ON dim_evento_gaming(serie_id);
            """)
            logger.info("  dim_evento_gaming creada")

            # dim_tag_gaming
            self.cursor.execute("""
                CREATE TABLE dim_tag_gaming (
                    tag_id     SERIAL PRIMARY KEY,
                    tag_nombre VARCHAR(200) NOT NULL UNIQUE
                );
            """)
            logger.info("  dim_tag_gaming creada")

            # dim_mercado_gaming
            self.cursor.execute("""
                CREATE TABLE dim_mercado_gaming (
                    mercado_id      VARCHAR(100) PRIMARY KEY,
                    pregunta        TEXT,
                    tipo_apuesta    VARCHAR(100),
                    videojuego_id   INT REFERENCES dim_videojuego(videojuego_id),
                    slug            VARCHAR(500),
                    esta_activo     BOOLEAN,
                    esta_cerrado    BOOLEAN,
                    fecha_fin       TIMESTAMP,
                    outcomes        TEXT,
                    fuente_resolucion VARCHAR(500),
                    creado_en       TIMESTAMP,
                    actualizado_en  TIMESTAMP
                );
                CREATE INDEX idx_mercado_juego ON dim_mercado_gaming(videojuego_id);
                CREATE INDEX idx_mercado_tipo ON dim_mercado_gaming(tipo_apuesta);
            """)
            logger.info("  dim_mercado_gaming creada")

            # fact_mercado_evento_gaming
            self.cursor.execute("""
                CREATE TABLE fact_mercado_evento_gaming (
                    mercado_evento_id SERIAL PRIMARY KEY,
                    mercado_id   VARCHAR(100) REFERENCES dim_mercado_gaming(mercado_id),
                    evento_id    VARCHAR(50) REFERENCES dim_evento_gaming(evento_id),
                    UNIQUE (mercado_id, evento_id)
                );
                CREATE INDEX idx_mercado_evento ON fact_mercado_evento_gaming(mercado_id);
                CREATE INDEX idx_evento_mercado ON fact_mercado_evento_gaming(evento_id);
            """)
            logger.info("  fact_mercado_evento_gaming creada")

            # fact_evento_tag_gaming
            self.cursor.execute("""
                CREATE TABLE fact_evento_tag_gaming (
                    evento_tag_id SERIAL PRIMARY KEY,
                    evento_id VARCHAR(50) REFERENCES dim_evento_gaming(evento_id),
                    tag_id    INT REFERENCES dim_tag_gaming(tag_id),
                    UNIQUE (evento_id, tag_id)
                );
                CREATE INDEX idx_evento_tag_evento ON fact_evento_tag_gaming(evento_id);
                CREATE INDEX idx_evento_tag_tag ON fact_evento_tag_gaming(tag_id);
            """)
            logger.info("  fact_evento_tag_gaming creada")

            # fact_metricas_gaming
            self.cursor.execute("""
                CREATE TABLE fact_metricas_gaming (
                    metrica_id      SERIAL PRIMARY KEY,
                    mercado_id      VARCHAR(100) REFERENCES dim_mercado_gaming(mercado_id),
                    fecha_id        INT REFERENCES dim_fecha(fecha_id),
                    volumen_total   NUMERIC(20,8),
                    liquidez_total  NUMERIC(20,8),
                    precio_ultimo   NUMERIC(10,6),
                    mejor_compra    NUMERIC(10,6),
                    mejor_venta     NUMERIC(10,6),
                    spread          NUMERIC(10,6),
                    interes_abierto NUMERIC(20,8)
                );
                CREATE INDEX idx_metricas_mercado ON fact_metricas_gaming(mercado_id);
                CREATE INDEX idx_metricas_fecha   ON fact_metricas_gaming(fecha_id);
            """)
            logger.info("  fact_metricas_gaming creada")

            self.conn.commit()
            logger.info("Schema gaming creado exitosamente")

        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error al crear schema: {e}")
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_videojuego_map(self) -> Dict[str, int]:
        """Devuelve {nombre_juego: videojuego_id}"""
        self.cursor.execute("SELECT videojuego_id, nombre_juego FROM dim_videojuego")
        return {nombre: vid_id for vid_id, nombre in self.cursor.fetchall()}

    def _get_or_create_fecha(self, dates) -> Dict:
        """Inserta fechas en dim_fecha y devuelve {date: fecha_id}"""
        fecha_map = {}
        dates_clean = pd.to_datetime(dates, errors='coerce').dropna().unique()
        for ts in dates_clean:
            d = pd.Timestamp(ts).date()
            self.cursor.execute("SELECT fecha_id FROM dim_fecha WHERE fecha = %s", (d,))
            row = self.cursor.fetchone()
            if row:
                fecha_map[d] = row[0]
            else:
                t = pd.Timestamp(ts)
                self.cursor.execute("""
                    INSERT INTO dim_fecha (fecha, anio, mes, dia, trimestre, dia_semana, es_finde)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING fecha_id
                """, (d, t.year, t.month, t.day,
                      (t.month-1)//3+1, t.dayofweek, t.dayofweek >= 5))
                fecha_map[d] = self.cursor.fetchone()[0]
        self.conn.commit()
        return fecha_map

    def _parse_list_value(self, value) -> List[str]:
        """Convierte campos tipo lista (str JSON o lista real) a lista de strings"""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v is not None]
        try:
            text = str(value).strip()
            if not text:
                return []
            if text.startswith('['):
                text = text.replace("'", '"')
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(v) for v in parsed if v is not None]
        except Exception:
            return []
        return []

    # ------------------------------------------------------------------
    # Carga dimensiones gaming adicionales
    # ------------------------------------------------------------------
    def load_dim_evento_gaming(self, events_df: pd.DataFrame):
        """Carga dim_evento_gaming"""
        try:
            df = events_df.copy()
            date_cols = ['startDate', 'endDate', 'creationDate', 'createdAt', 'updatedAt']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Pre-cargar serie_ids válidos para respetar FK
            self.cursor.execute("SELECT serie_id FROM dim_serie_gaming")
            valid_series = {r[0] for r in self.cursor.fetchall()}

            rows = []
            for _, r in df.iterrows():
                event_id = str(r['id']) if pd.notna(r.get('id')) else None
                if not event_id:
                    continue
                # Extraer serie_id del campo 'series' (JSON) o 'seriesId'
                serie_id = None
                if pd.notna(r.get('seriesId')):
                    serie_id = str(r['seriesId'])
                elif 'series' in r.index:
                    sval = r.get('series')
                    if sval is not None and not (isinstance(sval, float) and pd.isna(sval)):
                        sraw = str(sval).strip()
                        if sraw and sraw not in ('nan', 'None', '[]'):
                            try:
                                sp = json.loads(sraw.replace("'", '"'))
                                if isinstance(sp, list) and len(sp) > 0 and isinstance(sp[0], dict):
                                    serie_id = str(sp[0].get('id', ''))
                                elif isinstance(sp, dict) and sp.get('id'):
                                    serie_id = str(sp['id'])
                            except Exception:
                                pass
                if serie_id and serie_id not in valid_series:
                    serie_id = None  # No existe en dim_serie, poner NULL
                rows.append((
                    event_id,
                    DataTransformer.clean_string(r.get('title'), 2048),
                    DataTransformer.clean_string(r.get('category'), 200),
                    DataTransformer.clean_string(r.get('subcategory'), 200),
                    DataTransformer.clean_string(r.get('ticker'), 500),
                    DataTransformer.clean_string(r.get('slug'), 500),
                    DataTransformer.normalize_boolean(r.get('active')),
                    DataTransformer.normalize_boolean(r.get('closed')),
                    DataTransformer.normalize_boolean(r.get('featured')),
                    r.get('creationDate') if pd.notna(r.get('creationDate')) else None,
                    r.get('startDate') if pd.notna(r.get('startDate')) else None,
                    r.get('endDate') if pd.notna(r.get('endDate')) else None,
                    DataTransformer.clean_string(r.get('resolutionSource'), 500),
                    serie_id,
                ))

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO dim_evento_gaming
                    (evento_id, titulo, categoria, subcategoria, ticker, slug,
                     es_activo, es_cerrado, es_destacado, fecha_creacion,
                     fecha_inicio, fecha_fin, fuente_resolucion, serie_id)
                    VALUES %s
                    ON CONFLICT (evento_id) DO NOTHING
                """, rows, page_size=1000)
                self.conn.commit()
                logger.info(f"  dim_evento_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_dim_evento_gaming: {e}")
            raise

    def load_dim_serie_gaming(self, series_df: pd.DataFrame):
        """Carga dim_serie_gaming"""
        try:
            df = series_df.copy()
            rows = []
            for _, r in df.iterrows():
                serie_id = str(r['id']) if pd.notna(r.get('id')) else None
                if not serie_id:
                    continue
                rows.append((
                    serie_id,
                    DataTransformer.clean_string(r.get('slug'), 500),
                    DataTransformer.clean_string(r.get('title'), 2048),
                    DataTransformer.clean_string(r.get('description'), 5000),
                ))

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO dim_serie_gaming (serie_id, serie_slug, titulo, descripcion)
                    VALUES %s
                    ON CONFLICT (serie_id) DO NOTHING
                """, rows, page_size=500)
                self.conn.commit()
                logger.info(f"  dim_serie_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_dim_serie_gaming: {e}")
            raise

    def _parse_tags_field(self, val) -> List[str]:
        """Parsea el campo tags de eventos (JSON objects con label/slug o lista simple)"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return []
        raw = str(val).strip()
        if not raw or raw in ('nan', 'None', '[]'):
            return []
        try:
            parsed = json.loads(raw.replace("'", '"'))
            if isinstance(parsed, list):
                result = []
                for item in parsed:
                    if isinstance(item, dict):
                        label = item.get('label') or item.get('slug') or item.get('id')
                        if label and str(label).strip():
                            result.append(str(label).strip())
                    elif isinstance(item, str) and item.strip():
                        result.append(item.strip())
                return result
        except Exception:
            pass
        return self._parse_list_value(val)

    def load_dim_tag_gaming(self, events_df: pd.DataFrame):
        """Carga dim_tag_gaming desde tags de eventos (soporta JSON objects y strings)"""
        try:
            tags = set()
            if 'tags' in events_df.columns:
                for val in events_df['tags']:
                    for tag in self._parse_tags_field(val):
                        tags.add(tag)

            if not tags:
                logger.info("  dim_tag_gaming: no se encontraron tags")
                return

            execute_values(
                self.cursor,
                "INSERT INTO dim_tag_gaming (tag_nombre) VALUES %s ON CONFLICT (tag_nombre) DO NOTHING",
                [(t,) for t in sorted(tags)],
                page_size=1000
            )
            self.conn.commit()
            logger.info(f"  dim_tag_gaming: {len(tags):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_dim_tag_gaming: {e}")
            raise

    def load_relations_mercado_evento_gaming(self, markets_df: pd.DataFrame):
        """Carga relaciones mercado-evento (solo gaming)"""
        try:
            if 'events' not in markets_df.columns:
                logger.info("  fact_mercado_evento_gaming: columna events no disponible")
                return

            rows = []
            for _, r in markets_df.iterrows():
                mercado_id = str(r['id']) if pd.notna(r.get('id')) else None
                if not mercado_id:
                    continue
                # El campo events contiene objetos JSON completos, extraer id
                val = r.get('events')
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                raw = str(val).strip()
                if not raw or raw in ('nan', 'None', '[]'):
                    continue
                try:
                    parsed = json.loads(raw.replace("'", '"'))
                    if isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, dict):
                                eid = item.get('id')
                                if eid:
                                    rows.append((mercado_id, str(eid)))
                            elif item is not None:
                                rows.append((mercado_id, str(item)))
                    elif isinstance(parsed, dict):
                        eid = parsed.get('id')
                        if eid:
                            rows.append((mercado_id, str(eid)))
                except Exception:
                    pass

            if rows:
                # Filtrar solo evento_ids que existan en dim_evento_gaming
                self.cursor.execute("SELECT evento_id FROM dim_evento_gaming")
                valid_events = {r[0] for r in self.cursor.fetchall()}
                # Filtrar solo mercado_ids que existan en dim_mercado_gaming
                self.cursor.execute("SELECT mercado_id FROM dim_mercado_gaming")
                valid_markets = {r[0] for r in self.cursor.fetchall()}
                rows = [(m, e) for m, e in rows if e in valid_events and m in valid_markets]
                logger.info(f"  fact_mercado_evento_gaming: {len(rows):,} relaciones válidas")

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO fact_mercado_evento_gaming (mercado_id, evento_id)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, rows, page_size=1000)
                self.conn.commit()
                logger.info(f"  fact_mercado_evento_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_relations_mercado_evento_gaming: {e}")
            raise

    def load_relations_evento_tag_gaming(self, events_df: pd.DataFrame):
        """Carga relaciones evento-tag (solo gaming) - batch optimizado"""
        try:
            if 'tags' not in events_df.columns:
                logger.info("  fact_evento_tag_gaming: columna tags no disponible")
                return

            # Pre-cargar lookup maps
            self.cursor.execute("SELECT evento_id FROM dim_evento_gaming")
            valid_events = {r[0] for r in self.cursor.fetchall()}

            self.cursor.execute("SELECT tag_nombre, tag_id FROM dim_tag_gaming")
            tag_map = {r[0]: r[1] for r in self.cursor.fetchall()}

            rows = []
            for _, r in events_df.iterrows():
                event_id = str(r['id']) if pd.notna(r.get('id')) else None
                if not event_id or event_id not in valid_events:
                    continue
                for tag in self._parse_tags_field(r.get('tags')):
                    tag_id = tag_map.get(tag)
                    if tag_id:
                        rows.append((event_id, tag_id))

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO fact_evento_tag_gaming (evento_id, tag_id)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, rows, page_size=1000)
                self.conn.commit()
            logger.info(f"  fact_evento_tag_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_relations_evento_tag_gaming: {e}")
            raise

    # ------------------------------------------------------------------
    # Carga de tablas
    # ------------------------------------------------------------------
    def load_dim_mercado_gaming(self, df: pd.DataFrame):
        """Carga dim_mercado_gaming"""
        try:
            juego_map = self._get_videojuego_map()
            rows = []
            for _, r in df.iterrows():
                mid = str(r['id']) if pd.notna(r.get('id')) else None
                if not mid:
                    continue
                gaming_type = r.get('gaming_type') or 'Other Gaming'
                vid_id = juego_map.get(gaming_type, juego_map.get('Other Gaming'))

                outcomes = None
                try:
                    val = r.get('outcomes_list')
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        outcomes = json.dumps(list(val) if not isinstance(val, (str, list)) else val)[:2000]
                except Exception:
                    pass

                rows.append((
                    mid,
                    str(r['question'])[:2000] if pd.notna(r.get('question')) else None,
                    str(r.get('bet_type', ''))[:100] or None,
                    vid_id,
                    str(r.get('slug', ''))[:500] or None,
                    bool(r['active']) if pd.notna(r.get('active')) else False,
                    bool(r['closed']) if pd.notna(r.get('closed')) else False,
                    pd.to_datetime(r['endDate'], errors='coerce') if pd.notna(r.get('endDate')) else None,
                    outcomes,
                    str(r.get('resolutionSource', ''))[:500] or None,
                    pd.to_datetime(r['createdAt'], errors='coerce') if pd.notna(r.get('createdAt')) else None,
                    pd.to_datetime(r['updatedAt'], errors='coerce') if pd.notna(r.get('updatedAt')) else None,
                ))

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO dim_mercado_gaming
                    (mercado_id,pregunta,tipo_apuesta,videojuego_id,slug,
                     esta_activo,esta_cerrado,fecha_fin,outcomes,
                     fuente_resolucion,creado_en,actualizado_en)
                    VALUES %s
                    ON CONFLICT (mercado_id) DO NOTHING
                """, rows, page_size=1000)
                self.conn.commit()
                logger.info(f"  dim_mercado_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_dim_mercado_gaming: {e}")
            raise

    def load_fact_metricas_gaming(self, df: pd.DataFrame):
        """Carga fact_metricas_gaming"""
        try:
            df = df.copy()
            for col in ['volume','liquidity','lastTradePrice','bestBid','bestAsk','spread','openInterest']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df['_fecha'] = pd.to_datetime(
                df['updatedAt'] if 'updatedAt' in df.columns else datetime.now(),
                errors='coerce'
            ).dt.date

            fecha_map = self._get_or_create_fecha(df['_fecha'].unique())

            rows = []
            for _, r in df.iterrows():
                mid = str(r['id']) if pd.notna(r.get('id')) else None
                if not mid or r['_fecha'] not in fecha_map:
                    continue
                rows.append((
                    mid,
                    fecha_map[r['_fecha']],
                    float(r['volume'])       if pd.notna(r.get('volume'))         else None,
                    float(r['liquidity'])    if pd.notna(r.get('liquidity'))      else None,
                    float(r['lastTradePrice']) if pd.notna(r.get('lastTradePrice')) else None,
                    float(r['bestBid'])      if pd.notna(r.get('bestBid'))        else None,
                    float(r['bestAsk'])      if pd.notna(r.get('bestAsk'))        else None,
                    float(r['spread'])       if pd.notna(r.get('spread'))         else None,
                    float(r['openInterest']) if pd.notna(r.get('openInterest'))   else None,
                ))

            if rows:
                execute_values(self.cursor, """
                    INSERT INTO fact_metricas_gaming
                    (mercado_id,fecha_id,volumen_total,liquidez_total,
                     precio_ultimo,mejor_compra,mejor_venta,spread,interes_abierto)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, rows, page_size=500)
                self.conn.commit()
                logger.info(f"  fact_metricas_gaming: {len(rows):,} filas cargadas")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error en load_fact_metricas_gaming: {e}")
            raise

    # ------------------------------------------------------------------
    # Resumen
    # ------------------------------------------------------------------
    def generate_load_summary(self):
        """Imprime conteos de cada tabla"""
        tablas = {
            'dim_fecha':           'Fechas',
            'dim_videojuego':      'Videojuegos',
            'dim_serie_gaming':    'Series gaming',
            'dim_evento_gaming':   'Eventos gaming',
            'dim_tag_gaming':      'Tags gaming',
            'dim_mercado_gaming':  'Mercados gaming',
            'fact_mercado_evento_gaming': 'Relaciones mercado-evento',
            'fact_evento_tag_gaming':     'Relaciones evento-tag',
            'fact_metricas_gaming':'Metricas gaming',
        }
        logger.info("=== RESUMEN DE CARGA ===")
        for tabla, desc in tablas.items():
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                n = self.cursor.fetchone()[0]
                logger.info(f"  {desc}: {n:,} registros")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Pipeline principal (gaming por defecto)
    # ------------------------------------------------------------------
    def load_all(self):
        """
        Pipeline completo: Delta Lake -> transformacion -> NeonDB gaming.
        Se ejecuta al correr el archivo directamente.
        """
        try:
            logger.info("=" * 70)
            logger.info("PIPELINE GAMING: DELTA LAKE -> NEONDB")
            logger.info("=" * 70)

            # 1. Extraer y transformar
            logger.info("\n[1/3] Extrayendo y transformando datos gaming...")
            df, resumen = DataTransformer.pipeline_complete_gaming(
                datalake_path="datalake/raw/markets"
            )

            if df.empty:
                logger.error("No se encontraron mercados de gaming. Abortando.")
                return

            logger.info(f"  Mercados listos: {resumen['total_markets']:,}")
            logger.info(f"  Volumen total:   ${resumen['total_volume']:,.2f}")
            logger.info(f"  Tipos de juego:  {list(resumen['gaming_types'].keys())}")

            # Extraer eventos/series relacionados (solo gaming)
            events_df = pd.DataFrame()
            series_df = pd.DataFrame()
            event_ids = set()

            # El campo 'events' en markets contiene objetos JSON completos
            # (no IDs simples), hay que parsear y extraer el campo 'id'
            if 'events' in df.columns:
                for val in df['events']:
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        continue
                    raw = str(val).strip()
                    if not raw or raw in ('nan', 'None', '[]'):
                        continue
                    try:
                        parsed = json.loads(raw.replace("'", '"'))
                        if isinstance(parsed, list):
                            for item in parsed:
                                if isinstance(item, dict):
                                    eid = item.get('id')
                                    if eid:
                                        event_ids.add(str(eid))
                                elif item is not None:
                                    event_ids.add(str(item))
                        elif isinstance(parsed, dict):
                            eid = parsed.get('id')
                            if eid:
                                event_ids.add(str(eid))
                    except Exception:
                        pass
            logger.info(f"  Event IDs extraidos de markets: {len(event_ids):,}")

            events_path = BASE_PATH / "events"
            if events_path.exists():
                all_events = DeltaTable(str(events_path)).to_pandas()
            else:
                all_events = pd.DataFrame()
                logger.warning("  No se encontraron datos de eventos en Delta Lake")

            if event_ids and not all_events.empty:
                events_df = all_events[all_events['id'].astype(str).isin(event_ids)].copy()
                logger.info(f"  Eventos gaming (relacionados): {len(events_df):,}")

            if events_df.empty and not all_events.empty:
                # Fallback: filtrar eventos del DL por keywords esports
                # Usar \b para keywords cortos y evitar falsos positivos
                keywords_exact = [
                    r'\besports?\b', r'\bdota\b', r'\bdota\s*2\b', r'\bvalorant\b',
                    r'\bcs[:\-]?go\b', r'\bcounter[\-\s]strike\b',
                    r'\bleague of legends\b', r'\boverwatch\b', r'\bfortnite\b',
                    r'\bapex legends\b', r'\bcall of duty league\b',
                    r'\bhearthstone\b', r'\bstarcraft\b', r'\brocket league\b',
                    r'\brainbow six\b', r'\bblast premier\b', r'\besl pro\b',
                    r'\biem\b', r'\bfaceit\b', r'\bpgl major\b',
                    r'\bvct\b', r'\blck\b', r'\blcs\b', r'\blec\b',
                    r'\bworlds 20\d{2}\b', r'\brlcs\b', r'\bdreamhack\b',
                    r'\bowcs\b', r'\bmsi 20\d{2}\b', r'\bcs2\b',
                ]
                # Keywords de exclusión
                exclude_kws = [
                    'election', 'politic', 'president', 'senate', 'congress',
                    'democrat', 'republican', 'trump', 'biden',
                    'nfl', 'nba', 'mlb', 'nhl', 'soccer', 'football',
                    'baseball', 'hockey', 'tennis', 'golf', 'boxing',
                    'ufc', 'mma', 'horse', 'bitcoin', 'crypto',
                ]
                text_cols = [c for c in ['title', 'category', 'subcategory', 'slug', 'ticker']
                             if c in all_events.columns]
                if text_cols:
                    combined = all_events[text_cols].fillna('').astype(str).agg(' '.join, axis=1).str.lower()
                    pattern = '|'.join(keywords_exact)
                    mask_include = combined.str.contains(pattern, regex=True, na=False)
                    exclude_pattern = '|'.join([re.escape(k) for k in exclude_kws])
                    mask_exclude = combined.str.contains(exclude_pattern, regex=True, na=False)
                    events_df = all_events[mask_include & ~mask_exclude].copy()
                    logger.info(f"  Eventos gaming (filtrados por keywords): {len(events_df):,}")

            if not events_df.empty:
                # Extraer series IDs de dos fuentes:
                # 1) Campo 'series' en events DL (JSON embebido)
                # 2) Campo 'events' en markets (JSON con 'series' embebido)
                series_ids = set()

                # Fuente 1: campo 'series' en events_df
                if 'series' in events_df.columns:
                    for val in events_df['series']:
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            continue
                        raw = str(val).strip()
                        if not raw or raw in ('nan', 'None', '[]'):
                            continue
                        try:
                            parsed = json.loads(raw.replace("'", '"'))
                            if isinstance(parsed, list):
                                for si in parsed:
                                    if isinstance(si, dict) and si.get('id'):
                                        series_ids.add(str(si['id']))
                            elif isinstance(parsed, dict) and parsed.get('id'):
                                series_ids.add(str(parsed['id']))
                        except Exception:
                            pass

                # Fuente 2: campo 'events' en markets (JSON con campo 'series')
                if 'events' in df.columns:
                    for val in df['events']:
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            continue
                        raw = str(val).strip()
                        if not raw or raw in ('nan', 'None', '[]'):
                            continue
                        try:
                            parsed = json.loads(raw.replace("'", '"'))
                            if isinstance(parsed, list):
                                for item in parsed:
                                    if isinstance(item, dict) and 'series' in item:
                                        s = item['series']
                                        if isinstance(s, list):
                                            for si in s:
                                                if isinstance(si, dict) and si.get('id'):
                                                    series_ids.add(str(si['id']))
                        except Exception:
                            pass

                logger.info(f"  Series IDs encontrados: {len(series_ids)}")

                series_path = BASE_PATH / "series"
                if series_ids and series_path.exists():
                    series_df = DeltaTable(str(series_path)).to_pandas()
                    series_df = series_df[series_df['id'].astype(str).isin(series_ids)].copy()
                    logger.info(f"  Series gaming: {len(series_df):,}")

            # 2. Crear schema
            logger.info("\n[2/3] Creando schema gaming en NeonDB...")
            self.create_schema_gaming()

            # 3. Cargar tablas
            logger.info("\n[3/3] Cargando tablas...")
            if not series_df.empty:
                self.load_dim_serie_gaming(series_df)
            if not events_df.empty:
                self.load_dim_evento_gaming(events_df)
                self.load_dim_tag_gaming(events_df)
            self.load_dim_mercado_gaming(df)
            if not events_df.empty:
                self.load_relations_mercado_evento_gaming(df)
            if not events_df.empty:
                self.load_relations_evento_tag_gaming(events_df)
            self.load_fact_metricas_gaming(df)
            self.generate_load_summary()

            logger.info("\n" + "=" * 70)
            logger.info("CARGA COMPLETADA EXITOSAMENTE")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"Error en pipeline gaming: {e}", exc_info=True)
            raise
        finally:
            self.close()

    # ------------------------------------------------------------------
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexion cerrada")


if __name__ == '__main__':
    url = os.getenv('DATABASE_URL')
    if not url:
        logger.error("DATABASE_URL no esta configurada en .env")
        sys.exit(1)

    loader = WarehouseLoader(url)
    loader.connect()
    loader.load_all()

