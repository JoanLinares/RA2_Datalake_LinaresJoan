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
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

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
        """Crea (o recrea) las 4 tablas del modelo dimensional gaming"""
        try:
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_metricas_gaming CASCADE;
                DROP TABLE IF EXISTS dim_mercado_gaming CASCADE;
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
            # Insertar catálogo fijo de juegos
            juegos = [
                ('DOTA',             'MOBA',     True),
                ('Valorant',         'FPS',      True),
                ('CS:GO',            'FPS',      True),
                ('League of Legends','MOBA',     True),
                ('Fortnite',         'Battle Royale', True),
                ('FIFA',             'Deportes', False),
                ('NBA 2K',           'Deportes', False),
                ('Call of Duty',     'FPS',      True),
                ('Rocket League',    'Deportes', True),
                ('Overwatch',        'FPS',      True),
                ('Hearthstone',      'Cartas',   True),
                ('StarCraft',        'RTS',      True),
                ('Rainbow Six',      'FPS',      True),
                ('Apex Legends',     'Battle Royale', True),
                ('Minecraft',        'Sandbox',  False),
                ('Other Gaming',     'Other',    False),
            ]
            execute_values(
                self.cursor,
                "INSERT INTO dim_videojuego (nombre_juego, genero, es_esports) VALUES %s",
                juegos
            )
            logger.info("  dim_videojuego creada y poblada")

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
            'dim_mercado_gaming':  'Mercados gaming',
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

            # 2. Crear schema
            logger.info("\n[2/3] Creando schema gaming en NeonDB...")
            self.create_schema_gaming()

            # 3. Cargar tablas
            logger.info("\n[3/3] Cargando tablas...")
            self.load_dim_mercado_gaming(df)
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

