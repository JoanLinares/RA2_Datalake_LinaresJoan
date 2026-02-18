"""
Data Warehouse Loader - Fase 2
Carga datos desde Delta Lake a PostgreSQL (NeonDB) con modelo dimensional
Ejecuta automáticamente: data_transformer -> carga -> warehouse_validator
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from deltalake import DeltaTable
from ..utils.transformer_data import DataTransformer
from ..utils.validator_warehouse import WarehouseValidator

# Cargar variables de entorno
load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
DATABASE_URL = os.getenv('DATABASE_URL')
BASE_PATH = Path("datalake/raw")


class WarehouseLoader:
    """Cargador de datos hacia el Data Warehouse"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Conecta a la base de datos PostgreSQL"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor()
            logger.info("Conectado a NeonDB exitosamente")
        except psycopg2.Error as e:
            logger.error(f"Error al conectar a la base de datos: {e}")
            raise
    
    def create_schema(self):
        """Crea el esquema dimensional en la base de datos"""
        try:
            # Tabla de Dimensión Temporal
            self.cursor.execute("""
                DROP TABLE IF EXISTS dim_date CASCADE;
                CREATE TABLE dim_date (
                    date_id SERIAL PRIMARY KEY,
                    date DATE NOT NULL UNIQUE,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL,
                    quarter INT NOT NULL,
                    day_of_week INT NOT NULL,
                    is_weekend BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            logger.info("Tabla dim_date creada")
            
            # Tabla de Dimensión Eventos
            self.cursor.execute("""
                DROP TABLE IF EXISTS dim_event CASCADE;
                CREATE TABLE dim_event (
                    event_id VARCHAR(50) PRIMARY KEY,
                    title VARCHAR(2048),
                    description TEXT,
                    category VARCHAR(200),
                    subcategory VARCHAR(200),
                    ticker VARCHAR(500),
                    slug VARCHAR(500),
                    is_active BOOLEAN,
                    is_closed BOOLEAN,
                    is_featured BOOLEAN,
                    creation_date TIMESTAMP,
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    resolution_source VARCHAR(500),
                    series_slug VARCHAR(500),
                    sport VARCHAR(200),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX idx_event_category ON dim_event(category);
                CREATE INDEX idx_event_ticker ON dim_event(ticker);
            """)
            logger.info("Tabla dim_event creada")
            
            # Tabla de Dimensión Series
            self.cursor.execute("""
                DROP TABLE IF EXISTS dim_series CASCADE;
                CREATE TABLE dim_series (
                    series_id VARCHAR(50) PRIMARY KEY,
                    series_slug VARCHAR(500),
                    title VARCHAR(2048),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            logger.info("Tabla dim_series creada")
            
            # Tabla de Dimensión Tags
            self.cursor.execute("""
                DROP TABLE IF EXISTS dim_tag CASCADE;
                CREATE TABLE dim_tag (
                    tag_id SERIAL PRIMARY KEY,
                    tag_name VARCHAR(200) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            logger.info("Tabla dim_tag creada")
            
            # Tabla de relación Event-Tag
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_event_tag CASCADE;
                CREATE TABLE fact_event_tag (
                    event_tag_id SERIAL PRIMARY KEY,
                    event_id VARCHAR(50) NOT NULL,
                    tag_id INT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (event_id) REFERENCES dim_event(event_id),
                    FOREIGN KEY (tag_id) REFERENCES dim_tag(tag_id)
                );
                CREATE INDEX idx_event_tag_event ON fact_event_tag(event_id);
                CREATE INDEX idx_event_tag_tag ON fact_event_tag(tag_id);
            """)
            logger.info("Tabla fact_event_tag creada")
            
            # Tabla de Dimensión Mercado
            self.cursor.execute("""
                DROP TABLE IF EXISTS dim_market CASCADE;
                CREATE TABLE dim_market (
                    market_id VARCHAR(50) PRIMARY KEY,
                    question VARCHAR(2048),
                    market_type VARCHAR(100),
                    slug VARCHAR(500),
                    category VARCHAR(200),
                    subcategory VARCHAR(200),
                    end_date TIMESTAMP,
                    is_active BOOLEAN,
                    is_closed BOOLEAN,
                    is_featured BOOLEAN,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    resolution_source VARCHAR(500),
                    description TEXT,
                    outcomes TEXT,
                    created_at_warehouse TIMESTAMP DEFAULT NOW(),
                    updated_at_warehouse TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX idx_market_category ON dim_market(category);
                CREATE INDEX idx_market_type ON dim_market(market_type);
            """)
            logger.info("Tabla dim_market creada")
            
            # Tabla de relación Market-Event
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_market_event CASCADE;
                CREATE TABLE fact_market_event (
                    market_event_id SERIAL PRIMARY KEY,
                    market_id VARCHAR(50) NOT NULL,
                    event_id VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (market_id) REFERENCES dim_market(market_id),
                    FOREIGN KEY (event_id) REFERENCES dim_event(event_id)
                );
                CREATE INDEX idx_market_event_market ON fact_market_event(market_id);
                CREATE INDEX idx_market_event_event ON fact_market_event(event_id);
            """)
            logger.info("Tabla fact_market_event creada")
            
            # Tabla de Hechos - Métricas de Mercados
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_market_metrics CASCADE;
                CREATE TABLE fact_market_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    market_id VARCHAR(50) NOT NULL,
                    date_id INT NOT NULL,
                    volume NUMERIC(20, 8),
                    volume_24hr NUMERIC(20, 8),
                    volume_1wk NUMERIC(20, 8),
                    volume_1mo NUMERIC(20, 8),
                    volume_1yr NUMERIC(20, 8),
                    liquidity NUMERIC(20, 8),
                    liquidity_amm NUMERIC(20, 8),
                    liquidity_clob NUMERIC(20, 8),
                    last_trade_price NUMERIC(10, 6),
                    best_bid NUMERIC(10, 6),
                    best_ask NUMERIC(10, 6),
                    spread NUMERIC(10, 6),
                    open_interest NUMERIC(20, 8),
                    fee NUMERIC(10, 6),
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (market_id) REFERENCES dim_market(market_id),
                    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
                );
                CREATE INDEX idx_market_metrics_market ON fact_market_metrics(market_id);
                CREATE INDEX idx_market_metrics_date ON fact_market_metrics(date_id);
            """)
            logger.info("Tabla fact_market_metrics creada")
            
            # Tabla de Hechos - Métricas de Eventos
            self.cursor.execute("""
                DROP TABLE IF EXISTS fact_event_metrics CASCADE;
                CREATE TABLE fact_event_metrics (
                    event_metric_id SERIAL PRIMARY KEY,
                    event_id VARCHAR(50) NOT NULL,
                    date_id INT NOT NULL,
                    total_markets INT,
                    active_markets INT,
                    closed_markets INT,
                    total_volume NUMERIC(20, 8),
                    total_liquidity NUMERIC(20, 8),
                    comment_count INT,
                    tweet_count INT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (event_id) REFERENCES dim_event(event_id),
                    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
                );
                CREATE INDEX idx_event_metrics_event ON fact_event_metrics(event_id);
                CREATE INDEX idx_event_metrics_date ON fact_event_metrics(date_id);
            """)
            logger.info("Tabla fact_event_metrics creada")
            
            self.conn.commit()
            logger.info("Schema creado exitosamente")
            
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error al crear schema: {e}")
            raise
    
    def load_dim_tag(self, tags_data: pd.DataFrame):
        """Carga dimensión de Tags"""
        try:
            all_tags = set()
            if 'tags' in tags_data.columns:
                for tags_str in tags_data['tags'].dropna():
                    if isinstance(tags_str, str):
                        try:
                            tags_list = json.loads(tags_str.replace("'", '"'))
                            if isinstance(tags_list, list):
                                all_tags.update(tags_list)
                        except:
                            pass
            
            if not all_tags:
                logger.warning("No se encontraron tags para cargar")
                return
            
            insert_query = "INSERT INTO dim_tag (tag_name) VALUES %s ON CONFLICT (tag_name) DO NOTHING"
            execute_values(
                self.cursor,
                insert_query,
                [(tag,) for tag in sorted(all_tags)],
                page_size=1000
            )
            self.conn.commit()
            logger.info(f"Cargados {len(all_tags)} tags únicos")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar dim_tag: {e}")
    
    def load_dim_event(self, events_data: pd.DataFrame):
        """Carga dimensión de Eventos"""
        try:
            events_data = events_data.copy()
            
            date_cols = ['startDate', 'endDate', 'creationDate', 'createdAt', 'updatedAt']
            for col in date_cols:
                if col in events_data.columns:
                    events_data[col] = pd.to_datetime(events_data[col], errors='coerce')
            
            events_clean = []
            for _, row in events_data.iterrows():
                event = (
                    str(row['id']) if pd.notna(row.get('id')) else None,
                    str(row['title'])[:2048] if pd.notna(row.get('title')) else None,
                    str(row['description'])[:5000] if pd.notna(row.get('description')) else None,
                    str(row['category'])[:200] if pd.notna(row.get('category')) else None,
                    str(row['subcategory'])[:200] if pd.notna(row.get('subcategory')) else None,
                    str(row['ticker'])[:500] if pd.notna(row.get('ticker')) else None,
                    str(row['slug'])[:500] if pd.notna(row.get('slug')) else None,
                    row['active'] if pd.notna(row.get('active')) else False,
                    row['closed'] if pd.notna(row.get('closed')) else False,
                    row['featured'] == 'True' if pd.notna(row.get('featured')) else False,
                    row['creationDate'] if pd.notna(row.get('creationDate')) else None,
                    row['startDate'] if pd.notna(row.get('startDate')) else None,
                    row['endDate'] if pd.notna(row.get('endDate')) else None,
                    str(row['resolutionSource'])[:500] if pd.notna(row.get('resolutionSource')) else None,
                    str(row['seriesSlug'])[:500] if pd.notna(row.get('seriesSlug')) else None,
                    str(row['sport'])[:200] if pd.notna(row.get('sport')) else None,
                )
                if event[0]:
                    events_clean.append(event)
            
            insert_query = """
                INSERT INTO dim_event 
                (event_id, title, description, category, subcategory, ticker, slug, 
                 is_active, is_closed, is_featured, creation_date, start_date, end_date,
                 resolution_source, series_slug, sport)
                VALUES %s
                ON CONFLICT (event_id) DO UPDATE SET updated_at = NOW()
            """
            
            if events_clean:
                execute_values(
                    self.cursor,
                    insert_query,
                    events_clean,
                    page_size=1000
                )
                self.conn.commit()
                logger.info(f"Cargados {len(events_clean)} eventos")
            else:
                logger.warning("No se cargaron eventos")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar dim_event: {e}")
    
    def load_dim_market(self, markets_data: pd.DataFrame):
        """Carga dimensión de Mercados con transformación"""
        try:
            markets_data = DataTransformer.validate_and_clean_markets(markets_data)
            
            markets_clean = []
            for _, row in markets_data.iterrows():
                outcomes_str = None
                if pd.notna(row.get('outcomes_list')):
                    try:
                        outcomes_str = json.dumps(row['outcomes_list'])[:2000]
                    except:
                        pass
                
                market = (
                    str(row['id']) if pd.notna(row.get('id')) else None,
                    str(row['question']) if pd.notna(row.get('question')) else None,
                    str(row.get('marketType', ''))[:100],
                    str(row.get('slug', ''))[:500],
                    str(row.get('category', ''))[:200],
                    str(row.get('subcategory', ''))[:200],
                    row['endDate'] if pd.notna(row.get('endDate')) else None,
                    row['active'] if pd.notna(row.get('active')) else False,
                    row['closed'] if pd.notna(row.get('closed')) else False,
                    row['featured'] == 'True' if isinstance(row.get('featured'), str) else (row['featured'] if pd.notna(row.get('featured')) else False),
                    row['createdAt'] if pd.notna(row.get('createdAt')) else None,
                    row['updatedAt'] if pd.notna(row.get('updatedAt')) else None,
                    str(row.get('resolutionSource', ''))[:500],
                    str(row.get('description', ''))[:5000],
                    outcomes_str,
                )
                if market[0]:
                    markets_clean.append(market)
            
            insert_query = """
                INSERT INTO dim_market 
                (market_id, question, market_type, slug, category, subcategory,
                 end_date, is_active, is_closed, is_featured, created_at, updated_at,
                 resolution_source, description, outcomes)
                VALUES %s
                ON CONFLICT (market_id) DO UPDATE SET updated_at_warehouse = NOW()
            """
            
            if markets_clean:
                execute_values(
                    self.cursor,
                    insert_query,
                    markets_clean,
                    page_size=1000
                )
                self.conn.commit()
                logger.info(f"Cargados {len(markets_clean)} mercados")
            else:
                logger.warning("No se cargaron mercados")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar dim_market: {e}")
            raise
    
    def load_fact_market_metrics(self, markets_data: pd.DataFrame):
        """Carga tabla de hechos con métricas de mercados"""
        try:
            markets_data = markets_data.copy()
            
            numeric_cols = [
                'volume', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr',
                'liquidity', 'liquidityAmm', 'liquidityClob', 'lastTradePrice',
                'bestBid', 'bestAsk', 'spread', 'openInterest', 'fee', 'volumeNum',
                'liquidityNum', 'volume24hrClob', 'volumeAmm', 'volumeClob',
                'volume24hrAmm', 'oneDayPriceChange', 'oneHourPriceChange',
                'oneWeekPriceChange', 'oneMonthPriceChange', 'oneYearPriceChange'
            ]
            
            for col in numeric_cols:
                if col in markets_data.columns:
                    markets_data[col] = pd.to_numeric(markets_data[col], errors='coerce')
            
            markets_data['metric_date'] = pd.to_datetime(
                markets_data.get('updatedAt', datetime.now()), 
                errors='coerce'
            ).dt.date
            
            date_map = self._get_or_create_dates(markets_data['metric_date'].unique())
            
            metrics_clean = []
            for _, row in markets_data.iterrows():
                if pd.notna(row.get('id')) and row['metric_date'] in date_map:
                    metric = (
                        str(row['id']),
                        date_map[row['metric_date']],
                        float(row.get('volume')) if pd.notna(row.get('volume')) else None,
                        float(row.get('volume24hr')) if pd.notna(row.get('volume24hr')) else None,
                        float(row.get('volume1wk')) if pd.notna(row.get('volume1wk')) else None,
                        float(row.get('volume1mo')) if pd.notna(row.get('volume1mo')) else None,
                        float(row.get('volume1yr')) if pd.notna(row.get('volume1yr')) else None,
                        float(row.get('liquidity')) if pd.notna(row.get('liquidity')) else None,
                        float(row.get('liquidityAmm')) if pd.notna(row.get('liquidityAmm')) else None,
                        float(row.get('liquidityClob')) if pd.notna(row.get('liquidityClob')) else None,
                        float(row.get('lastTradePrice')) if pd.notna(row.get('lastTradePrice')) else None,
                        float(row.get('bestBid')) if pd.notna(row.get('bestBid')) else None,
                        float(row.get('bestAsk')) if pd.notna(row.get('bestAsk')) else None,
                        float(row.get('spread')) if pd.notna(row.get('spread')) else None,
                        float(row.get('openInterest')) if pd.notna(row.get('openInterest')) else None,
                        float(row.get('fee')) if pd.notna(row.get('fee')) else None,
                    )
                    metrics_clean.append(metric)
            
            if metrics_clean:
                insert_query = """
                    INSERT INTO fact_market_metrics 
                    (market_id, date_id, volume, volume_24hr, volume_1wk, volume_1mo,
                     volume_1yr, liquidity, liquidity_amm, liquidity_clob, 
                     last_trade_price, best_bid, best_ask, spread, open_interest, fee)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """
                execute_values(
                    self.cursor,
                    insert_query,
                    metrics_clean,
                    page_size=500
                )
                self.conn.commit()
                logger.info(f"Cargadas {len(metrics_clean)} métricas de mercados")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar fact_market_metrics: {e}")
    
    def _get_or_create_dates(self, dates) -> Dict:
        """Obtiene o crea IDs para fechas en la dimensión temporal"""
        date_map = {}
        dates = pd.to_datetime(dates, errors='coerce').dropna()
        dates = dates[dates.notna()].unique()
        
        for date in dates:
            date_obj = pd.Timestamp(date)
            date_only = date_obj.date()
            
            try:
                self.cursor.execute(
                    "SELECT date_id FROM dim_date WHERE date = %s",
                    (date_only,)
                )
                result = self.cursor.fetchone()
                
                if result:
                    date_map[date_only] = result[0]
                else:
                    self.cursor.execute("""
                        INSERT INTO dim_date 
                        (date, year, month, day, quarter, day_of_week, is_weekend)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING date_id
                    """, (
                        date_only,
                        date_obj.year,
                        date_obj.month,
                        date_obj.day,
                        (date_obj.month - 1) // 3 + 1,
                        date_obj.dayofweek,
                        date_obj.dayofweek >= 5
                    ))
                    date_map[date_only] = self.cursor.fetchone()[0]
                    
            except Exception as e:
                logger.error(f"Error procesando fecha {date_only}: {e}")
        
        self.conn.commit()
        return date_map
    
    def load_dim_series(self, series_data: pd.DataFrame):
        """Carga dimensión de Series"""
        try:
            series_data = series_data.copy()
            
            series_clean = []
            for _, row in series_data.iterrows():
                series = (
                    str(row['id']) if pd.notna(row.get('id')) else None,
                    str(row.get('slug', ''))[:500],
                    str(row.get('title', ''))[:2048],
                    str(row.get('description', ''))[:5000],
                )
                if series[0]:
                    series_clean.append(series)
            
            if not series_clean:
                logger.warning("No se cargaron series")
                return
            
            insert_query = """
                INSERT INTO dim_series (series_id, series_slug, title, description)
                VALUES %s
                ON CONFLICT (series_id) DO NOTHING
            """
            
            execute_values(
                self.cursor,
                insert_query,
                series_clean,
                page_size=500
            )
            self.conn.commit()
            logger.info(f"Cargadas {len(series_clean)} series")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar dim_series: {e}")
    
    def load_event_tag_relations(self, events_data: pd.DataFrame):
        """Carga relaciones entre eventos y tags"""
        try:
            relations = DataTransformer.extract_event_tag_relations(events_data)
            
            if not relations:
                logger.warning("No se encontraron relaciones event-tag")
                return
            
            insert_query = """
                INSERT INTO fact_event_tag (event_id, tag_id)
                SELECT %s, tag_id FROM dim_tag WHERE tag_name = %s
                ON CONFLICT DO NOTHING
            """
            
            for event_id, tag_name in relations:
                try:
                    self.cursor.execute(insert_query, (event_id, tag_name))
                except Exception as e:
                    logger.debug(f"Error inserting relation {event_id}-{tag_name}: {e}")
            
            self.conn.commit()
            logger.info(f"Cargadas {len(relations)} relaciones event-tag")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar relaciones event-tag: {e}")
    
    def load_market_event_relations(self, markets_data: pd.DataFrame):
        """Carga relaciones entre mercados y eventos"""
        try:
            relations = []
            
            if 'events' not in markets_data.columns:
                logger.warning("Campo 'events' no encontrado en mercados")
                return
            
            for _, market in markets_data.iterrows():
                market_id = market.get('id')
                if not market_id:
                    continue
                
                events_str = market.get('events')
                if not events_str or pd.isna(events_str):
                    continue
                
                try:
                    events_str = str(events_str).strip()
                    if events_str.startswith('['):
                        events_str = events_str.replace("'", '"')
                        event_ids = json.loads(events_str)
                        for event_id in event_ids:
                            if event_id and pd.notna(event_id):
                                relations.append((str(market_id), str(event_id)))
                except Exception as e:
                    logger.debug(f"Error parseando eventos para mercado {market_id}: {e}")
            
            if not relations:
                logger.warning("No se encontraron relaciones market-event")
                return
            
            insert_query = """
                INSERT INTO fact_market_event (market_id, event_id)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            execute_values(
                self.cursor,
                insert_query,
                relations,
                page_size=1000
            )
            self.conn.commit()
            logger.info(f"Cargadas {len(relations)} relaciones market-event")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error al cargar relaciones market-event: {e}")
    
    def load_all(self):
        """Carga todas las tablas del warehouse"""
        try:
            logger.info("="*70)
            logger.info("INICIANDO CARGA: DATA LAKE -> NEONDB")
            logger.info("="*70)
            
            # PASO 1: Lectura desde Delta Lake
            logger.info("\n[1/4] Leyendo datos desde Delta Lake...")
            
            events_path = BASE_PATH / "events"
            markets_path = BASE_PATH / "markets"
            series_path = BASE_PATH / "series"
            tags_path = BASE_PATH / "tags"
            
            if events_path.exists():
                events_df = DeltaTable(str(events_path)).to_pandas()
                logger.info(f"Leídos {len(events_df)} eventos")
            else:
                events_df = pd.DataFrame()
                logger.warning("No se encontró datos de eventos")
            
            if markets_path.exists():
                markets_df = DeltaTable(str(markets_path)).to_pandas()
                logger.info(f"Leídos {len(markets_df)} mercados")
            else:
                markets_df = pd.DataFrame()
                logger.warning("No se encontró datos de mercados")
            
            if series_path.exists():
                series_df = DeltaTable(str(series_path)).to_pandas()
                logger.info(f"Leídas {len(series_df)} series")
            else:
                series_df = pd.DataFrame()
                logger.warning("No se encontró datos de series")
            
            if tags_path.exists():
                tags_df = DeltaTable(str(tags_path)).to_pandas()
                logger.info(f"Leídas {len(tags_df)} tags")
            else:
                tags_df = pd.DataFrame()
                logger.warning("No se encontró datos de tags")
            
            # PASO 2: Transformación con DataTransformer
            logger.info("\n[2/4] Transformando y limpiando datos...")
            
            if not events_df.empty:
                logger.info("  • Transformando eventos...")
                events_df = DataTransformer.validate_and_clean_events(events_df)
                logger.info(f"    ✓ Eventos procesados: {len(events_df)} registros válidos")
            
            if not markets_df.empty:
                logger.info("  • Transformando mercados...")
                markets_df = DataTransformer.validate_and_clean_markets(markets_df)
                logger.info(f"    ✓ Mercados procesados: {len(markets_df)} registros válidos")
            
            logger.info("  ✓ Transformación completada")
            
            # PASO 3: Validación PRE-CARGA con WarehouseValidator
            logger.info("\n[3/4] Ejecutando validación pre-carga...")
            
            # Validar que tenemos datos mínimos para cargar
            if events_df.empty and markets_df.empty:
                logger.error("  ✗ No hay datos para cargar (eventos y mercados vacíos)")
                raise ValueError("No hay datos para cargar en el warehouse")
            
            # Validar estructura de datos
            logger.info("  • Validando estructura de eventos...")
            if not events_df.empty:
                required_event_cols = ['id', 'title']
                missing_cols = [col for col in required_event_cols if col not in events_df.columns]
                if missing_cols:
                    logger.error(f"  ✗ Faltan columnas requeridas en eventos: {missing_cols}")
                    raise ValueError(f"Columnas faltantes en eventos: {missing_cols}")
                logger.info(f"    ✓ Estructura válida ({len(events_df.columns)} columnas)")
            
            logger.info("  • Validando estructura de mercados...")
            if not markets_df.empty:
                required_market_cols = ['id', 'question']
                missing_cols = [col for col in required_market_cols if col not in markets_df.columns]
                if missing_cols:
                    logger.error(f"  ✗ Faltan columnas requeridas en mercados: {missing_cols}")
                    raise ValueError(f"Columnas faltantes en mercados: {missing_cols}")
                logger.info(f"    ✓ Estructura válida ({len(markets_df.columns)} columnas)")
            
            # Validar conexión a NeonDB antes de cargar
            logger.info("  • Validando conexión a NeonDB...")
            try:
                self.cursor.execute("SELECT version();")
                db_version = self.cursor.fetchone()[0]
                logger.info(f"    ✓ Conexión exitosa: {db_version.split(',')[0]}")
            except Exception as e:
                logger.error(f"  ✗ Error de conexión a NeonDB: {e}")
                raise
            
            logger.info("  ✓ Validación pre-carga completada")
            
            # PASO 4: Carga a NeonDB
            logger.info("\n[4/4] Cargando datos en NeonDB...")
            
            logger.info("  • Creando esquema dimensional...")
            self.create_schema()
            
            logger.info("  • Cargando dimensiones...")
            if not events_df.empty:
                self.load_dim_event(events_df)
                self.load_dim_tag(events_df)
            
            if not series_df.empty:
                self.load_dim_series(series_df)
            
            if not markets_df.empty:
                self.load_dim_market(markets_df)
                logger.info("  • Cargando métricas...")
                self.load_fact_market_metrics(markets_df)
            
            logger.info("  • Cargando relaciones...")
            if not events_df.empty:
                self.load_event_tag_relations(events_df)
            
            if not markets_df.empty:
                self.load_market_event_relations(markets_df)
            
            self.generate_load_summary()
            
            logger.info("\n  ✓ Carga completada en NeonDB")
            
            # Validación POST-CARGA (integridad del warehouse)
            logger.info("\n  • Ejecutando validación post-carga (integridad)...")
            validator = WarehouseValidator(self.database_url)
            validator.connect()
            validator.validate_all()
            
            logger.info("\n" + "="*70)
            logger.info("✓ PROCESO COMPLETADO EXITOSAMENTE")
            logger.info("="*70)
            
        except Exception as e:
            logger.error(f"\n✗ Error durante la carga: {e}")
            raise
        finally:
            self.close()
    
    def generate_load_summary(self):
        """Genera un resumen de los datos cargados"""
        try:
            logger.info("=== RESUMEN DE CARGA ===")
            
            tables = {
                'dim_date': 'Fechas',
                'dim_event': 'Eventos',
                'dim_market': 'Mercados',
                'dim_series': 'Series',
                'dim_tag': 'Tags',
                'fact_event_tag': 'Relaciones Event-Tag',
                'fact_market_event': 'Relaciones Market-Event',
                'fact_market_metrics': 'Métricas de Mercados',
            }
            
            for table, description in tables.items():
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                logger.info(f"{description}: {count:,} registros")
            
        except Exception as e:
            logger.warning(f"No se pudo generar resumen: {e}")
    
    def load_gaming_to_warehouse(self):
        """
        PIPELINE COMPLETO PARA GAMING
        Extrae -> Transforma -> Valida -> Carga
        
        Flujo:
        1. transformer_data.pipeline_complete_gaming() 
           - Extrae gaming del Delta Lake
           - Limpia y transforma
           - Genera resumen
        2. validator_warehouse.validate_all()
           - Valida integridad
        3. NeonDB
           - Carga en tablas dimensionales
        """
        try:
            logger.info("="*100)
            logger.info("PIPELINE COMPLETO GAMING: DATALAKE -> NEONDB")
            logger.info("="*100)
            
            # PASO 1: Extracción + Transformación (todo en transformer_data)
            logger.info("\n[PASO 1/3] EXTRACCION Y TRANSFORMACION DE GAMING")
            logger.info("-" * 100)
            
            df_gaming_clean, gaming_summary = DataTransformer.pipeline_complete_gaming(
                datalake_path="datalake/raw/markets"
            )
            
            if df_gaming_clean.empty:
                logger.error("❌ No se encontraron datos de gaming para cargar")
                return False
            
            logger.info(f"\n✓ Datos de gaming listos para carga:")
            logger.info(f"   Total mercados: {gaming_summary['total_markets']:,}")
            logger.info(f"   Mercados activos: {gaming_summary['active_markets']:,}")
            logger.info(f"   Volumen total: ${gaming_summary['total_volume']:,.2f}")
            
            # PASO 2: Validar esquema y conexión antes de cargar
            logger.info("\n[PASO 2/3] VALIDACION PRE-CARGA")
            logger.info("-" * 100)
            
            # Validar conexión
            logger.info("  • Validando conexión a NeonDB...")
            try:
                self.cursor.execute("SELECT version();")
                db_version = self.cursor.fetchone()[0]
                logger.info(f"    ✓ Conexión exitosa")
            except Exception as e:
                logger.error(f"  ✗ Error de conexión: {e}")
                return False
            
            # Validar estructura de datos
            logger.info("  • Validando estructura de datos...")
            required_cols = ['id', 'question', 'gaming_type', 'bet_type', 'volume']
            missing_cols = [col for col in required_cols if col not in df_gaming_clean.columns]
            
            if missing_cols:
                logger.error(f"  ✗ Faltan columnas: {missing_cols}")
                return False
            
            logger.info(f"    ✓ Estructura válida ({len(df_gaming_clean.columns)} columnas)")
            
            # PASO 3: Crear tablas y cargar datos
            logger.info("\n[PASO 3/3] CARGA EN NEONDB")
            logger.info("-" * 100)
            
            logger.info("  • Creando esquema dimensional...")
            self.create_schema()
            
            logger.info("  • Cargando mercados de gaming...")
            self.load_dim_market(df_gaming_clean)
            
            logger.info("  • Cargando métricas de gaming...")
            self.load_fact_market_metrics(df_gaming_clean)
            
            logger.info("  • Generando resumen de carga...")
            self.generate_load_summary()
            
            logger.info("\n" + "="*100)
            logger.info("✅ CARGA DE GAMING COMPLETADA EXITOSAMENTE")
            logger.info("="*100)
            logger.info(f"\n📊 Resumen final:")
            logger.info(f"   Mercados gaming cargados: {gaming_summary['total_markets']:,}")
            logger.info(f"   Tipos de juego: {len(gaming_summary['gaming_types'])}")
            logger.info(f"   Tipos de apuestas: {len(gaming_summary['bet_types'])}")
            logger.info(f"\n✓ Datos listos para análisis en Tableau")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline gaming: {e}", exc_info=True)
            return False
    
    def close(self):
        """Cierra la conexión a la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Conexión cerrada")

