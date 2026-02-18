"""
Extractor de datos de Polymarket con almacenamiento en Delta Lake
"""
import requests
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import json
import time
from pathlib import Path
from datetime import datetime
import logging
import traceback

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de la API de Polymarket
BASE_URL = "https://gamma-api.polymarket.com"

# Configuración de paralelización y paginación
CONFIG = {
    "events": {"threads": 10, "page_size": 500},
    "markets": {"threads": 10, "page_size": 500},
    "series": {"threads": 10, "page_size": 300},
    "tags": {"threads": 10, "page_size": 300}
}

# Rutas de almacenamiento
BASE_PATH = Path("datalake/raw")


class PolymarketExtractor:
    """Extractor de datos de Polymarket con paralelización"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.stats = {
            "events": {"total": 0, "active": 0, "closed": 0},
            "markets": {"total": 0, "active": 0, "closed": 0},
            "series": {"total": 0},
            "tags": {"total": 0}
        }
        self.relations = {
            "markets_per_event": {},
            "markets_per_series": {},
            "events_per_tag": {}
        }
    
    def fetch_page(self, endpoint: str, offset: int, limit: int) -> List[Dict]:
        """Obtiene una página de datos del endpoint"""
        url = f"{BASE_URL}/{endpoint}"
        params = {
            "limit": limit,
            "offset": offset
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Manejar diferentes formatos de respuesta
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Algunos endpoints devuelven {"data": [...]}
                if "data" in data:
                    return data["data"]
                elif endpoint in data:
                    return data[endpoint]
                else:
                    return [data]
            return []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener {endpoint} offset {offset}: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON de {endpoint} offset {offset}: {e}")
            return []
    
    def extract_endpoint_parallel(self, endpoint: str, threads: int, page_size: int) -> List[Dict]:
        """Extrae datos de un endpoint usando múltiples hilos"""
        logger.info(f"Iniciando extracción de {endpoint} con {threads} hilos...")
        
        all_data = []
        offset = 0
        batch_num = 0
        
        while True:
            # Crear tareas para procesamiento paralelo
            offsets = [offset + (i * page_size) for i in range(threads)]
            
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = {
                    executor.submit(self.fetch_page, endpoint, off, page_size): off 
                    for off in offsets
                }
                
                batch_data = []
                for future in as_completed(futures):
                    off = futures[future]
                    try:
                        data = future.result()
                        if data:
                            batch_data.extend(data)
                            logger.info(f"{endpoint}: Obtenidos {len(data)} registros desde offset {off}")
                        else:
                            logger.info(f"{endpoint}: No hay más datos desde offset {off}")
                    except Exception as e:
                        logger.error(f"Error procesando offset {off}: {e}")
                
                if not batch_data:
                    break
                
                all_data.extend(batch_data)
                offset += threads * page_size
                batch_num += 1
                
                logger.info(f"{endpoint}: Batch {batch_num} completado. Total acumulado: {len(all_data)}")
                
                # Si obtuvimos menos registros que el máximo posible, hemos llegado al final
                if len(batch_data) < threads * page_size:
                    break
                
                # Pequeña pausa para no saturar la API
                time.sleep(0.5)
        
        logger.info(f"{endpoint}: Extracción completada. Total: {len(all_data)} registros")
        return all_data
    
    def save_to_deltalake(self, data: List[Dict], entity: str):
        """Guarda los datos en formato Delta Lake"""
        if not data:
            logger.warning(f"No hay datos para guardar en {entity}")
            return
        
        try:
            df = pd.DataFrame(data)
            logger.info(f"DataFrame inicial: {len(df)} registros, {len(df.columns)} columnas")
            
            # Eliminar columnas que son completamente null
            null_columns = [col for col in df.columns if df[col].isna().all()]
            if null_columns:
                logger.info(f"Eliminando {len(null_columns)} columnas completamente nulas: {null_columns[:5]}...")
                df = df.drop(columns=null_columns)
            
            # Convertir tipos de datos para compatibilidad con Delta Lake
            for col in df.columns:
                # Verificar si la columna tiene algún valor no nulo
                if df[col].notna().any():
                    # Convertir objetos complejos a JSON strings
                    if df[col].dtype == 'object':
                        try:
                            df[col] = df[col].apply(
                                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else (str(x) if pd.notna(x) else None)
                            )
                        except Exception as e:
                            logger.warning(f"Error convirtiendo columna {col}: {e}")
                            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
                else:
                    # Si no hay valores no nulos, convertir a string vacío
                    df[col] = ""
            
            # Reemplazar NaN/None con valores por defecto según el tipo
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna("")
                elif df[col].dtype in ['int64', 'float64']:
                    df[col] = df[col].fillna(0)
                elif df[col].dtype == 'bool':
                    df[col] = df[col].fillna(False)
            
            path = str(BASE_PATH / entity)
            
            # Crear la carpeta si no existe
            Path(path).mkdir(parents=True, exist_ok=True)
            
            # Crear tabla de PyArrow con schema explícito para evitar tipos null
            schema_fields = []
            for col in df.columns:
                if df[col].dtype == 'object':
                    schema_fields.append(pa.field(col, pa.string()))
                elif df[col].dtype == 'int64':
                    schema_fields.append(pa.field(col, pa.int64()))
                elif df[col].dtype == 'float64':
                    schema_fields.append(pa.field(col, pa.float64()))
                elif df[col].dtype == 'bool':
                    schema_fields.append(pa.field(col, pa.bool_()))
                else:
                    schema_fields.append(pa.field(col, pa.string()))
            
            schema = pa.schema(schema_fields)
            table = pa.Table.from_pandas(df, schema=schema)
            
            # Escribir en Delta Lake
            write_deltalake(
                path,
                table,
                mode="overwrite",
                schema_mode="overwrite"
            )
            
            logger.info(f"✅ Datos de {entity} guardados en Delta Lake: {path}")
            logger.info(f"   - Registros: {len(df)}, Columnas: {len(df.columns)}")
            
            # Verificar que se creó el _delta_log
            delta_log_path = Path(path) / "_delta_log"
            if delta_log_path.exists():
                logger.info(f"   - Delta Log creado correctamente en {delta_log_path}")
            else:
                logger.warning(f"   - ⚠️ Delta Log no encontrado en {delta_log_path}")
            
        except Exception as e:
            logger.error(f"❌ Error al guardar {entity} en Delta Lake: {e}")
            logger.error(f"   Tipo de error: {type(e).__name__}")
            logger.error(f"   Traceback: {traceback.format_exc()}")
            raise  # Re-lanzar la excepción para que se vea el error completo
    
    def analyze_data(self, entity: str, data: List[Dict]):
        """Analiza los datos para generar estadísticas"""
        if not data:
            return
        
        self.stats[entity]["total"] = len(data)
        
        if entity == "markets":
            for market in data:
                status = market.get("closed", False)
                if status:
                    self.stats[entity]["closed"] += 1
                else:
                    self.stats[entity]["active"] += 1
                
                # Analizar relaciones
                event_id = market.get("event_id") or market.get("event") or market.get("eventId")
                if event_id:
                    self.relations["markets_per_event"][str(event_id)] = \
                        self.relations["markets_per_event"].get(str(event_id), 0) + 1
                
                series_id = market.get("series_id") or market.get("series") or market.get("seriesId")
                if series_id:
                    self.relations["markets_per_series"][str(series_id)] = \
                        self.relations["markets_per_series"].get(str(series_id), 0) + 1
        
        elif entity == "events":
            for event in data:
                status = event.get("closed", False) or event.get("active") == False
                if status:
                    self.stats[entity]["closed"] += 1
                else:
                    self.stats[entity]["active"] += 1
                
                # Analizar relaciones con tags
                tags = event.get("tags", []) or event.get("tag", [])
                if isinstance(tags, list):
                    for tag in tags:
                        tag_id = tag if isinstance(tag, str) else tag.get("id")
                        if tag_id:
                            self.relations["events_per_tag"][str(tag_id)] = \
                                self.relations["events_per_tag"].get(str(tag_id), 0) + 1
    
    def generate_volumetry_report(self):
        """Genera el reporte de volumetría"""
        report = {
            "fecha_extraccion": datetime.now().isoformat(),
            "resumen": {
                "registros_por_entidad": {
                    entity: self.stats[entity]["total"]
                    for entity in ["events", "markets", "series", "tags"]
                },
                "distribucion_markets": {
                    "total": self.stats["markets"]["total"],
                    "activos": self.stats["markets"]["active"],
                    "cerrados": self.stats["markets"]["closed"],
                    "porcentaje_activos": round(
                        (self.stats["markets"]["active"] / self.stats["markets"]["total"] * 100)
                        if self.stats["markets"]["total"] > 0 else 0, 2
                    )
                },
                "distribucion_events": {
                    "total": self.stats["events"]["total"],
                    "activos": self.stats["events"]["active"],
                    "cerrados": self.stats["events"]["closed"],
                    "porcentaje_activos": round(
                        (self.stats["events"]["active"] / self.stats["events"]["total"] * 100)
                        if self.stats["events"]["total"] > 0 else 0, 2
                    )
                }
            },
            "analisis_relaciones": {
                "markets_por_evento": {
                    "total_eventos_con_markets": len(self.relations["markets_per_event"]),
                    "promedio_markets_por_evento": round(
                        sum(self.relations["markets_per_event"].values()) / 
                        len(self.relations["markets_per_event"])
                        if self.relations["markets_per_event"] else 0, 2
                    ),
                    "maximo_markets_por_evento": max(
                        self.relations["markets_per_event"].values()
                    ) if self.relations["markets_per_event"] else 0,
                    "eventos_con_mas_markets": sorted(
                        [
                            {"event_id": k, "num_markets": v}
                            for k, v in self.relations["markets_per_event"].items()
                        ],
                        key=lambda x: x["num_markets"],
                        reverse=True
                    )[:10]
                },
                "markets_por_serie": {
                    "total_series_con_markets": len(self.relations["markets_per_series"]),
                    "promedio_markets_por_serie": round(
                        sum(self.relations["markets_per_series"].values()) / 
                        len(self.relations["markets_per_series"])
                        if self.relations["markets_per_series"] else 0, 2
                    ),
                    "series_con_mas_markets": sorted(
                        [
                            {"series_id": k, "num_markets": v}
                            for k, v in self.relations["markets_per_series"].items()
                        ],
                        key=lambda x: x["num_markets"],
                        reverse=True
                    )[:10]
                },
                "events_por_tag": {
                    "total_tags_con_events": len(self.relations["events_per_tag"]),
                    "promedio_events_por_tag": round(
                        sum(self.relations["events_per_tag"].values()) / 
                        len(self.relations["events_per_tag"])
                        if self.relations["events_per_tag"] else 0, 2
                    ),
                    "tags_mas_populares": sorted(
                        [
                            {"tag_id": k, "num_events": v}
                            for k, v in self.relations["events_per_tag"].items()
                        ],
                        key=lambda x: x["num_events"],
                        reverse=True
                    )[:10]
                }
            }
        }
        
        # Guardar reporte en datalake (no en raw)
        report_path = BASE_PATH.parent / "volumetry_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Reporte de volumetría generado: {report_path}")
        return report
    
    def run(self):
        """Ejecuta el proceso completo de extracción"""
        logger.info("="*60)
        logger.info("INICIANDO EXTRACCIÓN DE DATOS DE POLYMARKET")
        logger.info("="*60)
        
        start_time = time.time()
        
        for entity in ["tags", "series", "events", "markets"]:
            logger.info(f"\n{'='*60}")
            logger.info(f"Procesando: {entity.upper()}")
            logger.info(f"{'='*60}")
            
            config = CONFIG[entity]
            data = self.extract_endpoint_parallel(
                entity,
                config["threads"],
                config["page_size"]
            )
            
            if data:
                self.analyze_data(entity, data)
                self.save_to_deltalake(data, entity)
            else:
                logger.warning(f"No se obtuvieron datos de {entity}")
        
        # Generar reporte
        logger.info(f"\n{'='*60}")
        logger.info("GENERANDO REPORTE DE VOLUMETRÍA")
        logger.info(f"{'='*60}")
        report = self.generate_volumetry_report()
        
        elapsed = time.time() - start_time
        logger.info(f"\n{'='*60}")
        logger.info(f"EXTRACCIÓN COMPLETADA EN {elapsed:.2f} segundos")
        logger.info(f"{'='*60}")
        
        # Mostrar resumen
        print("\n📊 RESUMEN DE EXTRACCIÓN:")
        print(f"  - Tags: {self.stats['tags']['total']} registros")
        print(f"  - Series: {self.stats['series']['total']} registros")
        print(f"  - Events: {self.stats['events']['total']} registros "
              f"({self.stats['events']['active']} activos, {self.stats['events']['closed']} cerrados)")
        print(f"  - Markets: {self.stats['markets']['total']} registros "
              f"({self.stats['markets']['active']} activos, {self.stats['markets']['closed']} cerrados)")
        print(f"\n📁 Datos almacenados en: {BASE_PATH.absolute()}")
        print(f"📄 Reporte de volumetría: {BASE_PATH.parent / 'volumetry_report.json'}")


if __name__ == "__main__":
    extractor = PolymarketExtractor()
    extractor.run()
