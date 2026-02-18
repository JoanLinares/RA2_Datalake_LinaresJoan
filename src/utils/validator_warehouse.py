"""
Warehouse Validator - Fase 2
Validación de integridad y estadísticas del warehouse
Verifica: esquema, relaciones, integridad referencial, estadísticas
"""
import logging
from typing import Dict, List, Any
import psycopg2

logger = logging.getLogger(__name__)


class WarehouseValidator:
    """Validador de integridad del warehouse"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Conecta a la base de datos"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor()
            logger.info("Validador conectado a NeonDB")
        except psycopg2.Error as e:
            logger.error(f"Error de conexión: {e}")
            raise
    
    def validate_schema(self) -> Dict[str, int]:
        """Valida que todas las tablas esperadas existan"""
        logger.info("\n=== VALIDACIÓN DE ESQUEMA ===")
        
        expected_tables = {
            'dim_date': 'Dimensión Temporal',
            'dim_event': 'Dimensión Eventos',
            'dim_market': 'Dimensión Mercados',
            'dim_series': 'Dimensión Series',
            'dim_tag': 'Dimensión Tags',
            'fact_event_tag': 'Relaciones Event-Tag',
            'fact_market_event': 'Relaciones Market-Event',
            'fact_market_metrics': 'Métricas de Mercados',
            'fact_event_metrics': 'Métricas de Eventos'
        }
        
        table_counts = {}
        
        for table, description in expected_tables.items():
            try:
                self.cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=%s)",
                    (table,)
                )
                exists = self.cursor.fetchone()[0]
                
                if exists:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    table_counts[table] = count
                    logger.info(f"✓ {description:30} ({table}): {count:,} registros")
                else:
                    logger.warning(f"✗ {description:30} ({table}): NO EXISTE")
                    table_counts[table] = 0
            
            except Exception as e:
                logger.error(f"✗ {description:30}: Error - {e}")
                table_counts[table] = 0
        
        return table_counts
    
    def validate_data_integrity(self) -> bool:
        """Valida integridad referencial y restricciones"""
        logger.info("\n=== VALIDACIÓN DE INTEGRIDAD ===")
        
        all_valid = True
        
        # Validar IDs únicos en dimensiones
        unique_id_checks = {
            'dim_event': 'event_id',
            'dim_market': 'market_id',
            'dim_series': 'series_id',
            'dim_tag': 'tag_id',
            'dim_date': 'date_id'
        }
        
        logger.info("\nVerificando ID únicos:")
        for table, id_col in unique_id_checks.items():
            try:
                self.cursor.execute(f"""
                    SELECT COUNT(DISTINCT {id_col}), COUNT(*)
                    FROM {table}
                """)
                distinct, total = self.cursor.fetchone()
                
                if distinct == total:
                    logger.info(f"✓ {table}: {distinct:,} IDs únicos (válido)")
                else:
                    logger.warning(f"✗ {table}: {total:,} registros pero solo {distinct:,} IDs únicos")
                    all_valid = False
            
            except Exception as e:
                logger.error(f"✗ {table}: Error - {e}")
                all_valid = False
        
        # Validar relaciones event-tag
        logger.info("\nVerificando relaciones event-tag:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as orphaned
                FROM fact_event_tag fet
                LEFT JOIN dim_event de ON fet.event_id = de.event_id
                LEFT JOIN dim_tag dt ON fet.tag_id = dt.tag_id
                WHERE de.event_id IS NULL OR dt.tag_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            
            if orphaned == 0:
                logger.info("✓ fact_event_tag: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_event_tag: {orphaned:,} relaciones huérfanas")
                all_valid = False
        
        except Exception as e:
            logger.error(f"✗ fact_event_tag: Error - {e}")
        
        # Validar relaciones market-event
        logger.info("\nVerificando relaciones market-event:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as orphaned
                FROM fact_market_event fme
                LEFT JOIN dim_market dm ON fme.market_id = dm.market_id
                LEFT JOIN dim_event de ON fme.event_id = de.event_id
                WHERE dm.market_id IS NULL OR de.event_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            
            if orphaned == 0:
                logger.info("✓ fact_market_event: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_market_event: {orphaned:,} relaciones huérfanas")
                all_valid = False
        
        except Exception as e:
            logger.error(f"✗ fact_market_event: Error - {e}")
        
        # Validar métricas de mercados
        logger.info("\nVerificando fact_market_metrics:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as orphaned
                FROM fact_market_metrics fmm
                LEFT JOIN dim_market dm ON fmm.market_id = dm.market_id
                LEFT JOIN dim_date dd ON fmm.date_id = dd.date_id
                WHERE dm.market_id IS NULL OR dd.date_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            
            if orphaned == 0:
                logger.info("✓ fact_market_metrics: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_market_metrics: {orphaned:,} relaciones huérfanas")
                all_valid = False
        
        except Exception as e:
            logger.error(f"✗ fact_market_metrics: Error - {e}")
        
        return all_valid
    
    def generate_statistics(self) -> Dict[str, Any]:
        """Genera estadísticas del warehouse"""
        logger.info("\n=== ESTADÍSTICAS DEL WAREHOUSE ===")
        
        stats = {}
        
        # Estadísticas de eventos
        logger.info("\nEventos:")
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as activos,
                    SUM(CASE WHEN is_closed = true THEN 1 ELSE 0 END) as cerrados,
                    SUM(CASE WHEN is_featured = true THEN 1 ELSE 0 END) as destacados,
                    COUNT(DISTINCT category) as categorías_únicas
                FROM dim_event
            """)
            row = self.cursor.fetchone()
            logger.info(f"  Total: {row[0]:,} registros")
            logger.info(f"  Activos: {row[1]:,}")
            logger.info(f"  Cerrados: {row[2]:,}")
            logger.info(f"  Destacados: {row[3]:,}")
            logger.info(f"  Categorías únicas: {row[4]:,}")
            stats['events'] = row
        
        except Exception as e:
            logger.error(f"Error en estadísticas de eventos: {e}")
        
        # Estadísticas de mercados
        logger.info("\nMercados:")
        try:
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as activos,
                    SUM(CASE WHEN is_closed = true THEN 1 ELSE 0 END) as cerrados,
                    SUM(CASE WHEN is_featured = true THEN 1 ELSE 0 END) as destacados,
                    COUNT(DISTINCT category) as categorías_únicas,
                    COUNT(DISTINCT market_type) as tipos_únicos
                FROM dim_market
            """)
            row = self.cursor.fetchone()
            logger.info(f"  Total: {row[0]:,} registros")
            logger.info(f"  Activos: {row[1]:,}")
            logger.info(f"  Cerrados: {row[2]:,}")
            logger.info(f"  Destacados: {row[3]:,}")
            logger.info(f"  Categorías únicas: {row[4]:,}")
            logger.info(f"  Tipos únicos: {row[5]:,}")
            stats['markets'] = row
        
        except Exception as e:
            logger.error(f"Error en estadísticas de mercados: {e}")
        
        # Estadísticas de relaciones
        logger.info("\nRelaciones:")
        try:
            self.cursor.execute("SELECT COUNT(*) FROM fact_event_tag")
            event_tags = self.cursor.fetchone()[0]
            logger.info(f"  Event-Tag: {event_tags:,} relaciones")
            
            self.cursor.execute("SELECT COUNT(*) FROM fact_market_event")
            market_events = self.cursor.fetchone()[0]
            logger.info(f"  Market-Event: {market_events:,} relaciones")
            
            stats['relations'] = {
                'event_tags': event_tags,
                'market_events': market_events
            }
        
        except Exception as e:
            logger.error(f"Error en estadísticas de relaciones: {e}")
        
        # Estadísticas de métricas
        logger.info("\nMétricas:")
        try:
            self.cursor.execute("SELECT COUNT(*) FROM fact_market_metrics")
            market_metrics = self.cursor.fetchone()[0]
            logger.info(f"  Market Metrics: {market_metrics:,} registros")
            
            self.cursor.execute("SELECT COUNT(*) FROM fact_event_metrics")
            event_metrics = self.cursor.fetchone()[0]
            logger.info(f"  Event Metrics: {event_metrics:,} registros")
            
            stats['metrics'] = {
                'market_metrics': market_metrics,
                'event_metrics': event_metrics
            }
        
        except Exception as e:
            logger.error(f"Error en estadísticas de métricas: {e}")
        
        return stats
    
    def validate_all(self) -> bool:
        """Ejecuta toda la validación"""
        try:
            logger.info("="*70)
            logger.info("INICIANDO VALIDACIÓN DEL WAREHOUSE")
            logger.info("="*70)
            
            # Validar schema
            self.validate_schema()
            
            # Validar integridad
            integrity_valid = self.validate_data_integrity()
            
            # Generar estadísticas
            self.generate_statistics()
            
            logger.info("\n" + "="*70)
            if integrity_valid:
                logger.info("✓ VALIDACIÓN COMPLETADA EXITOSAMENTE")
            else:
                logger.warning("⚠ VALIDACIÓN COMPLETADA CON ADVERTENCIAS")
            logger.info("="*70)
            
            return integrity_valid
        
        except Exception as e:
            logger.error(f"Error durante validación: {e}")
            return False
        
        finally:
            self.close()
    
    def close(self):
        """Cierra la conexión"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

