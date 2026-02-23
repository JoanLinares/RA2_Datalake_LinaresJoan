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
            'dim_fecha':                  'Dimensión Temporal',
            'dim_videojuego':             'Dimensión Videojuegos',
            'dim_serie_gaming':           'Dimensión Series Gaming',
            'dim_evento_gaming':          'Dimensión Eventos Gaming',
            'dim_tag_gaming':             'Dimensión Tags Gaming',
            'dim_mercado_gaming':         'Dimensión Mercados Gaming',
            'fact_mercado_evento_gaming': 'Relaciones Mercado-Evento',
            'fact_evento_tag_gaming':     'Relaciones Evento-Tag',
            'fact_metricas_gaming':       'Métricas Gaming',
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
            'dim_evento_gaming':  'evento_id',
            'dim_mercado_gaming': 'mercado_id',
            'dim_serie_gaming':   'serie_id',
            'dim_tag_gaming':     'tag_id',
            'dim_fecha':          'fecha_id',
            'dim_videojuego':     'videojuego_id',
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
        
        # Validar relaciones evento-tag
        logger.info("\nVerificando relaciones evento-tag:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) AS orphaned
                FROM fact_evento_tag_gaming fet
                LEFT JOIN dim_evento_gaming de ON fet.evento_id = de.evento_id
                LEFT JOIN dim_tag_gaming dt ON fet.tag_id = dt.tag_id
                WHERE de.evento_id IS NULL OR dt.tag_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            if orphaned == 0:
                logger.info("✓ fact_evento_tag_gaming: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_evento_tag_gaming: {orphaned:,} relaciones huérfanas")
                all_valid = False
        except Exception as e:
            logger.error(f"✗ fact_evento_tag_gaming: Error - {e}")

        # Validar relaciones mercado-evento
        logger.info("\nVerificando relaciones mercado-evento:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) AS orphaned
                FROM fact_mercado_evento_gaming fme
                LEFT JOIN dim_mercado_gaming dm ON fme.mercado_id = dm.mercado_id
                LEFT JOIN dim_evento_gaming de ON fme.evento_id = de.evento_id
                WHERE dm.mercado_id IS NULL OR de.evento_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            if orphaned == 0:
                logger.info("✓ fact_mercado_evento_gaming: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_mercado_evento_gaming: {orphaned:,} relaciones huérfanas")
                all_valid = False
        except Exception as e:
            logger.error(f"✗ fact_mercado_evento_gaming: Error - {e}")

        # Validar métricas gaming
        logger.info("\nVerificando fact_metricas_gaming:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) AS orphaned
                FROM fact_metricas_gaming fmg
                LEFT JOIN dim_mercado_gaming dm ON fmg.mercado_id = dm.mercado_id
                LEFT JOIN dim_fecha df ON fmg.fecha_id = df.fecha_id
                WHERE dm.mercado_id IS NULL OR df.fecha_id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            if orphaned == 0:
                logger.info("✓ fact_metricas_gaming: Sin relaciones huérfanas (válido)")
            else:
                logger.warning(f"✗ fact_metricas_gaming: {orphaned:,} relaciones huérfanas")
                all_valid = False
        except Exception as e:
            logger.error(f"✗ fact_metricas_gaming: Error - {e}")
        
        return all_valid
    
    def generate_statistics(self) -> Dict[str, Any]:
        """Genera estadísticas del warehouse"""
        logger.info("\n=== ESTADÍSTICAS DEL WAREHOUSE ===")
        
        stats = {}
        
        # Estadísticas de eventos
        logger.info("\nEventos gaming:")
        try:
            self.cursor.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN es_activo  = true THEN 1 ELSE 0 END) AS activos,
                    SUM(CASE WHEN es_cerrado = true THEN 1 ELSE 0 END) AS cerrados,
                    SUM(CASE WHEN es_destacado = true THEN 1 ELSE 0 END) AS destacados,
                    COUNT(DISTINCT categoria) AS categorias_unicas
                FROM dim_evento_gaming
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
        logger.info("\nMercados gaming:")
        try:
            self.cursor.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN esta_activo  = true THEN 1 ELSE 0 END) AS activos,
                    SUM(CASE WHEN esta_cerrado = true THEN 1 ELSE 0 END) AS cerrados,
                    COUNT(DISTINCT tipo_apuesta) AS tipos_unicos,
                    COUNT(DISTINCT videojuego_id) AS juegos_unicos
                FROM dim_mercado_gaming
            """)
            row = self.cursor.fetchone()
            logger.info(f"  Total: {row[0]:,} registros")
            logger.info(f"  Activos: {row[1]:,}")
            logger.info(f"  Cerrados: {row[2]:,}")
            logger.info(f"  Tipos de apuesta únicos: {row[3]:,}")
            logger.info(f"  Videojuegos únicos: {row[4]:,}")
            stats['markets'] = row
        except Exception as e:
            logger.error(f"Error en estadísticas de mercados: {e}")

        # Estadísticas de relaciones
        logger.info("\nRelaciones:")
        try:
            self.cursor.execute("SELECT COUNT(*) FROM fact_evento_tag_gaming")
            evento_tags = self.cursor.fetchone()[0]
            logger.info(f"  Evento-Tag: {evento_tags:,} relaciones")

            self.cursor.execute("SELECT COUNT(*) FROM fact_mercado_evento_gaming")
            mercado_eventos = self.cursor.fetchone()[0]
            logger.info(f"  Mercado-Evento: {mercado_eventos:,} relaciones")

            stats['relations'] = {'evento_tags': evento_tags, 'mercado_eventos': mercado_eventos}
        except Exception as e:
            logger.error(f"Error en estadísticas de relaciones: {e}")

        # Estadísticas de métricas
        logger.info("\nMétricas:")
        try:
            self.cursor.execute("""
                SELECT COUNT(*),
                       COALESCE(SUM(volumen_total), 0),
                       COALESCE(SUM(liquidez_total), 0)
                FROM fact_metricas_gaming
            """)
            row = self.cursor.fetchone()
            logger.info(f"  Registros métricas: {row[0]:,}")
            logger.info(f"  Volumen total:      ${row[1]:,.2f}")
            logger.info(f"  Liquidez total:     ${row[2]:,.2f}")
            stats['metricas'] = {'total': row[0], 'volumen': row[1], 'liquidez': row[2]}
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

