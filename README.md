# Data Lake & Warehouse - Polymarket Data Pipeline

Pipeline completo de ETL para extracción, transformación y carga de datos de Polymarket a NeonDB (PostgreSQL Cloud). Implementa Arquitectura Medallion (Bronze → Silver → Gold).

## 🏗️ Arquitectura del Proyecto

```
Fase 1: BRONZE (Extracción)          Fase 2: SILVER → GOLD (Warehouse)
┌─────────────────────────┐          ┌──────────────────────────────┐
│  Polymarket API         │          │  Delta Lake (Raw Data)       │
│                         │          │  ├── events/                 │
│  ├── Events             │ ────────→ │  ├── markets/               │
│  ├── Markets            │          │  ├── series/                │
│  ├── Series             │          │  └── tags/                   │
│  └── Tags               │          └──────────────────────────────┘
└─────────────────────────┘                       │
                                                  │ Transformación
                                                  ├─ Normalización
                                                  ├─ Limpieza
                                                  ├─ Deduplicación
                                                  │
                                          ┌───────┴────────┐
                                          │                │
                                          ▼                ▼
                                   Validación      Carga en NeonDB
                                   📊 Integridad   🗄️ Schema Dimensional
```

## 📊 Fases del Pipeline

### Fase 1: BRONZE - Extracción (extractor_polymarket.py)
Extrae datos de la API pública de Polymarket con paralelización:
- **Endpoints**: `/events`, `/markets`, `/series`, `/tags`
- **Concurrencia**: 10 hilos por endpoint
- **Paginación**: Page size 300-500 registros
- **Almacenamiento**: Delta Lake (datalake/raw/)
- **Reporte**: Volumetría y estadísticas en JSON

**Datos extraídos**:
- ~200K eventos
- ~450K mercados
- ~1.1K series
- ~5K tags

### Fase 2A: SILVER - Transformación (DataTransformer)
Normalización y limpieza de datos en memoria antes de carga:

**Transformaciones**:
- ✓ Normalización booleanos (True/'True'/1 → boolean)
- ✓ Normalización números (US/EU/Mixed formats → float)
- ✓ Limpieza strings (trim, normalizar espacios, control)
- ✓ Deserialización JSON (precios, outcomes, tags)
- ✓ Conversión fechas (ISO 8601 → datetime)
- ✓ Deduplicación por ID
- ✓ Validación de tipos de datos

**Métodos principales**:
```python
DataTransformer.normalize_boolean(value)      # True/False
DataTransformer.normalize_numeric(value)      # float
DataTransformer.clean_string(value)           # string normalizado
DataTransformer.normalize_prices(prices_str)  # ['0.45', '0.55'] → [0.45, 0.55]
DataTransformer.normalize_outcomes(outcomes)  # "[' YES', ' NO']" → ['YES', 'NO']
DataTransformer.parse_tags(tags_str)         # "['tag1', 'tag2']" → ['tag1', 'tag2']
DataTransformer.validate_and_clean_events(df) # Pipeline completo eventos
DataTransformer.validate_and_clean_markets(df) # Pipeline completo mercados
```

### Fase 2B: Validación Pre-Carga (WarehouseValidator)
Verifica integridad de datos antes de la carga:
- Existencia de archivos Delta Lake
- Estructura de datos esperada
- Campos requeridos vs opcionales

**Métodos**:
```python
validator.validate_schema()        # Verifica tablas existen
validator.validate_data_integrity() # Integridad referencial
validator.generate_statistics()    # Estadísticas
```

### Fase 2C: GOLD - Carga en NeonDB (WarehouseLoader)
Carga datos a PostgreSQL con modelo dimensional:

**Tablas creadas** (9 total):

**Dimensiones (5)**:
- `dim_date` - Calendario (date_id PK, year, month, day, quarter, day_of_week, is_weekend)
- `dim_event` - Eventos (event_id PK, title, category, ticker, dates, status flags)
- `dim_market` - Mercados (market_id PK, question, type, category, outcomes)
- `dim_series` - Series (series_id PK, slug, title)
- `dim_tag` - Tags únicos (tag_id PK, tag_name UQ)

**Hechos (4)**:
- `fact_market_metrics` - (market_id FK, date_id FK, volume, liquidity, price, spread, etc.)
- `fact_event_metrics` - (event_id FK, date_id FK, active_markets, volume, liquidity)
- `fact_event_tag` - (event_id FK, tag_id FK) - Relación N:N
- `fact_market_event` - (market_id FK, event_id FK) - Relación N:N

**Índices**:
- Categorías (query performance)
- Tickers (búsquedas)
- Tipos de mercado (análisis por tipo)

## 🚀 Ejecución Rápida

### Requisitos Previos
```bash
# 1. Python 3.9+
python --version

# 2. Variables de entorno (.env)
DATABASE_URL=postgresql://user:password@host/database

# 3. Dependencias
uv sync  # o: pip install -r requirements.txt
```

### Ejecución Principal
```bash
# Ejecuta el pipeline completo (Extracción → Transformación → Carga → Validación)
python main.py
```

**Flujo automático**:
1. ✅ Verifica si `datalake/raw` existe
2. ✅ Si NO existe → ejecuta **extractor_polymarket.py**
3. ✅ Ejecuta **DataTransformer** (normalización)
4. ✅ Ejecuta **WarehouseValidator** (validación)
5. ✅ Ejecuta **WarehouseLoader** (carga NeonDB)

### Ejecución Individual

#### Phase 1: Extracción
```bash
python extractor_polymarket.py
# Output: datalake/raw/{events,markets,series,tags}/
#         datalake/volumetry_report.json
```

#### Phase 2A: Transformación (standalone)
```bash
python -c "
from src.utils.transformer_data import DataTransformer
from deltalake import DeltaTable
import pandas as pd

df = DeltaTable('datalake/raw/events').to_pandas()
df_clean = DataTransformer.validate_and_clean_events(df)
print(f'Eventos limpiados: {len(df_clean)} registros')
"
```

#### Phase 2C: Carga (sin validación previa)
```bash
python -c "
from src.warehouse.loader_NeonDB import WarehouseLoader
import os

loader = WarehouseLoader(os.getenv('DATABASE_URL'))
loader.connect()
loader.load_all()
"
```

#### Verificar Conexión NeonDB
```bash
python test_connection.py
```

## 📁 Estructura del Proyecto

```
.
├── main.py                          # 🎯 PUNTO DE ENTRADA (ejecuta todo)
├── extractor_polymarket.py          # Fase 1: Extracción de API
├── test_connection.py               # Verificación de conexión NeonDB
│
├── src/
│   ├── warehouse/
│   │   ├── loader_NeonDB.py        # WarehouseLoader - Carga a PostgreSQL
│   │   └── __init__.py
│   │
│   ├── utils/
│   │   ├── transformer_data.py      # DataTransformer - Normalización
│   │   ├── validator_warehouse.py   # WarehouseValidator - Validación
│   │   └── __init__.py
│   │
│   ├── extractor/                   # (Reservado para fase 1 futura)
│   └── __init__.py
│
├── datalake/
│   ├── raw/                         # Bronze - Datos crudos
│   │   ├── events/                 (Delta Lake)
│   │   ├── markets/                (Delta Lake)
│   │   ├── series/                 (Delta Lake)
│   │   └── tags/                   (Delta Lake)
│   └── volumetry_report.json        # Estadísticas de extracción
│
├── .env                             # Variables de entorno
├── pyproject.toml                   # Dependencias (Python)
├── README.md                        # Este archivo
└── WAREHOUSE_README.md              # Documentación detallada warehouse
```

## 🔧 Configuración

### 1. Variables de Entorno (.env)

```bash
# Conexión a NeonDB (PostgreSQL en la nube)
DATABASE_URL=postgresql://user:password@host:port/database?sslmode=require

# Ejemplo NeonDB real:
DATABASE_URL=postgresql://neondb_owner:npg_abc123@ep-xxx.c-3.region.aws.neon.tech/neondb?sslmode=require&channel_binding=require
```

### 2. Dependencias (pyproject.toml)

```toml
[project]
dependencies = [
    "requests>=2.31.0",           # API HTTP
    "pandas>=2.0.0",              # Datos tabular
    "pyarrow>=14.0.0",            # Columnar
    "deltalake>=0.15.0",          # Delta Lake
    "python-dotenv>=1.0.0",       # .env
    "psycopg2-binary>=2.9.0",     # PostgreSQL
    "sqlalchemy>=2.0.0"           # ORM/queries
]
```

### 3. Instalación

```bash
# Opción 1: Con uv (recomendado)
uv sync

# Opción 2: Con pip
pip install -r requirements.txt

# Opción 3: Manual
pip install requests pandas pyarrow deltalake python-dotenv psycopg2-binary sqlalchemy
```

## 📊 Estadísticas & Monitoreo

### Volumetría
Se genera automáticamente en `datalake/volumetry_report.json`:
```json
{
  "fecha_extraccion": "2024-01-15T10:30:00",
  "resumen": {
    "registros_por_entidad": {
      "events": 200000,
      "markets": 450000,
      "series": 1100,
      "tags": 5000
    },
    "distribucion_markets": {
      "total": 450000,
      "activos": 380000,
      "cerrados": 70000,
      "porcentaje_activos": 84.44
    }
  }
}
```

### Logs del Pipeline
```
2024-01-15 10:30:45 - INFO - FASE 1: EXTRACCIÓN DE DATOS DE POLYMARKET
2024-01-15 10:30:45 - INFO - Iniciando extracción de events con 10 hilos...
2024-01-15 10:31:25 - INFO - events: Extracción completada. Total: 200000 registros
2024-01-15 10:32:15 - INFO - ✓ FASE 1 COMPLETADA
2024-01-15 10:32:16 - INFO - FASE 2A: TRANSFORMACIÓN DE DATOS
2024-01-15 10:32:20 - INFO - Limpiando eventos... Removidos 100 duplicados
2024-01-15 10:32:22 - INFO - Eventos limpiados: 199900 registros válidos
2024-01-15 10:32:45 - INFO - ✓ FASE 2A COMPLETADA
2024-01-15 10:32:46 - INFO - FASE 2C: CARGA EN NEONDB
2024-01-15 10:33:05 - INFO - Conectado a NeonDB exitosamente
2024-01-15 10:33:06 - INFO - Tabla dim_event creada
2024-01-15 10:34:30 - INFO - Cargados 200000 eventos
2024-01-15 10:35:45 - INFO - ✓ VALIDACIÓN COMPLETADA EXITOSAMENTE
2024-01-15 10:36:00 - INFO - PIPELINE COMPLETADO EXITOSAMENTE
```

### Validación Post-Carga
```
=== VALIDACIÓN DE ESQUEMA ===
✓ Dimensión Temporal         (dim_date):               365 registros
✓ Dimensión Eventos          (dim_event):          200000 registros
✓ Dimensión Mercados         (dim_market):         450000 registros
✓ Dimensión Series           (dim_series):            1100 registros
✓ Dimensión Tags             (dim_tag):               5000 registros
✓ Relaciones Event-Tag       (fact_event_tag):    1200000 registros
✓ Relaciones Market-Event    (fact_market_event):  500000 registros
✓ Métricas de Mercados       (fact_market_metrics):450000 registros
✓ Métricas de Eventos        (fact_event_metrics):  200000 registros

=== VALIDACIÓN DE INTEGRIDAD ===
✓ dim_event: 200000 IDs únicos (válido)
✓ dim_market: 450000 IDs únicos (válido)
✓ fact_event_tag: Sin relaciones huérfanas (válido)
✓ fact_market_event: Sin relaciones huérfanas (válido)

=== ESTADÍSTICAS DEL WAREHOUSE ===
Eventos:
  Total: 200,000 registros
  Activos: 150,000
  Cerrados: 50,000
  Categorías únicas: 45
```

## 🎯 Casos de Uso

### Caso 1: Pipeline Completo (Recomendado)
```bash
python main.py
# Ejecuta: Extracción → Transformación → Carga → Validación
# Tiempo estimado: 3-5 minutos
```

### Caso 2: Solo Actualizar Datos del Warehouse
```bash
# Si ya tienes datalake/raw poblado:
python main.py  # Saltará extracción automáticamente
```

### Caso 3: Extracción Incremental
```bash
# Ejecuta solo extractor (mantiene datos previos)
python extractor_polymarket.py
```

### Caso 4: Testing/Debugging
```bash
# Verificar conexión
python test_connection.py

# Verificación manual de transformación
python -c "from src.utils.transformer_data import DataTransformer; ..."

# Verificar NeonDB tiene datos
psql "postgresql://..." -c "SELECT COUNT(*) FROM dim_market;"
```

## ⚠️ Troubleshooting

### Error: DATABASE_URL not found
```
❌ DATABASE_URL no encontrada en .env
```
**Solución**:
```bash
# Crear/verificar .env
echo "DATABASE_URL=postgresql://..." > .env
```

### Error: Connection refused (Polymarket API)
```
Error al obtener events offset 0: Connection refused
```
**Solución**:
```bash
# Verificar conectividad
python -c "import requests; print(requests.get('https://gamma-api.polymarket.com/events').status_code)"
```

### Error: Delta Lake not found
```
FileNotFoundError: datalake/raw/events
```
**Solución**:
```bash
# Ejecutar extracción
python extractor_polymarket.py

# O ejecutar main.py (detecta automáticamente)
python main.py
```

### Error: Connection timeout (NeonDB)
```
psycopg2.OperationalError: timeout expired
```
**Solución**:
```bash
# Verificar acceso a NeonDB
python test_connection.py

# Verificar credentials en .env
# Verificar firewall/IP whitelist en NeonDB console
```

### Error: Schema already exists
```
psycopg2.errors.DuplicateTable: relation "dim_event" already exists
```
**Solución**:
El script `loader_NeonDB.py` elimina automáticamente tablas previas. Si hay conflicto:
```bash
# Conectar a NeonDB y limpiar manualmente
psql "postgresql://..." -c "DROP TABLE IF EXISTS dim_event CASCADE;"
```

## 📈 Performance Tuning

| Fase | Métrica | Valor | Optimización |
|------|---------|-------|--------------|
| Extracción | Tiempo | 2 min | ↑ `threads` en CONFIG |
| Transformación | Memoria | 2GB | ↓ `batch_size` |
| Carga | I/O | 50K/s | ↑ `page_size` en execute_values |
| Validación | Queries | <1s | ✓ Usa índices |

## 🔐 Seguridad

- ✓ DATABASE_URL en .env (git-ignored)
- ✓ SSL mode en NeonDB (sslmode=require)
- ✓ Channel binding (protección MITM)
- ✓ Credenciales no loguean
- ✓ Data sanitization en inputs

## 📚 Recursos

- [Polymarket API Docs](https://docs.polymarket.com/)
- [Delta Lake Format](https://delta.io/)
- [NeonDB Docs](https://neon.tech/docs/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)

## 👨‍💻 Desarrollo y Contribuciones

### Agregar nueva transformación
```python
# En src/utils/transformer_data.py
class DataTransformer:
    @staticmethod
    def normalize_custom(value):
        """Tu transformación"""
        # ...
        return transformed_value
```

### Agregar nueva validación
```python
# En src/utils/validator_warehouse.py
class WarehouseValidator:
    def validate_custom_rule(self):
        """Tu validación"""
        # ...
        return is_valid
```

### Agregar nueva tabla fact/dimension
```python
# En src/warehouse/loader_NeonDB.py
class WarehouseLoader:
    def load_dim_custom(self, data):
        """Tu carga"""
        insert_query = "INSERT INTO dim_custom ..."
        # ...
```

## 📄 Licencia

Proyecto académico - RA2 S3 LinaresJoan

## 📞 Contacto

Joan Linares - [GitHub](https://github.com/JoanLinares)

---

**Última actualización**: Febrero 2026  
**Estado**: Producción ✅  
**Versión**: 2.0 (Fase 1 + Fase 2 completa)

