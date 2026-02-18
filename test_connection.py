"""
Connection Tester - Verifica la conexión a NeonDB
"""
import os
import logging
import psycopg2
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')


def test_connection():
    """Prueba la conexión a la base de datos"""
    if not DATABASE_URL:
        logger.error("DATABASE_URL no configurada en .env")
        return False
    
    try:
        logger.info("Conectando a NeonDB...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Prueba básica
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        logger.info(f"Conectado exitosamente: {db_version[0][:50]}...")
        
        # Verificar extensiones
        cursor.execute("""
            SELECT extname FROM pg_extension 
            ORDER BY extname;
        """)
        extensions = cursor.fetchall()
        logger.info(f"Extensiones disponibles: {', '.join([ext[0] for ext in extensions])}")
        
        cursor.close()
        conn.close()
        logger.info("Conexión de prueba finalizada exitosamente")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return False


if __name__ == "__main__":
    success = test_connection()
    exit(0 if success else 1)
