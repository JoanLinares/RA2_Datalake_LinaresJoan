#!/usr/bin/env python3
"""
S3 Uploader - Sube Delta Lake a Amazon S3
Sube todo el contenido de datalake/ a S3 bucket especificado
"""
import os
import sys
import logging
from pathlib import Path
from typing import List, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()


class S3Uploader:
    """Subidor de archivos a Amazon S3"""
    
    def __init__(self):
        """Inicializa cliente S3 con credenciales desde .env"""
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_REGION', 'eu-west-3')
        self.bucket = os.getenv('S3_BUCKET', 'lasalle-bigdata-2025-2026')
        self.prefix = os.getenv('S3_PREFIX', 'joan_linares/')
        
        # Validar credenciales
        if not self.access_key or not self.secret_key:
            raise ValueError("AWS credentials not found in .env file")
        
        # Crear cliente S3
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            logger.info(f"Cliente S3 inicializado: {self.region}")
        except Exception as e:
            logger.error(f"Error al crear cliente S3: {e}")
            raise
    
    def get_files_to_upload(self, local_path: Path) -> List[Tuple[Path, str]]:
        """
        Obtiene lista de archivos a subir
        Returns: [(local_file_path, s3_key), ...]
        """
        files_to_upload = []
        
        if not local_path.exists():
            logger.warning(f"Ruta no existe: {local_path}")
            return files_to_upload
        
        # Recorrer todos los archivos recursivamente
        for file_path in local_path.rglob('*'):
            if file_path.is_file():
                # Calcular ruta relativa desde datalake/
                relative_path = file_path.relative_to(local_path.parent)
                
                # Crear S3 key con prefijo
                s3_key = f"{self.prefix}{relative_path.as_posix()}"
                
                files_to_upload.append((file_path, s3_key))
        
        return files_to_upload
    
    def upload_file(self, local_file: Path, s3_key: str) -> bool:
        """
        Sube un archivo individual a S3
        Returns: True si éxito, False si error
        """
        try:
            # Determinar content type
            content_type = self._get_content_type(local_file)
            
            # Subir archivo
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.s3_client.upload_file(
                str(local_file),
                self.bucket,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"✓ Subido: {s3_key}")
            return True
            
        except FileNotFoundError:
            logger.error(f"✗ Archivo no encontrado: {local_file}")
            return False
        except NoCredentialsError:
            logger.error("✗ Credenciales AWS no válidas")
            return False
        except ClientError as e:
            logger.error(f"✗ Error al subir {s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Error inesperado al subir {s3_key}: {e}")
            return False
    
    def _get_content_type(self, file_path: Path) -> str:
        """Determina el content type basado en la extensión"""
        extension = file_path.suffix.lower()
        
        content_types = {
            '.parquet': 'application/octet-stream',
            '.json': 'application/json',
            '.csv': 'text/csv',
            '.txt': 'text/plain',
            '.log': 'text/plain',
            '.md': 'text/markdown'
        }
        
        return content_types.get(extension, 'application/octet-stream')
    
    def upload_datalake(self, local_path: str = "datalake") -> bool:
        """
        Sube toda la carpeta datalake a S3
        Returns: True si todo se subió correctamente
        """
        logger.info("="*70)
        logger.info("INICIANDO CARGA A AMAZON S3")
        logger.info("="*70)
        
        local_path = Path(local_path)
        
        # Obtener lista de archivos
        logger.info(f"\n📁 Escaneando: {local_path.absolute()}")
        files_to_upload = self.get_files_to_upload(local_path)
        
        if not files_to_upload:
            logger.warning("⚠️  No se encontraron archivos para subir")
            return False
        
        logger.info(f"📊 Archivos a subir: {len(files_to_upload)}")
        logger.info(f"🪣 Bucket destino: s3://{self.bucket}/{self.prefix}")
        
        # Subir archivos
        logger.info(f"\n⬆️  Subiendo archivos...")
        
        success_count = 0
        error_count = 0
        total_size = 0
        
        for local_file, s3_key in files_to_upload:
            file_size = local_file.stat().st_size
            total_size += file_size
            
            if self.upload_file(local_file, s3_key):
                success_count += 1
            else:
                error_count += 1
        
        # Resumen
        logger.info("\n" + "="*70)
        logger.info("RESUMEN DE CARGA A S3")
        logger.info("="*70)
        logger.info(f"✓ Archivos exitosos: {success_count}")
        logger.info(f"✗ Archivos con error: {error_count}")
        logger.info(f"📦 Tamaño total: {total_size / (1024*1024):.2f} MB")
        logger.info(f"🌐 URL: https://{self.bucket}.s3.{self.region}.amazonaws.com/{self.prefix}")
        logger.info(f"🔗 Console: https://{self.region}.console.aws.amazon.com/s3/buckets/{self.bucket}?prefix={self.prefix}")
        
        if error_count == 0:
            logger.info("\n✅ CARGA COMPLETADA EXITOSAMENTE")
            logger.info("="*70)
            return True
        else:
            logger.warning(f"\n⚠️  CARGA COMPLETADA CON {error_count} ERRORES")
            logger.info("="*70)
            return False
    
    def verify_upload(self) -> bool:
        """Verifica que los archivos existan en S3"""
        try:
            logger.info("\n🔍 Verificando archivos en S3...")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix
            )
            
            if 'Contents' in response:
                file_count = len(response['Contents'])
                total_size = sum(obj['Size'] for obj in response['Contents'])
                
                logger.info(f"✓ Archivos en S3: {file_count}")
                logger.info(f"✓ Tamaño total: {total_size / (1024*1024):.2f} MB")
                
                # Mostrar algunos archivos
                logger.info("\nPrimeros 10 archivos:")
                for obj in response['Contents'][:10]:
                    logger.info(f"  • {obj['Key']} ({obj['Size'] / 1024:.1f} KB)")
                
                return True
            else:
                logger.warning("⚠️  No se encontraron archivos en S3")
                return False
                
        except ClientError as e:
            logger.error(f"✗ Error al verificar S3: {e}")
            return False


def main():
    """Función principal"""
    try:
        logger.info("🚀 S3 Uploader - Delta Lake to Amazon S3\n")
        
        # Crear uploader
        uploader = S3Uploader()
        
        # Subir datalake
        success = uploader.upload_datalake("datalake")
        
        if success:
            # Verificar
            uploader.verify_upload()
            return 0
        else:
            return 1
            
    except ValueError as e:
        logger.error(f"❌ Error de configuración: {e}")
        logger.error("Asegúrate de tener AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en .env")
        return 1
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        logger.exception("Traceback:")
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

