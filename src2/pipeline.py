import logging
from datetime import datetime
import pytz
from src2.config import config
from src2.services.firestore import FirestoreService
from src2.services.transformer import DataTransformer
from src2.services.sheets import SheetsService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info(">>> Iniciando Pipeline ETL")

    try:
        # 0. Definir TimeStamps del Proceso
        # Usamos la zona horaria definida en config
        tz_chile = pytz.timezone(config.timezone)
        now_utc = datetime.now(pytz.utc)
        now_chile = now_utc.astimezone(tz_chile)

        # Formatos string para el header
        str_chile = now_chile.strftime("%Y-%m-%d %H:%M:%S")
        str_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

        # 1. Extracción
        logger.info("Conectando a Firestore...")
        firestore = FirestoreService()
        raw_docs = list(firestore.get_documents())
        
        if not raw_docs:
            logger.warning("No hay datos. Finalizando.")
            return

        # 2. Transformación
        logger.info("Transformando datos...")
        transformer = DataTransformer()
        datasets = transformer.process_data(raw_docs)

        # 3. Carga (Snapshot + Historial)
        logger.info("Conectando a Google Sheets...")
        sheets = SheetsService()

        # --- SERVIDORES ---
        logger.info("Procesando Servidores...")
        # A. Actualizar hoja principal (Snapshot con Header)
        sheets.update_snapshot(config.servers_config, datasets["servers"], str_chile, str_utc)
        # B. Guardar en Historial
        sheets.append_history(config.servers_config, datasets["servers"], str_chile)

        # --- CÁMARAS ---
        logger.info("Procesando Cámaras...")
        # A. Actualizar hoja principal (Snapshot con Header)
        sheets.update_snapshot(config.cameras_config, datasets["cameras"], str_chile, str_utc)
        # B. Guardar en Historial
        sheets.append_history(config.cameras_config, datasets["cameras"], str_chile)

        logger.info("<<< Pipeline finalizado con éxito.")

    except Exception as e:
        logger.error(f"Error fatal en el pipeline: {e}", exc_info=True)