import pandas as pd
from typing import Dict, List, Any, Optional
from src.config import config


class DataTransformer:
    """
    Clase encargada de transformar y normalizar los datos crudos de Firestore.
    """

    def __init__(self) -> None:
        """
        Inicializa el transformador con la zona horaria configurada.
        """
        self.tz = config.timezone

    def process_data(
        self,
        raw_docs: List[tuple]
    ) -> Dict[str, pd.DataFrame]:
        """
        Coordina la transformación de datos de Servidores y Cámaras.

        :param raw_docs: Lista de tuplas (id, data_dict) desde Firestore.
        :return: Diccionario con DataFrames procesados.
        """
        servers_rows: List[Dict[str, Any]] = []
        cameras_rows: List[Dict[str, Any]] = []

        for doc_id, data in raw_docs:
            # 1. Procesa datos del Servidor
            server_row = self._flatten_server(doc_id, data)
            servers_rows.append(server_row)

            # Extrae timestamp del servidor para propagar a cámaras
            server_query_time = server_row.get("timestamp_query_dt")

            # 2. Procesa datos de Cámaras asociadas
            if "cameras_status" in data and isinstance(data["cameras_status"], dict):
                for cam_name, cam_data in data["cameras_status"].items():
                    if isinstance(cam_data, dict):
                        cam_row = self._flatten_camera(
                            doc_id,
                            cam_name,
                            cam_data
                        )
                        # Propaga timestamp del servidor a la cámara
                        if server_query_time is not None:
                            cam_row["timestamp_query_dt"] = server_query_time
                        
                        cameras_rows.append(cam_row)

        # Genera DataFrames
        df_servers = pd.DataFrame(servers_rows)
        df_cameras = pd.DataFrame(cameras_rows)

        # Aplica filtrado de columnas y ordenamiento según configuración
        df_servers = self._rename_and_filter(
            df_servers,
            config.servers_config["columns"]
        )
        df_cameras = self._rename_and_filter(
            df_cameras,
            config.cameras_config["columns"]
        )

        return {"servers": df_servers, "cameras": df_cameras}

    def _flatten_server(
        self,
        doc_id: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aplana la estructura anidada de un servidor.

        :param doc_id: ID del documento (Nombre del servidor).
        :param data: Diccionario de datos crudos.
        :return: Diccionario aplanado.
        """
        row = {"Server Name": doc_id}
        for k, v in data.items():
            if k == "cameras_status":
                continue
            
            # Aplana sub-diccionario server_stats
            if k == "server_stats" and isinstance(v, dict):
                for sk, sv in v.items():
                    row[f"server_stats_{sk}"] = sv
            else:
                row[k] = v
        
        return self._fix_timestamps(row)

    def _flatten_camera(
        self,
        server_id: str,
        cam_name: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aplana la estructura anidada de una cámara.

        :param server_id: ID del servidor padre.
        :param cam_name: Nombre de la cámara.
        :param data: Datos crudos de la cámara.
        :return: Diccionario aplanado.
        """
        row = {
            "Server Name": server_id,
            "camera_name": cam_name
        }
        row.update(data)
        return self._fix_timestamps(row)

    def _fix_timestamps(
        self,
        row: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normaliza campos de fecha/hora.

        Busca claves con 'timestamp' o 'utc', convierte a datetime,
        ajusta zona horaria y elimina info de zona para compatibilidad.

        :param row: Fila de datos.
        :return: Fila con fechas normalizadas.
        """
        new_row = row.copy()
        for k, v in row.items():
            k_lower = k.lower()
            if "timestamp" in k_lower or "utc" in k_lower:
                try:
                    dt = pd.to_datetime(
                        v,
                        unit='s' if isinstance(v, (int, float)) else None,
                        utc=True,
                        errors='coerce'
                    )
                    
                    if pd.notna(dt):
                        # Genera sufijo _dt si no existe
                        new_key = f"{k}_dt" if "_dt" not in k else k
                        
                        # Convierte TZ y remueve info (naive datetime)
                        converted_dt = dt.tz_convert(self.tz).tz_localize(None)
                        new_row[new_key] = converted_dt
                except Exception:
                    # Si falla la conversión, mantiene el valor original
                    pass
        return new_row

    def _rename_and_filter(
        self,
        df: pd.DataFrame,
        columns_config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Renombra columnas y filtra según configuración.

        :param df: DataFrame original.
        :param columns_config: Configuración de columnas (YAML).
        :return: DataFrame transformado.
        """
        if df.empty:
            return df
        
        rename_map = {k: v["name"] for k, v in columns_config.items()}
        df = df.rename(columns=rename_map)
        
        # Filtra columnas permitidas y asegura orden
        final_cols = [
            v["name"] for k, v in columns_config.items()
            if v["name"] in df.columns
        ]
        
        return df[final_cols]