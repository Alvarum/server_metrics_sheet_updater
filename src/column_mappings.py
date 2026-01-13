from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class ColumnMappings:
    """
    Contiene los mapeos de columnas para presentación en Google Sheets.

    :param servers_exact: Mapeo exacto para hoja "servers".
    :type servers_exact: Dict[str, str]
    :param cameras_exact: Mapeo exacto para hoja "cameras".
    :type cameras_exact: Dict[str, str]
    :param prefix_map: Mapeo por prefijo para columnas no exactas.
    :type prefix_map: Dict[str, str]
    """
    servers_exact: Dict[str, str]
    cameras_exact: Dict[str, str]
    prefix_map: Dict[str, str]


def default_column_mappings() -> ColumnMappings:
    """
    Retorna los mapeos por defecto de columnas.

    Aquí SOLO se definen nombres, no lógica.

    :return: ColumnMappings por defecto.
    :rtype: ColumnMappings
    """
    servers_exact: Dict[str, str] = {
        "Server Name": "Servidor",
        "Timestamp Query": "Timestamp Consulta (Chile)",

        "bank_id": "Banco",
        "bank_ip": "IP Banco",
        "bank_num": "Número Banco",
        "bank_utc": "UTC Banco",

        "active_cameras_count": "Cámaras activas",
        "operating_system": "Sistema Operativo",
        "raptor_version": "Versión Raptor",
        "process_duration_sec": "Duración Proceso (s)",

        "server_stats_cpu_usage_pct": "Uso CPU (%)",
        "server_stats_ram_usage_pct": "Uso RAM (%)",
        "server_stats_disk_usage_pct": "Uso Disco (%)",
        "server_stats_cpu_temperature_celsius": "Temperatura CPU (°C)",
        "server_stats_network_speed_recieve_kbps": "RX (kbps)",
        "server_stats_network_speed_send_kbps": "TX (kbps)",
        "server_stats_status_server": "Estado Servidor",
        "server_stats_timestamp_boot": "Boot Sistema (Chile)",
        "server_stats_uptime_days": "Uptime (días)",
    }

    cameras_exact: Dict[str, str] = {
        "Server Name": "Servidor",
        "camera_name": "Cámara",
        "camera_ip": "IP Cámara",
        "last_image_age_min": "Edad última imagen (min)",
        "rotation_duration_min": "Duración rotación (min)",
        "status_all_images": "Estado imágenes",
        "timestamp_last_image": "Epoch última imagen",
        "timestamp_last_image_dt": "Última imagen (Chile)",
    }

    prefix_map: Dict[str, str] = {
        "server_stats_": "Server · ",
        "raptor_running_": "Raptor · ",
    }

    return ColumnMappings(
        servers_exact=servers_exact,
        cameras_exact=cameras_exact,
        prefix_map=prefix_map,
    )
