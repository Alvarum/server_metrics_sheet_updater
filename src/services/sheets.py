"""
Servicio encargado de la interacción con la API de Google Sheets.

Este módulo gestiona la escritura de datos, aplicación de formatos,
estilos visuales y reglas de formato condicional.
"""

from typing import Dict, List, Any, Optional
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1, a1_range_to_grid_range
from src.config import config


class SheetsService:
    """
    Clase para gestionar la conexión y manipulación de hojas de cálculo.
    """

    # Define formatos de número para la API de Sheets
    FORMATS: Dict[str, Dict[str, Any]] = {
        "NUMBER": {
            "numberFormat": {"type": "NUMBER", "pattern": "0.0"}
        },
        "INTEGER": {
            "numberFormat": {"type": "NUMBER", "pattern": "0"}
        },
        "PERCENT": {
            "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}
        },
        "DATE_TIME": {
            "numberFormat": {
                "type": "DATE_TIME",
                "pattern": "yyyy-mm-dd hh:mm:ss"
            }
        },
        "TEXT": {
            "numberFormat": {"type": "TEXT"}
        },
        "DURATION": {
            "numberFormat": {"type": "TIME", "pattern": "[h]:mm:ss"}
        }
    }

    # Define estilos visuales (colores, fuentes, bordes)
    STYLES: Dict[str, Dict[str, Any]] = {
        "HEADER_MAIN": {
            "backgroundColor": {"red": 0.258, "green": 0.52, "blue": 0.956},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True,
                "fontSize": 12
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE"
        },
        "METADATA_LABEL": {
            "backgroundColor": {"red": 0.258, "green": 0.52, "blue": 0.956},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True,
                "fontSize": 10
            },
            "horizontalAlignment": "RIGHT"
        },
        "HEADER_BLUE": {
            "backgroundColor": {"red": 0.258, "green": 0.52, "blue": 0.956},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True,
                "fontSize": 10
            },
            "horizontalAlignment": "CENTER"
        },
        "COLUMN_YELLOW": {
            "backgroundColor": {"red": 0.98, "green": 0.73, "blue": 0.01},
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0},
                "bold": True
            },
            "verticalAlignment": "MIDDLE",
            "horizontalAlignment": "CENTER"
        },
        "TABLE_BASE": {
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            },
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0}
            }
        },
        "METADATA_VALUE": {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            },
            "textFormat": {
                "foregroundColor": {"red": 0, "green": 0, "blue": 0}
            },
            "horizontalAlignment": "CENTER"
        },
        "ALERT_RED": {
            "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
            "textFormat": {
                "foregroundColor": {"red": 0.8, "green": 0.0, "blue": 0.0},
                "bold": True
            }
        }
    }

    CONDITION_MAP: Dict[str, str] = {
        ">": "NUMBER_GREATER",
        ">=": "NUMBER_GREATER_THAN_EQ",
        "<": "NUMBER_LESS",
        "<=": "NUMBER_LESS_THAN_EQ",
        "==": "TEXT_EQ",
        "between": "NUMBER_BETWEEN",
        "not_between": "NUMBER_NOT_BETWEEN"
    }

    def __init__(self) -> None:
        """
        Inicializa la conexión con Google Sheets.
        """
        gc = gspread.service_account(filename=config.creds_sheets)
        self.sh = gc.open_by_key(config.sheet_id)

    def update_snapshot(
        self,
        tab_config: Dict[str, Any],
        df: pd.DataFrame,
        time_chile: str,
        time_utc: str
    ) -> None:
        """
        Actualiza la hoja principal con los datos actuales.

        Realiza limpieza profunda, configura encabezados, escribe datos
        y aplica estilos condicionales.

        :param tab_config: Configuración de la pestaña (servidores/cámaras).
        :param df: DataFrame con los datos a escribir.
        :param time_chile: String de fecha/hora en Chile.
        :param time_utc: String de fecha/hora en UTC.
        """
        if df.empty:
            return

        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        tab_name: str = tab_config["tab_name"]
        ws: gspread.Worksheet = self._get_or_create_worksheet(tab_name)

        self._clean_sheet_metadata(ws)

        ws.merge_cells("A1:B1")
        ws.update("A1", [[tab_config.get("title", "Reporte")]])
        ws.format("A1", self.STYLES["HEADER_MAIN"])

        ws.update(
            "A2",
            [["Última actualización (Chile)"], ["Tiempo transcurrido:"]]
        )
        ws.update("B2", [[str(time_chile)]])
        ws.update_acell("B3", '=NOW()-B2')

        ws.format("A2:A3", self.STYLES["METADATA_LABEL"])
        ws.format("B2:B3", self.STYLES["METADATA_VALUE"])
        ws.format("B2", self.FORMATS["DATE_TIME"])
        ws.format("B3", self.FORMATS["DURATION"])

        start_row: int = 6
        set_with_dataframe(
            ws,
            df,
            row=start_row,
            resize=True
        )

        self._apply_data_formats(ws, df, tab_config["columns"], start_row)
        self._apply_visual_styles(ws, df, tab_config, start_row)
        self._apply_conditional_rules(ws, df, tab_config["columns"], start_row)

    def append_history(
        self,
        tab_config: Dict[str, Any],
        df: pd.DataFrame,
        time_chile: str
    ) -> None:
        """
        Agrega datos al final de la hoja de historial.

        Convierte timestamps a string para evitar errores de serialización JSON.

        :param tab_config: Configuración de la pestaña.
        :param df: DataFrame con los datos actuales.
        :param time_chile: Timestamp de ejecución.
        """
        if df.empty:
            return

        hist_tab_name: Optional[str] = tab_config.get("history_tab")
        if not hist_tab_name:
            return

        # Copia para no mutar el original
        df_history = df.copy()

        ws: gspread.Worksheet = self._get_or_create_worksheet(hist_tab_name)
        check_header = ws.get_values("A1")
        needs_header = not check_header or not check_header[0]

        if needs_header:
            set_with_dataframe(
                ws,
                df_history,
                row=1,
                include_column_header=True,
                resize=True
            )
            ws.format("1:1", self.STYLES["HEADER_BLUE"])
        else:
            # --- CORRECCIÓN JSON SERIALIZABLE ---
            # Convierte columnas datetime a string explícito
            for col in df_history.columns:
                if pd.api.types.is_datetime64_any_dtype(df_history[col]):
                    # Formato compatible con Google Sheets
                    df_history[col] = df_history[col].dt.strftime(
                        '%Y-%m-%d %H:%M:%S'
                    )

            # Reemplaza NaN/NaT por None para que JSON envíe null
            df_clean = df_history.where(pd.notnull(df_history), None)
            
            payload = df_clean.values.tolist()
            
            # value_input_option='USER_ENTERED' re-interpreta el string
            # '2026-01-20 12:00:00' como fecha válida en el Sheet.
            ws.append_rows(payload, value_input_option='USER_ENTERED')

    def _get_or_create_worksheet(self, name: str) -> gspread.Worksheet:
        """
        Obtiene o crea una hoja de trabajo.
        """
        try:
            return self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(
                title=name,
                rows=100,
                cols=20
            )

    def _clean_sheet_metadata(self, ws: gspread.Worksheet) -> None:
        """
        Limpia contenido, merges y reglas condicionales.
        """
        ws.clear()
        try:
            ws.unmerge_cells(f"A1:Z{ws.row_count}")
        except Exception:
            pass
        
        try:
            meta = self.sh.fetch_sheet_metadata({'includeGridData': False})
            sheet_meta = next(
                (s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id),
                None
            )

            if sheet_meta and 'conditionalFormats' in sheet_meta:
                rules_count = len(sheet_meta['conditionalFormats'])
                requests = [
                    {
                        "deleteConditionalFormatRule": {
                            "sheetId": ws.id,
                            "index": 0
                        }
                    }
                    for _ in range(rules_count)
                ]
                if requests:
                    self.sh.batch_update({'requests': requests})
        except Exception as e:
            print(f"Advertencia: No se pudieron limpiar reglas antiguas: {e}")

    def _apply_data_formats(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        columns_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        """
        Aplica formatos numéricos y de fecha a las columnas.
        """
        batch: List[Dict[str, Any]] = []
        final_name_map = {v["name"]: v for k, v in columns_config.items()}

        for i, col_name in enumerate(df.columns):
            if col_name in final_name_map:
                fmt_type = final_name_map[col_name].get("format", "TEXT")
                rule = self.FORMATS.get(fmt_type)

                if rule:
                    col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                    data_start = start_row_idx + 1
                    rng = (
                        f"{col_letter}{data_start}:"
                        f"{col_letter}{data_start + len(df)}"
                    )
                    batch.append({"range": rng, "format": rule})
        
        if batch:
            ws.batch_format(batch)

    def _apply_visual_styles(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        tab_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        """
        Aplica estilos visuales base.
        """
        batch_styles: List[Dict[str, Any]] = []
        last_col_idx = len(df.columns)
        last_row_idx = start_row_idx + len(df)
        last_col_letter = rowcol_to_a1(1, last_col_idx).replace("1", "")

        full_table_range = f"A{start_row_idx}:{last_col_letter}{last_row_idx}"
        batch_styles.append({
            "range": full_table_range,
            "format": self.STYLES["TABLE_BASE"]
        })

        header_range = f"A{start_row_idx}:{last_col_letter}{start_row_idx}"
        batch_styles.append({
            "range": header_range,
            "format": self.STYLES["HEADER_BLUE"]
        })

        if batch_styles:
            ws.batch_format(batch_styles)

    def _apply_conditional_rules(
        self,
        ws: gspread.Worksheet,
        df: pd.DataFrame,
        columns_config: Dict[str, Any],
        start_row_idx: int
    ) -> None:
        """
        Aplica reglas de formato condicional.
        """
        rules: List[Dict[str, Any]] = []
        display_to_config = {v["name"]: v for k, v in columns_config.items()}

        row_start = start_row_idx + 1
        row_end = start_row_idx + len(df)

        for i, col_name in enumerate(df.columns):
            conf = display_to_config.get(col_name)

            if conf and "threshold" in conf:
                threshold: Dict[str, Any] = conf["threshold"]
                operator: str = threshold.get("operator", "")
                
                col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                range_a1 = f"{col_letter}{row_start}:{col_letter}{row_end}"
                grid_range = a1_range_to_grid_range(range_a1, ws.id)

                condition_type: Optional[str] = None
                condition_values: List[Dict[str, str]] = []

                if operator == "!=":
                    val = threshold.get("value")
                    fmt = conf.get("format", "TEXT")
                    val_str = f'"{val}"' if fmt == "TEXT" else str(val)
                    formula = f'=TRIM({col_letter}{row_start})<>{val_str}'
                    
                    condition_type = "CUSTOM_FORMULA"
                    condition_values = [{"userEnteredValue": formula}]
                else:
                    gs_type = self.CONDITION_MAP.get(operator)
                    if gs_type:
                        condition_type = gs_type
                        if operator in ["between", "not_between"]:
                            condition_values = [
                                {"userEnteredValue": str(threshold.get("min"))},
                                {"userEnteredValue": str(threshold.get("max"))}
                            ]
                        else:
                            condition_values = [
                                {"userEnteredValue": str(threshold.get("value"))}
                            ]

                if condition_type:
                    rule = {
                        "ranges": [grid_range],
                        "booleanRule": {
                            "condition": {
                                "type": condition_type,
                                "values": condition_values
                            },
                            "format": self.STYLES["ALERT_RED"]
                        }
                    }
                    rules.append({
                        "addConditionalFormatRule": {
                            "rule": rule,
                            "index": 0
                        }
                    })

        if rules:
            self.sh.batch_update({"requests": rules})