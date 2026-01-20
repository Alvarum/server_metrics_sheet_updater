"""
Servicio principal de orquestación para Google Sheets.
"""

import time
from typing import Dict, Any, Optional
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1
from src.config import config
from src.services.sheets_styles import STYLES, FORMATS, CONDITION_MAP
from src.services.sheets_dashboard import DashboardBuilder


class SheetsService:
    def __init__(self) -> None:
        gc = gspread.service_account(filename=config.creds_sheets)
        self.sh = gc.open_by_key(config.sheet_id)
        self.dashboard = DashboardBuilder(self.sh)

    def update_snapshot(self, tab_config: Dict[str, Any], df: pd.DataFrame, time_chile: str, time_utc: str) -> None:
        if df.empty: return

        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        tab_name: str = tab_config["tab_name"]
        ws = self._get_or_create_worksheet(tab_name)

        # Pausa larga
        time.sleep(3)
        self._clean_sheet_metadata(ws)
        time.sleep(2)

        # --- CABECERA ESTRICTA (Solo A1:B1 Azul) ---
        ws.merge_cells("A1:B1")
        ws.update("A1", [[tab_config.get("title", "Reporte")]])
        ws.format("A1:B1", STYLES["HEADER_MAIN"])
        
        # Limpieza explícita del resto del encabezado para que quede blanco (Imagen 6)
        ws.format("C1:Z1", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}})

        # Metadata
        ws.update("A2", [["Última actualización (Chile)"], ["Tiempo transcurrido:"]])
        ws.update("B2", [[str(time_chile)]])
        ws.update_acell("B3", "=NOW()-B2")

        ws.format("A2:A3", STYLES["METADATA_LABEL"])
        ws.format("B2:B3", STYLES["METADATA_VALUE"])
        ws.format("B2", FORMATS["DATE_TIME"])
        ws.format("B3", FORMATS["DURATION"])

        time.sleep(2)

        # Datos
        start_row: int = 6
        set_with_dataframe(ws, df, row=start_row, resize=True)

        self._apply_formats_and_styles(ws, df, tab_config, start_row)

    def append_history(self, tab_config: Dict[str, Any], df: pd.DataFrame, time_chile: str) -> None:
        if df.empty: return

        hist_tab_name: Optional[str] = tab_config.get("history_tab")
        if not hist_tab_name: return

        time.sleep(2)

        df_history = df.copy()
        ws = self._get_or_create_worksheet(hist_tab_name)
        check_header = ws.get_values("A1")
        needs_header = not check_header or not check_header[0]

        if needs_header:
            set_with_dataframe(ws, df_history, row=1, include_column_header=True, resize=True)
            ws.format("1:1", STYLES["HEADER_BLUE"])
        else:
            for col in df_history.columns:
                if pd.api.types.is_datetime64_any_dtype(df_history[col]):
                    df_history[col] = df_history[col].dt.strftime("%Y-%m-%d %H:%M:%S")

            df_clean = df_history.where(pd.notnull(df_history), None)
            payload = df_clean.values.tolist()
            ws.append_rows(payload, value_input_option="USER_ENTERED")

    def setup_dashboard(self) -> None:
        # Pausa crítica antes de la operación más pesada
        time.sleep(5)
        self.dashboard.build()

    def _get_or_create_worksheet(self, name: str) -> gspread.Worksheet:
        try:
            return self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title=name, rows=100, cols=20)

    def _clean_sheet_metadata(self, ws: gspread.Worksheet) -> None:
        ws.clear()
        try:
            ws.unmerge_cells(f"A1:Z{ws.row_count}")
        except Exception:
            pass
        
        try:
            meta = self.sh.fetch_sheet_metadata({'includeGridData': False})
            props = next((s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id), None)
            if props and 'conditionalFormats' in props:
                count = len(props['conditionalFormats'])
                requests = [{"deleteConditionalFormatRule": {"sheetId": ws.id, "index": 0}} for _ in range(count)]
                if requests:
                    self.sh.batch_update({'requests': requests})
                    time.sleep(2)
        except Exception as e:
            print(f"Advertencia limpieza reglas: {e}")

    def _apply_formats_and_styles(self, ws: gspread.Worksheet, df: pd.DataFrame, tab_config: Dict[str, Any], start_row: int) -> None:
        cols_conf = tab_config["columns"]
        batch_fmt = []
        name_map = {v["name"]: v for k, v in cols_conf.items()}
        
        for i, col_name in enumerate(df.columns):
            if col_name in name_map:
                fmt = name_map[col_name].get("format", "TEXT")
                rule = FORMATS.get(fmt)
                if rule:
                    col_let = rowcol_to_a1(1, i + 1).replace("1", "")
                    rng = f"{col_let}{start_row+1}:{col_let}{start_row+len(df)}"
                    batch_fmt.append({"range": rng, "format": rule})
        
        if batch_fmt:
            ws.batch_format(batch_fmt)
            time.sleep(2)

        last_col = rowcol_to_a1(1, len(df.columns)).replace("1", "")
        last_row = start_row + len(df)
        batch_styles = [
            {"range": f"A{start_row}:{last_col}{last_row}", "format": STYLES["TABLE_BASE"]},
            {"range": f"A{start_row}:{last_col}{start_row}", "format": STYLES["HEADER_BLUE"]}
        ]
        ws.batch_format(batch_styles)
        time.sleep(2)

        self._apply_conditional_rules(ws, df, cols_conf, start_row)

    def _apply_conditional_rules(self, ws: gspread.Worksheet, df: pd.DataFrame, cols_conf: Dict[str, Any], start_row: int) -> None:
        rules = []
        display_map = {v["name"]: v for k, v in cols_conf.items()}
        row_s = start_row + 1
        row_e = start_row + len(df)

        for i, col_name in enumerate(df.columns):
            conf = display_map.get(col_name)
            if conf and "threshold" in conf:
                th = conf["threshold"]
                op = th.get("operator", "")
                col_let = rowcol_to_a1(1, i + 1).replace("1", "")
                from gspread.utils import a1_range_to_grid_range
                rng = a1_range_to_grid_range(f"{col_let}{row_s}:{col_let}{row_e}", ws.id)

                cond_type = None
                cond_vals = []

                if op == "!=":
                    val_str = f'"{th.get("value")}"' if conf.get("format", "TEXT") == "TEXT" else str(th.get("value"))
                    cond_type = "CUSTOM_FORMULA"
                    cond_vals = [{"userEnteredValue": f'=TRIM({col_let}{row_s})<>{val_str}'}]
                else:
                    gs_type = CONDITION_MAP.get(op)
                    if gs_type:
                        cond_type = gs_type
                        if op in ["between", "not_between"]:
                            cond_vals = [{"userEnteredValue": str(th.get("min"))}, {"userEnteredValue": str(th.get("max"))}]
                        else:
                            cond_vals = [{"userEnteredValue": str(th.get("value"))}]
                
                if cond_type:
                    rule = {"ranges": [rng], "booleanRule": {"condition": {"type": cond_type, "values": cond_vals}, "format": STYLES["ALERT_RED"]}}
                    rules.append({"addConditionalFormatRule": {"rule": rule, "index": 0}})
        
        if rules:
            self.sh.batch_update({"requests": rules})
            time.sleep(2)