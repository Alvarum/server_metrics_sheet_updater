import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from gspread.utils import rowcol_to_a1
from src.config import config

class SheetsService:
    # --- 1. Definición de Formatos de Datos ---
    FORMATS = {
        "NUMBER": {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}},
        "PERCENT": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}},
        "DATE_TIME": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},
        "TEXT": {"numberFormat": {"type": "TEXT"}},
    }

    # --- 2. Definición de Estilos Visuales ---
    STYLES = {
        # Azul Google: Fondo Azul, Letra Blanca
        "HEADER_BLUE": {
            "backgroundColor": {"red": 0.258, "green": 0.52, "blue": 0.956},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        },
        # Amarillo: Fondo Amarillo, Letra Negra
        "COLUMN_YELLOW": {
            "backgroundColor": {"red": 0.98, "green": 0.73, "blue": 0.01},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "bold": True},
            "verticalAlignment": "MIDDLE",
            "horizontalAlignment": "CENTER"
        },
        # BASE: Bordes negros Y LETRA NEGRA (Esto arregla el texto invisible)
        "TABLE_BASE": {
            "borders": {
                "top": {"style": "SOLID"},
                "bottom": {"style": "SOLID"},
                "left": {"style": "SOLID"},
                "right": {"style": "SOLID"}
            },
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}} # <--- ESTO ES LA CLAVE
        }
    }

    def __init__(self):
        gc = gspread.service_account(filename=config.creds_sheets)
        self.sh = gc.open_by_key(config.sheet_id)

    def update_snapshot(self, tab_config: dict, df: pd.DataFrame, time_chile, time_utc):
        if df.empty: return
        
        tab_name = tab_config["tab_name"]
        ws = self._get_or_create_worksheet(tab_name)
        ws.clear()

        # A. Metadatos (Filas 1-3)
        header_data = [
            ["Título", tab_config.get("title", "Reporte")],
            ["Última actualización (Chile)", str(time_chile)],
            ["Última actualización (UTC)", str(time_utc)]
        ]
        ws.update("A1:B3", header_data)
        ws.format("A1:A3", self.STYLES["HEADER_BLUE"])
        ws.format("B1:B3", self.STYLES["TABLE_BASE"]) # Aplicamos borde y texto negro

        # B. Datos (Fila 6)
        start_row = 6
        set_with_dataframe(ws, df, row=start_row, resize=True)

        # C. Formatos de Datos
        self._apply_data_formats(ws, df, tab_config["columns"], start_row)

        # D. Estilos Visuales (Aquí arreglamos los colores)
        self._apply_visual_styles(ws, df, tab_config, start_row)

        # E. Merge
        if "merge_column" in tab_config:
            self._merge_cells(ws, df, tab_config["columns"][tab_config["merge_column"]]["name"], start_row)

    def append_history(self, tab_config: dict, df: pd.DataFrame, time_chile):
        if df.empty: return
        hist_tab_name = tab_config.get("history_tab")
        if not hist_tab_name: return

        df_history = df.copy()
        df_history.insert(0, "Fecha Extracción", time_chile)

        ws = self._get_or_create_worksheet(hist_tab_name)
        
        if len(ws.get_all_values()) == 0:
            set_with_dataframe(ws, df_history, row=1)
        else:
            payload = df_history.astype(str).values.tolist()
            ws.append_rows(payload)

    def _get_or_create_worksheet(self, name):
        try: return self.sh.worksheet(name)
        except: return self.sh.add_worksheet(name, rows=100, cols=20)

    def _apply_data_formats(self, ws, df, columns_config, start_row_idx):
        batch = []
        final_name_map = {v["name"]: v for k, v in columns_config.items()}

        for i, col_name in enumerate(df.columns):
            if col_name in final_name_map:
                fmt_type = final_name_map[col_name].get("format", "TEXT")
                rule = self.FORMATS.get(fmt_type)
                if rule:
                    col_letter = rowcol_to_a1(1, i + 1).replace("1", "")
                    data_start = start_row_idx + 1
                    batch.append({
                        "range": f"{col_letter}{data_start}:{col_letter}{data_start + len(df)}",
                        "format": rule
                    })
        if batch: ws.batch_format(batch)

    def _apply_visual_styles(self, ws, df, tab_config, start_row_idx):
        batch_styles = []
        
        last_col_idx = len(df.columns)
        last_row_idx = start_row_idx + len(df)
        last_col_letter = rowcol_to_a1(1, last_col_idx).replace("1", "")
        
        # 1. BASE: A todo el rango le ponemos bordes Y LETRA NEGRA
        full_table_range = f"A{start_row_idx}:{last_col_letter}{last_row_idx}"
        batch_styles.append({
            "range": full_table_range,
            "format": self.STYLES["TABLE_BASE"]
        })

        # 2. HEADER: A la primera fila le ponemos AZUL Y LETRA BLANCA (sobrescribe lo anterior)
        header_range = f"A{start_row_idx}:{last_col_letter}{start_row_idx}"
        batch_styles.append({
            "range": header_range,
            "format": self.STYLES["HEADER_BLUE"]
        })

        # 3. COLUMNA AMARILLA (Merge)
        if "merge_column" in tab_config:
            target_col_name = tab_config["columns"][tab_config["merge_column"]]["name"]
            
            if target_col_name in df.columns:
                col_idx = df.columns.get_loc(target_col_name) + 1
                col_letter = rowcol_to_a1(1, col_idx).replace("1", "")
                
                # Desde debajo del header hasta el final
                side_range = f"{col_letter}{start_row_idx + 1}:{col_letter}{last_row_idx}"
                
                batch_styles.append({
                    "range": side_range,
                    "format": self.STYLES["COLUMN_YELLOW"]
                })

        if batch_styles:
            ws.batch_format(batch_styles)

    def _merge_cells(self, ws, df, col_name, start_row_idx):
        if col_name not in df.columns: return
        col_idx = df.columns.get_loc(col_name) + 1
        values = df[col_name].tolist()
        data_offset = start_row_idx + 1
        
        start = 0
        for i in range(1, len(values)):
            if values[i] != values[start]:
                if i - start > 1:
                    start_cell = rowcol_to_a1(start + data_offset, col_idx)
                    end_cell = rowcol_to_a1(i + data_offset - 1, col_idx)
                    ws.merge_cells(f"{start_cell}:{end_cell}")
                start = i
        if len(values) - start > 1:
             start_cell = rowcol_to_a1(start + data_offset, col_idx)
             end_cell = rowcol_to_a1(len(values) + data_offset - 1, col_idx)
             ws.merge_cells(f"{start_cell}:{end_cell}")