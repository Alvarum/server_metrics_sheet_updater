"""
Servicio encargado de la construcción y configuración del Dashboard.
"""

import time
from typing import List, Dict, Any, Optional
import gspread
from gspread.utils import a1_range_to_grid_range, a1_to_rowcol
from src.config import config
from src.services.sheets_styles import STYLES


class DashboardBuilder:
    def __init__(self, spreadsheet: gspread.Spreadsheet) -> None:
        self.sh = spreadsheet

    def build(self) -> None:
        # Pausa inicial fuerte para recuperar cuota de los pasos anteriores
        time.sleep(5)
        
        ws = self._prepare_worksheet("Dashboard")
        time.sleep(3)
        
        # 1. Estructura visual
        self._setup_layout(ws)
        time.sleep(2)
        
        # 2. Obtención de métricas
        srv_metrics = self._get_numeric_metrics(config.servers_config)
        cam_metrics = self._get_numeric_metrics(config.cameras_config)

        # 3. Selectores
        self._setup_selectors(ws, srv_metrics, cam_metrics)
        time.sleep(2)
        
        # 4. Fórmulas y Datos
        self._inject_formulas(ws, config.servers_config, config.cameras_config)
        self._format_hidden_data(ws)
        
        # Pausa LARGA antes de los gráficos (operación más costosa de la API)
        time.sleep(6)
        
        # 5. Gráficos
        self._create_charts(ws)

    def _prepare_worksheet(self, name: str) -> gspread.Worksheet:
        try:
            ws = self.sh.worksheet(name)
            ws.clear()
            time.sleep(1) # Pausa tras clear
            
            meta = self.sh.fetch_sheet_metadata({'includeGridData': False})
            sheet_meta = next((s for s in meta['sheets'] if s['properties']['sheetId'] == ws.id), None)
            
            requests = []
            
            # Borrar Gráficos
            if sheet_meta and 'charts' in sheet_meta:
                requests.extend([{"deleteEmbeddedObject": {"objectId": c["chartId"]}} for c in sheet_meta['charts']])

            # Borrar Validaciones
            requests.append({
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0, "endRowIndex": 1000,
                        "startColumnIndex": 0, "endColumnIndex": 50
                    },
                    "rule": None
                }
            })

            if requests:
                self.sh.batch_update({"requests": requests})
                time.sleep(2)
                    
        except gspread.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=name, rows=100, cols=35)

        self.sh.batch_update({
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "index": 0,
                        "gridProperties": {"hideGridlines": True}
                    },
                    "fields": "index,gridProperties.hideGridlines"
                }
            }]
        })
        return ws

    def _setup_layout(self, ws: gspread.Worksheet) -> None:
        ws.merge_cells("A1:Z1")
        ws.update("A1", [["DASHBOARD OPERATIVO - RAPTOR"]])
        ws.format("A1", STYLES["HEADER_DASHBOARD"])
        ws.format("A2:Z100", STYLES["DASHBOARD_BG"])

        ws.update_acell("B3", "Servidor:")
        ws.format("B3", STYLES["LABEL_BOLD"])
        
        ws.update_acell("E3", "Métrica:")
        ws.format("E3", STYLES["LABEL_BOLD"])
        
        ws.update_acell("B28", "Cámara:")
        ws.format("B28", STYLES["LABEL_BOLD"])
        
        ws.update_acell("E28", "Métrica Cámara:")
        ws.format("E28", STYLES["LABEL_BOLD"])

        ws.update("AA4", [["Fecha", "Valor"]])
        ws.update("AD4", [["Fecha", "Valor"]])

    def _setup_selectors(self, ws: gspread.Worksheet, srv_metrics: List[str], cam_metrics: List[str]) -> None:
        hist_srv = config.servers_config["history_tab"]
        hist_cam = config.cameras_config["history_tab"]
        
        def_srv = self._get_valid_default(hist_srv, 1)
        def_cam = self._get_valid_default(hist_cam, 2)
        
        if def_srv: ws.update_acell("C3", def_srv)
        if srv_metrics: ws.update_acell("F3", srv_metrics[0])
        
        if def_cam: ws.update_acell("C28", def_cam)
        if cam_metrics: ws.update_acell("F28", cam_metrics[0])

        requests = [
            {
                "setDataValidation": {
                    "range": a1_range_to_grid_range("C3", ws.id),
                    "rule": {
                        "condition": {"type": "ONE_OF_RANGE", "values": [{"userEnteredValue": f"='{hist_srv}'!A2:A"}]},
                        "showCustomUi": True
                    }
                }
            },
            {
                "setDataValidation": {
                    "range": a1_range_to_grid_range("F3", ws.id),
                    "rule": {
                        "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": m} for m in srv_metrics]},
                        "showCustomUi": True
                    }
                }
            },
            {
                "setDataValidation": {
                    "range": a1_range_to_grid_range("C28", ws.id),
                    "rule": {
                        "condition": {"type": "ONE_OF_RANGE", "values": [{"userEnteredValue": f"='{hist_cam}'!B2:B"}]},
                        "showCustomUi": True
                    }
                }
            },
            {
                "setDataValidation": {
                    "range": a1_range_to_grid_range("F28", ws.id),
                    "rule": {
                        "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": m} for m in cam_metrics]},
                        "showCustomUi": True
                    }
                }
            }
        ]
        self.sh.batch_update({"requests": requests})

    def _inject_formulas(self, ws: gspread.Worksheet, srv_conf: Dict[str, Any], cam_conf: Dict[str, Any]) -> None:
        h_srv = srv_conf["history_tab"]
        h_cam = cam_conf["history_tab"]

        f_srv = f'=IFERROR(SORT(FILTER(HSTACK(CHOOSECOLS(\'{h_srv}\'!A:Z, XMATCH("Fecha consulta", \'{h_srv}\'!1:1)), CHOOSECOLS(\'{h_srv}\'!A:Z, XMATCH(F3, \'{h_srv}\'!1:1))), \'{h_srv}\'!A:A = C3), 1, TRUE), "")'
        ws.update_acell("AA5", f_srv)

        f_cam = f'=IFERROR(SORT(FILTER(HSTACK(CHOOSECOLS(\'{h_cam}\'!A:Z, XMATCH("Fecha - hora consulta", \'{h_cam}\'!1:1)), CHOOSECOLS(\'{h_cam}\'!A:Z, XMATCH(F28, \'{h_cam}\'!1:1))), \'{h_cam}\'!B:B = C28), 1, TRUE), "")'
        ws.update_acell("AD5", f_cam)

    def _format_hidden_data(self, ws: gspread.Worksheet) -> None:
        fmt = {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm"}}
        reqs = [
            {"repeatCell": {"range": a1_range_to_grid_range("AA5:AA1000", ws.id), "cell": {"userEnteredFormat": fmt}, "fields": "userEnteredFormat"}},
            {"repeatCell": {"range": a1_range_to_grid_range("AD5:AD1000", ws.id), "cell": {"userEnteredFormat": fmt}, "fields": "userEnteredFormat"}}
        ]
        self.sh.batch_update({"requests": reqs})

    def _create_charts(self, ws: gspread.Worksheet) -> None:
        r_srv, c_srv = a1_to_rowcol("B6")
        r_cam, c_cam = a1_to_rowcol("B31")
        
        chart_srv = {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Evolución Temporal (Servidor)",
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [{"position": "BOTTOM_AXIS", "title": "Fecha"}, {"position": "LEFT_AXIS"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": ws.id, "startRowIndex": 4, "endRowIndex": 1000, "startColumnIndex": 26, "endColumnIndex": 27}]}}}],
                            "series": [{"series": {"sourceRange": {"sources": [{"sheetId": ws.id, "startRowIndex": 4, "endRowIndex": 1000, "startColumnIndex": 27, "endColumnIndex": 28}]}}, "targetAxis": "LEFT_AXIS"}],
                            "headerCount": 1,
                            "interpolateNulls": True
                        }
                    },
                    "position": {"overlayPosition": {"anchorCell": {"sheetId": ws.id, "rowIndex": r_srv - 1, "columnIndex": c_srv - 1}}}
                }
            }
        }

        chart_cam = {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Evolución Temporal (Cámara)",
                        "basicChart": {
                            "chartType": "LINE",
                            "legendPosition": "BOTTOM_LEGEND",
                            "axis": [{"position": "BOTTOM_AXIS", "title": "Fecha"}, {"position": "LEFT_AXIS"}],
                            "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": ws.id, "startRowIndex": 4, "endRowIndex": 1000, "startColumnIndex": 29, "endColumnIndex": 30}]}}}],
                            "series": [{"series": {"sourceRange": {"sources": [{"sheetId": ws.id, "startRowIndex": 4, "endRowIndex": 1000, "startColumnIndex": 30, "endColumnIndex": 31}]}}, "targetAxis": "LEFT_AXIS"}],
                            "headerCount": 1,
                            "interpolateNulls": True
                        }
                    },
                    "position": {"overlayPosition": {"anchorCell": {"sheetId": ws.id, "rowIndex": r_cam - 1, "columnIndex": c_cam - 1}}}
                }
            }
        }
        
        # Intento seguro con reintento simple
        try:
            self.sh.batch_update({"requests": [chart_srv, chart_cam]})
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                print("⚠️ Cuota excedida al crear gráficos. Esperando 15 segundos...")
                time.sleep(15)
                self.sh.batch_update({"requests": [chart_srv, chart_cam]})
            else:
                raise e

    def _get_numeric_metrics(self, tab_config: Dict[str, Any]) -> List[str]:
        return [v["name"] for k, v in tab_config["columns"].items() if v.get("format") in ["NUMBER", "INTEGER", "PERCENT"]]

    def _get_valid_default(self, tab_name: str, col_idx: int) -> Optional[str]:
        try:
            ws = self.sh.worksheet(tab_name)
            vals = ws.col_values(col_idx)[1:] 
            if vals:
                return vals[0]
            return None
        except Exception:
            return None