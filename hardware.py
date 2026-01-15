"""
Contexto:
Juan Pablo necesitaba saber que componentes son los que más suelen
tener los servidores porque necesitaba armar uno, hice este script
para obtener metricas de hardware y luego promediar, pero, como
estamos en temporada no se me permitió ejecutarlo en los servidores.
"""

import subprocess
import os

class HardwareInfo:
    def __init__(self):
        self.is_root = os.geteuid() == 0

    def run_command(self, command) -> str:
        """
        Ejecuta comandos de shell y devuelve la salida limpia.

        :param command: Comando a ejecutar.
        :return: Salida limpia del comando.
        """
        try:
            result = subprocess.check_output(
                command,
                shell=True,
                stderr=subprocess.DEVNULL
            )
            return result.decode('utf-8').strip()
        except Exception:
            return ""

    def get_cpu(self) -> str:
        """Obtiene el nombre del procesador leyendo /proc/cpuinfo."""
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if "model name" in line:
                        return line.split(':', 1)[1].strip()
        except:
            pass
        return "Desconocido"

    def get_motherboard(self) -> str:
        """Obtiene nombre y fabricante de la placa base desde /sys."""
        try:
            vendor = self.run_command(
                "cat /sys/devices/virtual/dmi/id/board_vendor"
            )
            name = self.run_command(
                "cat /sys/devices/virtual/dmi/id/board_name"
            )
            if vendor or name:
                return f"{vendor} {name}"
        except:
            pass
        return "Desconocido (puede requerir sudo)"

    def get_gpu(self) -> list[str]:
        """Obtiene tarjetas gráficas usando lspci."""
        gpus = []
        try:
            # operacion de solo lectura, así que debería ser segura
            # lspci viene en Fedora, asi que tmapoco se necesitaría
            # ps instalar alguna cosita
            raw_out = self.run_command(
                "lspci -mm | grep -E 'VGA|3D'"
            )
            for line in raw_out.splitlines():
                parts = line.split('"')
                if len(parts) >= 6:
                    vendor = parts[3]
                    device = parts[5]
                    gpus.append(f"{vendor} {device}")
                else:
                    gpus.append(line)
        except:
            gpus.append("No se pudo detectar (falta lspci?)")
        return gpus

    def get_ram_summary(self) -> str:
        """
        Obtiene el total de RAM desde /proc/meminfo
        (no requiere root).
        """
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if "MemTotal" in line:
                        parts = line.split()
                        kb = int(parts[1])
                        gb = round(kb / (1024 * 1024), 2)
                        return f"{gb} GB (Total Sistema)"
        except:
            return "Desconocido"

    def get_ram_details(self) -> list[str]:
        """
        Obtiene detalles físicos de la RAM usando dmidecode
        (REQUIERE SUDO).
        """
        if not self.is_root:
            return [
                "Para ver Marca y Tipo DDR ejecuta el script con sudo, dah"
            ]

        sticks = []
        raw_data = self.run_command("dmidecode -t memory")
        
        # Parseo básico de la salida de dmidecode
        current_stick = {}
        in_device = False
        
        for line in raw_data.splitlines():
            line = line.strip()
            if line == "Memory Device":
                no_module = "No Module" not in current_stick.get("Size", "")

                if current_stick.get("Size") and no_module:
                    sticks.append(current_stick)
                current_stick = {}
                in_device = True
                continue

            if in_device:
                if line.startswith("Size:"):
                    current_stick["Size"] = line.split(":", 1)[1].strip()
                elif line.startswith("Type:"):
                    current_stick["Type"] = line.split(":", 1)[1].strip()
                elif line.startswith("Manufacturer:"):
                    current_stick["Manufacturer"] =(
                        line.split(":", 1)[1].strip()
                    )
                elif line.startswith("Speed:"):
                     current_stick["Speed"] = line.split(":", 1)[1].strip()
                elif line == "":
                    in_device = False

        # Agregar el último si existe
        no_module2: bool= "No Module" not in current_stick["Size"]
        if current_stick.get("Size") and  no_module2:
            sticks.append(current_stick)

        formatted_sticks = []
        for s in sticks:
            info = (
                f"{s.get('Size', '?')} | {s.get('Type', '?')} | "
                +f"{s.get('Manufacturer', '?')} | {s.get('Speed', '?')}"
            )
            formatted_sticks.append(info)
        
        if not formatted_sticks:
            return [
                "No se detectaron módulos físicos (¿Es una Máquina Virtual?)"
            ]
            
        return formatted_sticks

    def get_disks(self) -> list[str]:
        """Obtiene discos usando lsblk."""
        disks = []
        # lsblk -d (device) -n (no header) -o (columns)
        # ROTA: 1 = HDD, 0 = SSD
        raw_out = (
            self.run_command("lsblk -d -n -o NAME,SIZE,ROTA,MODEL,TYPE")
        )

        for line in raw_out.splitlines():
            parts = line.split(maxsplit=4)
            if len(parts) >= 3:
                dtype = parts[4] if len(parts) > 4 else "?"
                if dtype != "disk":
                    continue

                name = parts[0]
                size = parts[1]
                is_rota = parts[2]
                model = parts[3] if len(parts) > 3 else "Genérico"
                
                disk_type = "HDD" if is_rota == "1" else "SSD"
                disks.append(
                    f"[{disk_type}] {model} - Capacidad: {size} ({name})"
                )
        if not disks:
            return ["No se detectaron discos físicos"]
        return disks

    def print_report(self) -> None:
        """Imprime el reporte todo cute, todo wonito"""
        print("="*60)
        print(f" REPORTE DE HARDWARE")
        print("="*60)

        # Procesador
        print(f"\n[PROCESADOR]\n  -> {self.get_cpu()}")

        # Placa Madre
        print(f"\n[PLACA MADRE]\n  -> {self.get_motherboard()}")

        # RAM
        print(f"\n[MEMORIA RAM]")
        print(
            f"  -> Capacidad Total Detectada (OS): {self.get_ram_summary()}"
        )
        print(f"  -> Módulos Físicos (Detalle):")
        ram_details = self.get_ram_details()

        text = len(ram_details) if '!!' not in ram_details[0] else '?'
        print(f"     (Cantidad de módulos detectados: {text})")
        for stick in ram_details:
            print(f"     - {stick}")

        # Gráficas
        print(f"\n[TARJETAS GRÁFICAS]")
        for gpu in self.get_gpu():
            print(f"  -> {gpu}")

        # Discos
        print(f"\n[ALMACENAMIENTO]")
        disks = self.get_disks()
        print(f"  -> Cantidad de discos: {len(disks)}")
        for disk in disks:
            print(f"  -> {disk}")
        print("\n" + "="*60)

if __name__ == "__main__":
    info = HardwareInfo()
    info.print_report()