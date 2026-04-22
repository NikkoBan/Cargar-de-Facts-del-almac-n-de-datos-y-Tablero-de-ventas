import asyncio
import csv
import json
from pathlib import Path
from typing import Any

from core.trace_manager import TraceManager


class DataSinkManager:

    def __init__(self, staging_folder: str, trace: TraceManager):
        self._staging_path = Path(staging_folder)
        self._staging_path.mkdir(parents=True, exist_ok=True)
        self._trace = trace

    async def save_as_json(self, data: list[dict[str, Any]], filename: str) -> str:
        file_path = self._staging_path / f"{filename}.json"
        await asyncio.to_thread(self._write_json, data, file_path)
        self._trace.info(f"💾 Guardado JSON: {file_path.name} ({len(data)} registros)")
        self._trace.increment("staging_files_written")
        return str(file_path)

    async def save_as_csv(self, data: list[dict[str, Any]], filename: str) -> str:
        if not data:
            self._trace.warning(f"Sin datos para guardar en '{filename}.csv'")
            return ""

        file_path = self._staging_path / f"{filename}.csv"
        await asyncio.to_thread(self._write_csv, data, file_path)
        self._trace.info(f"💾 Guardado CSV: {file_path.name} ({len(data)} registros)")
        self._trace.increment("staging_files_written")
        return str(file_path)

    async def save_grouped(
        self, data: list[dict[str, Any]], group_key: str, prefix: str
    ) -> list[str]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for record in data:
            key = str(record.get(group_key, "unknown"))
            groups.setdefault(key, []).append(record)

        saved_files: list[str] = []
        for group_name, group_data in groups.items():
            safe_name = group_name.replace("/", "_").replace("\\", "_")
            path = await self.save_as_json(group_data, f"{prefix}_{safe_name}")
            if path:
                saved_files.append(path)

        return saved_files

    @staticmethod
    def _write_json(data: list[dict[str, Any]], file_path: Path) -> None:
        clean_data = [
            {k: v for k, v in record.items() if not k.startswith("_")}
            for record in data
        ]
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(clean_data, f, indent=2, ensure_ascii=False, default=str)

    @staticmethod
    def _write_csv(data: list[dict[str, Any]], file_path: Path) -> None:
        clean_data = [
            {k: v for k, v in record.items() if not k.startswith("_")}
            for record in data
        ]
        if not clean_data:
            return
        fieldnames = list(clean_data[0].keys())
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(clean_data)
