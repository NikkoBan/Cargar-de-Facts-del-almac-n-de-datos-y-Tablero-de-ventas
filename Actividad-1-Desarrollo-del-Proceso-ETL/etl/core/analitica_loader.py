import asyncio
from datetime import datetime, date
from typing import Any

import pyodbc

from core.trace_manager import TraceManager


class AnaliticaLoader:

    def __init__(self, dw_connection_string: str, trace: TraceManager):
        self._conn_str = dw_connection_string
        self._trace = trace

    @staticmethod
    def _normalize_text(value: Any, default: str = "DESCONOCIDO") -> str:
        if value is None:
            return default
        text = str(value).strip().upper()
        return text if text else default

    @staticmethod
    def _normalize_email(value: Any) -> str:
        if value is None:
            return "N/A"
        text = str(value).strip()
        return text if text else "N/A"

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            return int(str(value).strip())
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _to_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(str(value).strip()[:19], fmt).date()
            except ValueError:
                continue
        return None

    def _get_watermark(self, conn: pyodbc.Connection, entity: str) -> datetime:
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT LastExtractedAt FROM ETL_DimensionWatermark WHERE EntityName = ?",
                (entity,),
            )
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else datetime(2000, 1, 1)
        except Exception as e:
            self._trace.warning(f"Watermark no disponible para '{entity}': {e}")
            return datetime(2000, 1, 1)

    def _update_watermark(
        self, conn: pyodbc.Connection, entity: str, max_fecha: datetime
    ) -> None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                IF EXISTS (SELECT 1 FROM ETL_DimensionWatermark WHERE EntityName = ?)
                    UPDATE ETL_DimensionWatermark
                       SET LastExtractedAt = ?, UpdatedAt = GETDATE()
                     WHERE EntityName = ?
                ELSE
                    INSERT INTO ETL_DimensionWatermark (EntityName, LastExtractedAt, UpdatedAt)
                    VALUES (?, ?, GETDATE())
                """,
                (entity, max_fecha, entity, entity, max_fecha),
            )
            conn.commit()
            cursor.close()
            self._trace.debug(f"Watermark actualizado: {entity} → {max_fecha}")
        except Exception as e:
            self._trace.warning(f"No se pudo actualizar watermark '{entity}': {e}")

    async def load_dim_clientes_scd2(self) -> dict[str, int]:
        return await asyncio.to_thread(self._run_scd2)

    def _run_scd2(self) -> dict[str, int]:
        inserted = updated = skipped = errors = 0
        conn = None
        try:
            conn = pyodbc.connect(self._conn_str, timeout=30)

            wm = self._get_watermark(conn, "DimClientesSCD2")
            self._trace.info(f"SCD2 watermark: {wm}")

            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT cliente_id, nombre, apellido, email,
                       ciudad, pais, segmento, fecha_carga
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY cliente_id
                               ORDER BY fecha_carga DESC
                           ) AS rn
                    FROM stg_clientes
                    WHERE fecha_carga > ?
                      AND TRY_CAST(cliente_id AS INT) IS NOT NULL
                ) t
                WHERE rn = 1
                """,
                (wm,),
            )
            staging_rows = cursor.fetchall()
            max_fecha: datetime = wm

            for row in staging_rows:
                (
                    cliente_id_raw, nombre, apellido, email,
                    ciudad, pais, segmento, fecha_carga,
                ) = row

                if fecha_carga and fecha_carga > max_fecha:
                    max_fecha = fecha_carga

                cliente_id = self._to_int(cliente_id_raw)
                if cliente_id is None:
                    errors += 1
                    continue

                nom_c = self._normalize_text(nombre)
                ape_c = self._normalize_text(apellido)
                ema_c = self._normalize_email(email)
                ciu_c = self._normalize_text(ciudad)
                pai_c = self._normalize_text(pais)
                seg_c = self._normalize_text(segmento, default="GENERAL")

                cursor.execute(
                    """
                    SELECT ClienteSK, Nombre, Apellido, Email, Ciudad, Pais
                    FROM DimClientesSCD2
                    WHERE ClienteID = ? AND IsCurrent = 1
                    """,
                    (cliente_id,),
                )
                vigente = cursor.fetchone()

                if vigente is None:
                    cursor.execute(
                        """
                        INSERT INTO DimClientesSCD2
                            (ClienteID, Nombre, Apellido, Email, Ciudad, Pais,
                             Segmento, StartDate, EndDate, IsCurrent, LoadedAt)
                        VALUES (?, ?, ?, ?, ?, ?, ?,
                                GETDATE(), '9999-12-31', 1, GETDATE())
                        """,
                        (cliente_id, nom_c, ape_c, ema_c, ciu_c, pai_c, seg_c),
                    )
                    inserted += 1

                else:
                    sk, nom_v, ape_v, ema_v, ciu_v, pai_v = vigente

                    changed = (
                        (nom_v or "") != nom_c
                        or (ape_v or "") != ape_c
                        or (ema_v or "") != ema_c
                        or (ciu_v or "") != ciu_c
                        or (pai_v or "") != pai_c
                    )

                    if changed:
                        cursor.execute(
                            """
                            UPDATE DimClientesSCD2
                               SET EndDate = GETDATE(), IsCurrent = 0
                             WHERE ClienteSK = ?
                            """,
                            (sk,),
                        )
                        cursor.execute(
                            """
                            INSERT INTO DimClientesSCD2
                                (ClienteID, Nombre, Apellido, Email, Ciudad, Pais,
                                 Segmento, StartDate, EndDate, IsCurrent, LoadedAt)
                            VALUES (?, ?, ?, ?, ?, ?, ?,
                                    GETDATE(), '9999-12-31', 1, GETDATE())
                            """,
                            (cliente_id, nom_c, ape_c, ema_c, ciu_c, pai_c, seg_c),
                        )
                        updated += 1
                    else:
                        skipped += 1

            conn.commit()

            if staging_rows and max_fecha > wm:
                self._update_watermark(conn, "DimClientesSCD2", max_fecha)

            self._trace.info(
                f"DimClientesSCD2 → INSERT: {inserted} | "
                f"SCD2-UPDATE: {updated} | SIN CAMBIO: {skipped} | ERRORES: {errors}"
            )
            self._trace.increment("scd2_inserted", inserted)
            self._trace.increment("scd2_updated", updated)

        except pyodbc.Error as e:
            self._trace.error("Error en load_dim_clientes_scd2", e)
        finally:
            if conn:
                conn.close()

        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    async def run_analytics_etl(self) -> None:
        self._trace.start_timer("analytics_etl")
        self._trace.info("── FASE ANALÍTICA: SCD Tipo 2 + Watermark ──")

        try:
            stats = await self.load_dim_clientes_scd2()
            self._trace.info(f"  DimClientesSCD2 completado: {stats}")
        except Exception as e:
            self._trace.error("Error en run_analytics_etl", e)
        finally:
            self._trace.stop_timer("analytics_etl")
