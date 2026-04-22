import asyncio
from typing import Any

import pyodbc

from core.trace_manager import TraceManager


class OltpLoader:

    def __init__(self, connection_string: str, trace: TraceManager):
        self._conn_str = connection_string
        self._trace = trace

    async def load_from_csv(self, records: list[dict[str, Any]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in records:
            key = r.get("_source_file", "unknown")
            grouped.setdefault(key, []).append(r)

        if any(k.lower().startswith("customers") for k in grouped):
            await self._load_clientes(grouped.get("customers.csv", []))

        if any(k.lower().startswith("products") for k in grouped):
            await self._load_productos(grouped.get("products.csv", []))

        if any(k.lower().startswith("orders") for k in grouped):
            await self._load_ventas(grouped.get("orders.csv", []))

        if any(k.lower().startswith("order_details") for k in grouped):
            await self._load_detalle_ventas(grouped.get("order_details.csv", []))

    async def _load_clientes(self, records: list[dict[str, Any]]) -> int:
        sql = """
            IF NOT EXISTS (SELECT 1 FROM Clientes WHERE ClienteID = ?)
            BEGIN
                INSERT INTO Clientes (ClienteID, Nombre, Apellido, Email, Telefono, Ciudad, Pais, Segmento)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            END
        """
        rows = [
            (
                r.get("customer_id"), r.get("customer_id"),
                r.get("first_name"), r.get("last_name"),
                r.get("email"), r.get("phone"),
                r.get("city"), r.get("country"),
                r.get("segmento", "General"),
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "Clientes")

    async def _load_productos(self, records: list[dict[str, Any]]) -> int:
        category_cache: dict[str, int] = {}
        for r in records:
            cat = (r.get("category") or "").strip()
            if cat and cat not in category_cache:
                category_cache[cat] = self._ensure_categoria(cat)

        sql = """
            IF NOT EXISTS (SELECT 1 FROM Productos WHERE ProductoID = ?)
            BEGIN
                INSERT INTO Productos (ProductoID, Nombre, CategoriaID, Precio, Stock, Estado)
                VALUES (?, ?, ?, ?, ?, ?)
            END
        """
        rows = []
        for r in records:
            cat = (r.get("category") or "").strip()
            rows.append((
                r.get("product_id"), r.get("product_id"),
                r.get("product_name"), category_cache.get(cat),
                r.get("price"), r.get("stock"), "Activo",
            ))
        return await asyncio.to_thread(self._execute_many, sql, rows, "Productos")

    def _ensure_categoria(self, nombre: str) -> int:
        cat_id = None
        try:
            conn = pyodbc.connect(self._conn_str, timeout=30)
            cursor = conn.cursor()
            cursor.execute(
                "IF NOT EXISTS (SELECT 1 FROM Categorias WHERE Nombre = ?) "
                "BEGIN INSERT INTO Categorias (Nombre, Descripcion) VALUES (?, NULL) END",
                (nombre, nombre),
            )
            conn.commit()
            cursor.execute("SELECT CategoriaID FROM Categorias WHERE Nombre = ?", (nombre,))
            row = cursor.fetchone()
            if row:
                cat_id = row[0]
            cursor.close()
            conn.close()
        except pyodbc.Error as e:
            self._trace.error("Error cargando categoría", e)
        return cat_id or 0

    async def _load_ventas(self, records: list[dict[str, Any]]) -> int:
        sql = """
            IF NOT EXISTS (SELECT 1 FROM Ventas WHERE VentaID = ?)
            BEGIN
                INSERT INTO Ventas (VentaID, ClienteID, FechaVenta, Estado)
                VALUES (?, ?, ?, ?)
            END
        """
        rows = [
            (r.get("order_id"), r.get("order_id"), r.get("customer_id"),
             r.get("order_date"), r.get("status"))
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "Ventas")

    async def _load_detalle_ventas(self, records: list[dict[str, Any]]) -> int:
        sql = """
            IF NOT EXISTS (SELECT 1 FROM DetalleVentas WHERE VentaID = ? AND ProductoID = ?)
            BEGIN
                INSERT INTO DetalleVentas (VentaID, ProductoID, Cantidad, PrecioUnitario, TotalLinea)
                VALUES (?, ?, ?, ?, ?)
            END
        """
        rows = []
        for r in records:
            cantidad = r.get("quantity") or 0
            total = r.get("total_price") or 0
            precio_u = float(total) / float(cantidad) if cantidad and float(cantidad) != 0 else 0
            rows.append((
                r.get("order_id"), r.get("product_id"),
                r.get("order_id"), r.get("product_id"),
                cantidad, precio_u, total,
            ))
        return await asyncio.to_thread(self._execute_many, sql, rows, "DetalleVentas")

    def _execute_many(self, sql: str, rows: list[tuple], table_name: str) -> int:
        if not rows:
            return 0
        count = 0
        try:
            conn = pyodbc.connect(self._conn_str, timeout=30)
            cursor = conn.cursor()
            for row in rows:
                try:
                    cursor.execute(sql, row)
                    count += 1
                except pyodbc.Error as e:
                    self._trace.warning(f"Fila rechazada en {table_name}: {e}")
            conn.commit()
            cursor.close()
            conn.close()
            self._trace.info(f"  → {count}/{len(rows)} filas insertadas en {table_name}")
        except pyodbc.Error as e:
            self._trace.error(f"Error conectando al OLTP para {table_name}", e)
        return count
