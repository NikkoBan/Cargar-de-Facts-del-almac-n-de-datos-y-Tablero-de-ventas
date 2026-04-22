# Proyecto ETL — Carga de Dimensiones del Almacén de Datos

## Descripción General

Sistema ETL (Extract, Transform, Load) implementado en Python que extrae datos desde múltiples fuentes, los valida, los carga en una zona de staging y finalmente los transforma hacia un **Data Warehouse** con modelo estrella (Star Schema) en SQL Server LocalDB.

---

## Arquitectura del Modelo Estrella

```
          ┌──────────────┐
          │  DimCliente  │
          └──────┬───────┘
                 │
  ┌────────────┐ │ ┌──────────────┐
  │ DimTiempo  │─┼─│ DimCategoria │
  └────────────┘ │ └──────────────┘
                 │
          ┌──────┴───────┐
          │  FactVentas  │  ← Tabla de Hechos
          └──────┬───────┘
                 │
          ┌──────┴───────┐
          │ DimProducto  │
          └──────────────┘
```

---

## Dimensiones del Data Warehouse

### DimTiempo
| Campo | Tipo | Descripción |
|---|---|---|
| idTiempo | INT (PK) | Clave sustituta |
| Fecha | DATE | Fecha completa |
| Anio | INT | Año |
| Mes | INT | Número de mes |
| NombreMes | NVARCHAR | Nombre del mes |
| Trimestre | INT | Trimestre (1-4) |
| Dia | INT | Día del mes |
| DiaSemana | INT | Día de la semana |
| NombreDia | NVARCHAR | Nombre del día |
| Semana | INT | Semana del año |
| EsFinDeSemana | BIT | 1 = fin de semana |

> Poblada automáticamente con fechas del 2023-01-01 al 2026-12-31.

---

### DimCliente
| Campo | Tipo | Descripción |
|---|---|---|
| idCliente | INT (PK) | Clave sustituta |
| ClienteIDOrigen | INT | Clave natural del OLTP |
| NombreCompleto | NVARCHAR | Nombre y apellido concatenados |
| Email | NVARCHAR | Correo electrónico |
| Telefono | NVARCHAR | Teléfono |
| Ciudad | NVARCHAR | Ciudad (UPPER) |
| Pais | NVARCHAR | País (UPPER) |
| Segmento | NVARCHAR | Segmento de cliente |
| FechaRegistro | DATE | Fecha de registro |
| Activo | BIT | Estado activo |

**Estrategia de carga:** SCD Tipo 1 — actualiza atributos cambiados; solo escribe si hay cambio real.

---

### DimCategoria
| Campo | Tipo | Descripción |
|---|---|---|
| idCategoria | INT (PK) | Clave sustituta |
| CategoriaIDOrigen | INT | Clave natural del OLTP |
| NombreCategoria | NVARCHAR | Nombre en UPPER |
| Descripcion | NVARCHAR | Descripción |

**Estrategia de carga:** INSERT de categorías nuevas con estandarización a mayúsculas.

---

### DimProducto
| Campo | Tipo | Descripción |
|---|---|---|
| idProducto | INT (PK) | Clave sustituta |
| ProductoIDOrigen | INT | Clave natural del OLTP |
| NombreProducto | NVARCHAR | Nombre del producto (UPPER) |
| Categoria | NVARCHAR | Desnormalizado desde DimCategoria |
| Precio | DECIMAL(10,2) | Precio unitario |
| Estado | NVARCHAR | Estado del producto |

**Estrategia de carga:** SCD Tipo 1 — actualiza precio, nombre y categoría si cambiaron.

---

### DimClientesSCD2 (Historial)
| Campo | Tipo | Descripción |
|---|---|---|
| ClienteSK | INT (PK) | Surrogate key |
| ClienteID | INT | Business key |
| Nombre | NVARCHAR | Nombre del cliente |
| Apellido | NVARCHAR | Apellido |
| Email | NVARCHAR | Email |
| Ciudad | NVARCHAR | Ciudad |
| Pais | NVARCHAR | País |
| Segmento | NVARCHAR | Segmento |
| StartDate | DATETIME2 | Inicio de vigencia |
| EndDate | DATETIME2 | Fin de vigencia (9999-12-31 = vigente) |
| IsCurrent | BIT | 1 = fila vigente actual |

**Estrategia de carga:** SCD Tipo 2 — cierra la fila vigente e inserta nueva versión cuando cambian atributos. Usa watermark incremental para procesar solo registros nuevos.

---

### DimEstadosPedido
| Campo | Tipo | Descripción |
|---|---|---|
| EstadoID | INT (PK) | Identificador |
| Estado | NVARCHAR | UNKNOWN, PENDING, SHIPPED, DELIVERED, CANCELLED |

**Estrategia de carga:** MERGE — catálogo controlado con valores fijos.

---

## Tabla de Hechos

### FactVentas
| Campo | Tipo | Descripción |
|---|---|---|
| idHecho | INT (PK) | Clave sustituta |
| idCliente | INT (FK) | → DimCliente |
| idProducto | INT (FK) | → DimProducto |
| idTiempo | INT (FK) | → DimTiempo |
| idCategoria | INT (FK) | → DimCategoria |
| Cantidad | INT | Unidades vendidas |
| PrecioUnitario | DECIMAL | Precio por unidad |
| TotalVenta | DECIMAL | Total de la línea |
| VentaIDOrigen | INT | ID original del OLTP |
| EstadoVenta | NVARCHAR | Estado de la venta |

---

## Flujo ETL Completo

```
FUENTES DE DATOS
  ├── CSV (customers, products, orders, order_details)
  ├── SQL Server OLTP (VentasDB)
  └── API REST (JSONPlaceholder)
         │
         ▼
   EXTRACCIÓN (FlatFileReader, SqlDataGateway, HttpDataCollector)
         │
         ▼
   VALIDACIÓN (DTOs Pydantic)
         │
         ▼
   STAGING ARCHIVOS (JSON/CSV en /staging)
         │
         ▼
   STAGING SQL (stg_clientes, stg_productos, stg_ventas, stg_detalle_ventas)
         │
         ▼
   sp_EjecutarETLCompleto (9 pasos)
     [1] sp_CargarDimEstadosPedido   → MERGE estados base
     [2] sp_CargarDimCategoria       → INSERT categorías nuevas
     [3] sp_CargarDimCliente         → SCD Tipo 1
     [4] sp_CargarDimProducto        → SCD Tipo 1
     [5] sp_CargarFactVentas         → Hechos granularidad línea
     [6] sp_CargarFactPedidos        → Cabecera de pedidos
     [7] sp_CargarFactDetallesPedido → Líneas de pedido
     [8] sp_CargarFactFacturacion    → Total facturado
     [9] Limpieza staging (> 30 días)
         │
         ▼
   AnaliticaLoader (Python)
     └── DimClientesSCD2 — SCD Tipo 2 con watermark incremental
```

---

## KPIs del Dashboard

| KPI | Descripción |
|---|---|
| KPI 1 — Ventas por Mes | Ingresos y cantidad de transacciones por mes/año |
| KPI 2 — Top 10 Clientes | Clientes con mayor ingreso total generado |
| KPI 3 — Ventas por Categoría | Distribución de ingresos por categoría de producto |
| KPI 4 — Ventas por Trimestre | Ingresos agrupados por trimestre y año |
| KPI 5 — Top 10 Productos | Productos más vendidos por unidades |
| KPI 6 — Laboral vs Fin de Semana | Comparativa de ventas por tipo de día |

---

## Estructura del Proyecto

```
etl/
├── main.py                    ← Punto de entrada (ETL Worker)
├── requirements.txt
├── config/
│   ├── settings.py            ← Configuración desde config.json y .env
│   └── config.json
├── core/
│   ├── flat_file_reader.py    ← Extractor CSV
│   ├── sql_data_gateway.py    ← Extractor SQL Server
│   ├── http_data_collector.py ← Extractor API REST
│   ├── data_sink_manager.py   ← Guardado en staging (JSON/CSV)
│   ├── dw_loader.py           ← Carga staging SQL + ejecuta SPs
│   ├── oltp_loader.py         ← Carga base OLTP (VentasDB)
│   ├── analitica_loader.py    ← SCD Tipo 2 + Watermark
│   └── trace_manager.py       ← Logs y métricas
├── dashboard/
│   └── app.py                 ← Dashboard Streamlit + Plotly
├── database/
│   └── VentasAnalisis.sql     ← DDL completo del Data Warehouse
├── dto/                       ← DTOs Pydantic para validación
├── staging/                   ← Archivos JSON/CSV generados
└── logs/
    └── etl.log                ← Log de ejecución
```

---

## Cómo Ejecutar

### 1. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar la base de datos
Ejecutar el script SQL en SQL Server LocalDB:
```
database/VentasAnalisis.sql
```

### 3. Ejecutar el proceso ETL
```bash
python main.py
```

### 4. Lanzar el Dashboard
```bash
streamlit run dashboard/app.py
```

---

## Calidad de Datos

- **UPPER()** en todos los campos de texto de dimensiones
- **TRIM()** para eliminar espacios en blanco
- Valores nulos reemplazados: `"DESCONOCIDO"`, `"N/A"`, `"GENERAL"`
- Validación de tipos con `TRY_CAST` antes de insertar
- Deduplicación por `Business Key` antes de cada INSERT
- Prevención de escrituras innecesarias (solo actualiza si hubo cambio real)
