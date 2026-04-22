# CONTEXTO COMPLETO — Proyecto ETL & Dashboard de Ventas

## 1. DESCRIPCIÓN GENERAL

Sistema ETL en Python + Dashboard Streamlit conectado a un Data Warehouse en SQL Server LocalDB.

- **Servidor BD:** `(localdb)\MSSQLLocalDB`
- **BD OLTP:** `VentasDB`
- **BD Analítica (DW):** `VentasAnalisisDB`
- **Modelo:** Estrella (Star Schema)
- **Entorno:** Python 3.14, venv en `etl\venv\`
- **Ruta del proyecto:** `C:\Users\SEJM_8909\OneDrive\Escritorio\Cargar de dimensiones del almacén de datos\Actividad-1-Desarrollo-del-Proceso-ETL\etl\`

---

## 2. ESTRUCTURA DE CARPETAS

```
etl/
├── main.py                    ← Punto de entrada ETL
├── requirements.txt           ← pydantic, aiohttp, pyodbc, streamlit, pandas, plotly
├── config/
│   ├── settings.py
│   └── config.json            ← Cadenas de conexión y rutas
├── core/
│   ├── flat_file_reader.py    ← Extractor CSV
│   ├── sql_data_gateway.py    ← Extractor SQL Server
│   ├── http_data_collector.py ← Extractor API REST
│   ├── data_sink_manager.py   ← Staging JSON/CSV
│   ├── dw_loader.py           ← Carga staging SQL + ejecuta SP
│   ├── oltp_loader.py         ← Carga VentasDB OLTP
│   ├── analitica_loader.py    ← SCD Tipo 2 + Watermark
│   └── trace_manager.py       ← Logs y métricas
├── dashboard/
│   └── app.py                 ← Dashboard Streamlit + Plotly
├── database/
│   ├── Ventas.sql             ← DDL VentasDB (OLTP)
│   └── VentasAnalisis.sql     ← DDL VentasAnalisisDB (DW)
├── dto/                       ← DTOs Pydantic
├── staging/                   ← Archivos JSON/CSV generados
└── logs/etl.log
```

---

## 3. CADENAS DE CONEXIÓN (config/config.json)

```json
{
  "database": {
    "oltp_connection_string": "Driver={ODBC Driver 17 for SQL Server};Server=(localdb)\\MSSQLLocalDB;Database=VentasDB;Trusted_Connection=yes;",
    "dw_connection_string": "Driver={ODBC Driver 17 for SQL Server};Server=(localdb)\\MSSQLLocalDB;Database=VentasAnalisisDB;Trusted_Connection=yes;"
  },
  "api": {
    "base_url": "https://jsonplaceholder.typicode.com",
    "endpoints": {"users": "/users", "posts": "/posts", "comments": "/comments"},
    "retry": {"max_attempts": 3, "delay_seconds": 2}
  },
  "paths": {
    "csv_folder": "./cvs/",
    "staging_folder": "./staging/",
    "logs_folder": "./logs/"
  }
}
```

---

## 4. MODELO ESTRELLA — VentasAnalisisDB

### Dimensiones

| Tabla | Estrategia | Campos clave |
|---|---|---|
| DimTiempo | Auto-poblada 2023-2026 | idTiempo, Fecha, Anio, Mes, Trimestre, EsFinDeSemana |
| DimCliente | SCD Tipo 1 | idCliente, ClienteIDOrigen, NombreCompleto, Email, Ciudad, Pais, Segmento |
| DimClientesSCD2 | SCD Tipo 2 + Watermark | ClienteSK, ClienteID, StartDate, EndDate, IsCurrent |
| DimCategoria | INSERT nuevas | idCategoria, NombreCategoria |
| DimProducto | SCD Tipo 1 | idProducto, NombreProducto, Categoria, Precio |
| DimEstadosPedido | MERGE catálogo | EstadoID, Estado |

### Tablas de Hechos

| Tabla | Descripción |
|---|---|
| FactVentas | Granularidad línea de venta |
| FactPedidos | Cabecera de pedidos |
| FactDetallesPedido | Líneas de pedido |
| FactFacturacion | Total facturado por pedido |

### Tablas Staging

`stg_clientes`, `stg_productos`, `stg_ventas`, `stg_detalle_ventas`, `stg_api_comentarios`

### Stored Procedures

- `sp_CargarDimEstadosPedido` → Paso 1
- `sp_CargarDimCategoria` → Paso 2
- `sp_CargarDimCliente` → Paso 3 (SCD1)
- `sp_CargarDimProducto` → Paso 4 (SCD1)
- `sp_CargarFactVentas` → Paso 5
- `sp_CargarFactPedidos` → Paso 6
- `sp_CargarFactDetallesPedido` → Paso 7
- `sp_CargarFactFacturacion` → Paso 8
- Limpieza staging → Paso 9
- `sp_EjecutarETLCompleto` → Orquesta los 9 pasos

### Control incremental

Tabla `ETL_DimensionWatermark` con campos `EntityName`, `LastExtractedAt`.

---

## 5. FLUJO ETL COMPLETO

```
FUENTES → FlatFileReader (CSV) + SqlDataGateway (SQL) + HttpDataCollector (API)
       → Validación Pydantic (DTOs)
       → DataSinkManager (archivos staging JSON/CSV)
       → DwLoader: stg_clientes, stg_productos, stg_ventas, stg_detalle_ventas
       → sp_EjecutarETLCompleto (9 pasos)
       → AnaliticaLoader: DimClientesSCD2 SCD Tipo 2 + Watermark
```

---

## 6. DASHBOARD — KPIs

| KPI | Gráfico |
|---|---|
| KPI 1 — Ingresos por Mes | Barras + línea doble eje |
| KPI 2 — Top 10 Clientes | Barras horizontales |
| KPI 3 — Ventas por Categoría | Donut |
| KPI 4 — Ventas por Trimestre | Barras |
| KPI 5 — Top 10 Productos | Barras horizontales |
| KPI 6 — Laboral vs Fin de Semana | Donut |
| Extra — Tendencia Diaria | Área con relleno |

**Tecnologías:** Streamlit 1.56, Plotly 6.7, Pandas 3.0, pyodbc 5.3

---

## 7. CÓMO EJECUTAR

```powershell
cd "C:\Users\SEJM_8909\OneDrive\Escritorio\Cargar de dimensiones del almacén de datos\Actividad-1-Desarrollo-del-Proceso-ETL\etl"
.\venv\Scripts\Activate.ps1
python main.py
.\venv\Scripts\streamlit.exe run dashboard\app.py
```

---

## 8. PROTOCOLOS DE SOPORTE ACTIVOS

### Tipo 1 — Errores Pyrefly (c:\__pyrefly_virtual__\inmemory\*.py)
Falsos positivos. El código es correcto. Solo recargar el IDE.

### Tipo 2 — Cannot find module
Problema de intérprete. Activar venv + `pip install -r requirements.txt` + seleccionar intérprete en VS Code.

### Error 4060 — Cannot open database
La BD no existe. Crear con `sqlcmd -S "(localdb)\MSSQLLocalDB" -i "database\VentasAnalisis.sql"`.

### Error 18456 — Login failed
Consecuencia del 4060. Se resuelve al crear la BD. No cambiar credenciales (usa Trusted_Connection=yes).

### Error 28000 — pyodbc.InterfaceError
Misma causa que 4060/18456. Verificar que LocalDB esté corriendo: `sqllocaldb info MSSQLLocalDB`.

---

## 9. ESTADO ACTUAL

- VentasDB: ONLINE
- VentasAnalisisDB: ONLINE (16 tablas + 9 SPs funcionales)
- venv instalado en etl\venv\ con todas las dependencias
- Dashboard corriendo en http://localhost:8501
- Execution policy de PowerShell configurada: RemoteSigned
