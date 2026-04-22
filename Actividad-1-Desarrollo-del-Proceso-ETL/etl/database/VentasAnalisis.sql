

-- =====================================================================
-- VENTASANALISISDB — BASE DE DATOS ANALÍTICA (DATA WAREHOUSE)
-- Motor   : SQL Server — (localdb)\MSSQLLocalDB
-- Archivo : database/VentasAnalisis.sql
-- Diseño  : Modelo Estrella (Star Schema)
-- =====================================================================
--
--  ARQUITECTURA DEL MODELO ESTRELLA:
--
--          ┌──────────────┐
--          │  DimCliente  │
--          └──────┬───────┘
--                 │
--  ┌────────────┐ │ ┌──────────────┐
--  │ DimTiempo  │─┼─│ DimCategoria │
--  └────────────┘ │ └──────────────┘
--                 │
--          ┌──────┴───────┐
--          │  FactVentas  │  ← Tabla de Hechos (centro)
--          └──────┬───────┘
--                 │
--          ┌──────┴───────┐
--          │ DimProducto  │
--          └──────────────┘
--
-- =====================================================================
-- Atributos de calidad:
--   • Rendimiento    → Índices en FK de hechos y columnas de agregación
--   • Escalabilidad  → Agregar dimensiones sin modificar hechos
--   • Seguridad      → Sin credenciales hardcodeadas
--   • Mantenibilidad → Separación OLTP / Staging / DW, nombres claros
--   • Desnormalización→ Dimensiones planas para consultas rápidas
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- 0. CREACIÓN DE LA BASE DE DATOS ANALÍTICA
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'VentasAnalisisDB')
BEGIN
    CREATE DATABASE VentasAnalisisDB;
END
GO

USE VentasAnalisisDB;
GO

-- =====================================================================
--                    ZONA DE STAGING (INTERMEDIA)
-- =====================================================================
-- Tablas sin restricciones complejas para carga rápida desde ETL.
-- Los datos llegan aquí antes de ser transformados y cargados al DW.
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- STG-1. stg_clientes — datos crudos de clientes
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_clientes')
BEGIN
    CREATE TABLE stg_clientes (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        cliente_id      NVARCHAR(50)      NULL,
        nombre          NVARCHAR(100)     NULL,
        apellido        NVARCHAR(100)     NULL,
        email           NVARCHAR(200)     NULL,
        telefono        NVARCHAR(100)     NULL,
        ciudad          NVARCHAR(100)     NULL,
        pais            NVARCHAR(100)     NULL,
        segmento        NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,   -- 'CSV', 'OLTP', 'API'
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_clientes PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-2. stg_productos — datos crudos de productos
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_productos')
BEGIN
    CREATE TABLE stg_productos (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        producto_id     NVARCHAR(50)      NULL,
        nombre          NVARCHAR(200)     NULL,
        categoria       NVARCHAR(100)     NULL,
        precio          NVARCHAR(50)      NULL,
        stock           NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_productos PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-3. stg_ventas — datos crudos de ventas/órdenes
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_ventas')
BEGIN
    CREATE TABLE stg_ventas (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        venta_id        NVARCHAR(50)      NULL,
        cliente_id      NVARCHAR(50)      NULL,
        fecha_venta     NVARCHAR(50)      NULL,
        estado          NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_ventas PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-4. stg_detalle_ventas — datos crudos de líneas de venta
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_detalle_ventas')
BEGIN
    CREATE TABLE stg_detalle_ventas (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        venta_id        NVARCHAR(50)      NULL,
        producto_id     NVARCHAR(50)      NULL,
        cantidad        NVARCHAR(50)      NULL,
        precio_unitario NVARCHAR(50)      NULL,
        total_linea     NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_detalle_ventas PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-5. stg_api_comentarios — datos crudos desde API REST
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_api_comentarios')
BEGIN
    CREATE TABLE stg_api_comentarios (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        post_id         NVARCHAR(50)      NULL,
        comentario_id   NVARCHAR(50)      NULL,
        nombre          NVARCHAR(500)     NULL,
        email           NVARCHAR(200)     NULL,
        cuerpo          NVARCHAR(MAX)     NULL,
        fuente          NVARCHAR(50)      NULL DEFAULT N'API',
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_api_comentarios PRIMARY KEY (id_carga)
    );
END
GO

-- =====================================================================
--              TABLAS DIMENSIONALES (MODELO ESTRELLA)
-- =====================================================================
-- Características:
--   • Claves sustitutas (IDENTITY) independientes del sistema OLTP
--   • Desnormalizadas para consultas rápidas
--   • Preparadas para dashboards y reportes BI
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- DIM-1. DimTiempo — Dimensión Tiempo (OBLIGATORIA para BI)
-- ─────────────────────────────────────────────────────────────────────
-- Permite analizar: ventas por mes, por trimestre, por año,
-- comparaciones históricas, tendencias estacionales.
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimTiempo')
BEGIN
    CREATE TABLE DimTiempo (
        idTiempo        INT IDENTITY(1,1) NOT NULL,
        Fecha           DATE              NOT NULL,
        Anio            INT               NOT NULL,
        Mes             INT               NOT NULL,
        NombreMes       NVARCHAR(20)      NOT NULL,
        Trimestre       INT               NOT NULL,
        Dia             INT               NOT NULL,
        DiaSemana       INT               NOT NULL,
        NombreDia       NVARCHAR(20)      NOT NULL,
        Semana          INT               NOT NULL,
        EsFinDeSemana   BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_DimTiempo PRIMARY KEY (idTiempo),
        CONSTRAINT UQ_DimTiempo_Fecha UNIQUE (Fecha)
    );

    -- Índices para consultas analíticas frecuentes
    CREATE NONCLUSTERED INDEX IX_DimTiempo_Anio_Mes
        ON DimTiempo (Anio, Mes);

    CREATE NONCLUSTERED INDEX IX_DimTiempo_Trimestre
        ON DimTiempo (Anio, Trimestre);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- Poblar DimTiempo automáticamente (2023-01-01 a 2026-12-31)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM DimTiempo)
BEGIN
    DECLARE @fecha DATE = '2023-01-01';
    DECLARE @fin   DATE = '2026-12-31';

    WHILE @fecha <= @fin
    BEGIN
        INSERT INTO DimTiempo (
            Fecha, Anio, Mes, NombreMes, Trimestre,
            Dia, DiaSemana, NombreDia, Semana, EsFinDeSemana
        )
        VALUES (
            @fecha,
            YEAR(@fecha),
            MONTH(@fecha),
            DATENAME(MONTH, @fecha),
            DATEPART(QUARTER, @fecha),
            DAY(@fecha),
            DATEPART(WEEKDAY, @fecha),
            DATENAME(WEEKDAY, @fecha),
            DATEPART(WEEK, @fecha),
            CASE WHEN DATEPART(WEEKDAY, @fecha) IN (1, 7) THEN 1 ELSE 0 END
        );

        SET @fecha = DATEADD(DAY, 1, @fecha);
    END
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-2. DimCliente — Dimensión Cliente (desnormalizada)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimCliente')
BEGIN
    CREATE TABLE DimCliente (
        idCliente       INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        ClienteIDOrigen INT               NOT NULL,  -- Clave natural del OLTP
        NombreCompleto  NVARCHAR(200)     NOT NULL,
        Email           NVARCHAR(200)     NULL,
        Telefono        NVARCHAR(100)     NULL,
        Ciudad          NVARCHAR(100)     NULL,
        Pais            NVARCHAR(100)     NULL,
        Segmento        NVARCHAR(50)      NOT NULL DEFAULT N'General',
        FechaRegistro   DATE              NULL,
        Activo          BIT               NOT NULL DEFAULT 1,
        FechaCargaDW    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimCliente PRIMARY KEY (idCliente),
        CONSTRAINT UQ_DimCliente_Origen UNIQUE (ClienteIDOrigen)
    );

    CREATE NONCLUSTERED INDEX IX_DimCliente_Pais
        ON DimCliente (Pais);

    CREATE NONCLUSTERED INDEX IX_DimCliente_Segmento
        ON DimCliente (Segmento);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-3. DimCategoria — Dimensión Categoría
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimCategoria')
BEGIN
    CREATE TABLE DimCategoria (
        idCategoria       INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        CategoriaIDOrigen INT               NULL,      -- Clave natural del OLTP
        NombreCategoria   NVARCHAR(100)     NOT NULL,
        Descripcion       NVARCHAR(255)     NULL,
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimCategoria PRIMARY KEY (idCategoria),
        CONSTRAINT UQ_DimCategoria_Nombre UNIQUE (NombreCategoria)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-4. DimProducto — Dimensión Producto (desnormalizada con categoría)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimProducto')
BEGIN
    CREATE TABLE DimProducto (
        idProducto        INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        ProductoIDOrigen  INT               NOT NULL,  -- Clave natural del OLTP
        NombreProducto    NVARCHAR(200)     NOT NULL,
        Categoria         NVARCHAR(100)     NOT NULL,  -- Desnormalizado desde DimCategoria
        Precio            DECIMAL(10,2)     NOT NULL,
        Estado            NVARCHAR(20)      NOT NULL DEFAULT N'Activo',
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimProducto PRIMARY KEY (idProducto),
        CONSTRAINT UQ_DimProducto_Origen UNIQUE (ProductoIDOrigen)
    );

    CREATE NONCLUSTERED INDEX IX_DimProducto_Categoria
        ON DimProducto (Categoria);
END
GO

-- =====================================================================
--               TABLA DE HECHOS (CENTRO DEL MODELO)
-- =====================================================================
-- FactVentas contiene las métricas cuantificables del negocio.
-- Cada fila = una línea de detalle de venta.
-- Las FK apuntan a las dimensiones (estrella).
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- FACT. FactVentas — Tabla de Hechos principal
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'FactVentas')
BEGIN
    CREATE TABLE FactVentas (
        idHecho           INT IDENTITY(1,1) NOT NULL,  -- PK autoincremental
        -- Claves foráneas a dimensiones (modelo estrella)
        idCliente         INT               NOT NULL,
        idProducto        INT               NOT NULL,
        idTiempo          INT               NOT NULL,
        idCategoria       INT               NOT NULL,
        -- Métricas / Medidas
        Cantidad          INT               NOT NULL,
        PrecioUnitario    DECIMAL(10,2)     NOT NULL,
        TotalVenta        DECIMAL(12,2)     NOT NULL,
        -- Atributos degenerados (de la venta original)
        VentaIDOrigen     INT               NOT NULL,
        EstadoVenta       NVARCHAR(50)      NOT NULL,
        -- Auditoría
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_FactVentas PRIMARY KEY (idHecho),
        CONSTRAINT FK_Fact_DimCliente
            FOREIGN KEY (idCliente)   REFERENCES DimCliente(idCliente),
        CONSTRAINT FK_Fact_DimProducto
            FOREIGN KEY (idProducto)  REFERENCES DimProducto(idProducto),
        CONSTRAINT FK_Fact_DimTiempo
            FOREIGN KEY (idTiempo)    REFERENCES DimTiempo(idTiempo),
        CONSTRAINT FK_Fact_DimCategoria
            FOREIGN KEY (idCategoria) REFERENCES DimCategoria(idCategoria)
    );

    -- ─── Índices optimizados para consultas analíticas ───
    -- Índices en cada FK (rendimiento en JOINs con dimensiones)
    CREATE NONCLUSTERED INDEX IX_Fact_Cliente
        ON FactVentas (idCliente);

    CREATE NONCLUSTERED INDEX IX_Fact_Producto
        ON FactVentas (idProducto);

    CREATE NONCLUSTERED INDEX IX_Fact_Tiempo
        ON FactVentas (idTiempo);

    CREATE NONCLUSTERED INDEX IX_Fact_Categoria
        ON FactVentas (idCategoria);

    -- Índice compuesto para consultas de ventas por tiempo y categoría
    CREATE NONCLUSTERED INDEX IX_Fact_Tiempo_Categoria
        ON FactVentas (idTiempo, idCategoria)
        INCLUDE (Cantidad, TotalVenta);

    -- Índice para agregaciones por cliente y tiempo
    CREATE NONCLUSTERED INDEX IX_Fact_Cliente_Tiempo
        ON FactVentas (idCliente, idTiempo)
        INCLUDE (TotalVenta);
END
GO

-- =====================================================================
--            PROCEDIMIENTOS ETL: CARGA STAGING → DW
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- ETL-1. sp_CargarDimCategoria
-- Carga la dimensión categoría desde staging de productos
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimCategoria', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimCategoria;
GO

CREATE PROCEDURE sp_CargarDimCategoria
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══ INSERT: Categorías nuevas con estandarización a UPPER ═══
    INSERT INTO DimCategoria (NombreCategoria, Descripcion)
    SELECT DISTINCT
        UPPER(LTRIM(RTRIM(s.categoria))),
        N'Categoría importada desde staging'
    FROM stg_productos s
    WHERE s.procesado = 0
      AND s.categoria IS NOT NULL
      AND LTRIM(RTRIM(s.categoria)) <> ''
      AND NOT EXISTS (
          SELECT 1 FROM DimCategoria d
          WHERE d.NombreCategoria = UPPER(LTRIM(RTRIM(s.categoria)))
      );

    PRINT '✅ DimCategoria cargada (con Data Cleansing).';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-2. sp_CargarDimCliente
-- Carga la dimensión cliente desde staging
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimCliente', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimCliente;
GO

CREATE PROCEDURE sp_CargarDimCliente
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══ PASO 1: SCD TIPO 1 — Actualizar clientes existentes que cambiaron ═══
    -- Lookup por Business Key (ClienteIDOrigen). Solo actualiza si hay cambio real.
    UPDATE d SET
        d.NombreCompleto = UPPER(LTRIM(RTRIM(ISNULL(s.nombre, ''))) + ' ' + LTRIM(RTRIM(ISNULL(s.apellido, '')))),
        d.Email          = ISNULL(NULLIF(LTRIM(RTRIM(s.email)),    ''), N'N/A'),
        d.Telefono       = ISNULL(NULLIF(LTRIM(RTRIM(s.telefono)), ''), N'N/A'),
        d.Ciudad         = UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.ciudad)), ''), N'DESCONOCIDO')),
        d.Pais           = UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.pais)),   ''), N'DESCONOCIDO')),
        d.Segmento       = UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.segmento)), ''), N'GENERAL')),
        d.FechaCargaDW   = GETDATE()
    FROM DimCliente d
    INNER JOIN stg_clientes s
        ON d.ClienteIDOrigen = CAST(s.cliente_id AS INT)
    WHERE s.procesado = 0
      AND TRY_CAST(s.cliente_id AS INT) IS NOT NULL
      -- Solo actualiza si AL MENOS UN atributo cambió (evita writes innecesarios)
      AND (
          d.NombreCompleto <> UPPER(LTRIM(RTRIM(ISNULL(s.nombre, ''))) + ' ' + LTRIM(RTRIM(ISNULL(s.apellido, ''))))
       OR ISNULL(d.Email,    '') <> ISNULL(NULLIF(LTRIM(RTRIM(s.email)),    ''), N'N/A')
       OR ISNULL(d.Telefono, '') <> ISNULL(NULLIF(LTRIM(RTRIM(s.telefono)), ''), N'N/A')
       OR ISNULL(d.Ciudad,   '') <> UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.ciudad)), ''), N'DESCONOCIDO'))
       OR ISNULL(d.Pais,     '') <> UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.pais)),   ''), N'DESCONOCIDO'))
       OR ISNULL(d.Segmento, '') <> UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.segmento)), ''), N'GENERAL'))
      );

    -- ═══ PASO 2: INSERT de clientes nuevos (con Data Cleansing) ═══
    INSERT INTO DimCliente (
        ClienteIDOrigen, NombreCompleto, Email, Telefono,
        Ciudad, Pais, Segmento, FechaRegistro
    )
    SELECT DISTINCT
        CAST(s.cliente_id AS INT),
        UPPER(LTRIM(RTRIM(ISNULL(s.nombre, ''))) + ' ' + LTRIM(RTRIM(ISNULL(s.apellido, '')))),
        ISNULL(NULLIF(LTRIM(RTRIM(s.email)),    ''), N'N/A'),
        ISNULL(NULLIF(LTRIM(RTRIM(s.telefono)), ''), N'N/A'),
        UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.ciudad)), ''), N'DESCONOCIDO')),
        UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.pais)),   ''), N'DESCONOCIDO')),
        UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.segmento)), ''), N'GENERAL')),
        GETDATE()
    FROM stg_clientes s
    WHERE s.procesado = 0
      AND s.cliente_id IS NOT NULL
      AND TRY_CAST(s.cliente_id AS INT) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM DimCliente d
          WHERE d.ClienteIDOrigen = CAST(s.cliente_id AS INT)
      );

    -- Marcar como procesados
    UPDATE stg_clientes SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ DimCliente cargada (SCD Tipo 1 + Data Cleansing).';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-3. sp_CargarDimProducto
-- Carga la dimensión producto desde staging (desnormalizada con categoría)
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimProducto', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimProducto;
GO

CREATE PROCEDURE sp_CargarDimProducto
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══ PASO 1: SCD TIPO 1 — Actualizar productos existentes que cambiaron ═══
    -- Lookup por Business Key (ProductoIDOrigen). Solo actualiza si hay cambio real.
    UPDATE d SET
        d.NombreProducto = UPPER(LTRIM(RTRIM(s.nombre))),
        d.Categoria      = UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.categoria)), ''), N'SIN CATEGORÍA')),
        d.Precio         = CAST(s.precio AS DECIMAL(10,2)),
        d.FechaCargaDW   = GETDATE()
    FROM DimProducto d
    INNER JOIN stg_productos s
        ON d.ProductoIDOrigen = CAST(s.producto_id AS INT)
    WHERE s.procesado = 0
      AND TRY_CAST(s.producto_id AS INT) IS NOT NULL
      AND TRY_CAST(s.precio AS DECIMAL(10,2)) IS NOT NULL
      -- Solo actualiza si AL MENOS UN atributo cambió
      AND (
          d.NombreProducto <> UPPER(LTRIM(RTRIM(s.nombre)))
       OR d.Categoria      <> UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.categoria)), ''), N'SIN CATEGORÍA'))
       OR d.Precio         <> CAST(s.precio AS DECIMAL(10,2))
      );

    -- ═══ PASO 2: INSERT de productos nuevos (con Data Cleansing) ═══
    INSERT INTO DimProducto (
        ProductoIDOrigen, NombreProducto, Categoria, Precio, Estado
    )
    SELECT DISTINCT
        CAST(s.producto_id AS INT),
        UPPER(LTRIM(RTRIM(s.nombre))),
        UPPER(ISNULL(NULLIF(LTRIM(RTRIM(s.categoria)), ''), N'SIN CATEGORÍA')),
        CAST(s.precio AS DECIMAL(10,2)),
        N'ACTIVO'
    FROM stg_productos s
    WHERE s.procesado = 0
      AND s.producto_id IS NOT NULL
      AND TRY_CAST(s.producto_id AS INT) IS NOT NULL
      AND TRY_CAST(s.precio AS DECIMAL(10,2)) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM DimProducto d
          WHERE d.ProductoIDOrigen = CAST(s.producto_id AS INT)
      );

    -- Marcar como procesados
    UPDATE stg_productos SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ DimProducto cargada (SCD Tipo 1 + Data Cleansing).';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-4. sp_CargarFactVentas
-- Carga la tabla de hechos cruzando staging con dimensiones.
-- Las dimensiones DEBEN estar cargadas previamente.
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarFactVentas', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarFactVentas;
GO

CREATE PROCEDURE sp_CargarFactVentas
AS
BEGIN
    SET NOCOUNT ON;

    -- Insertar hechos: cada línea de detalle = un hecho
    INSERT INTO FactVentas (
        idCliente, idProducto, idTiempo, idCategoria,
        Cantidad, PrecioUnitario, TotalVenta,
        VentaIDOrigen, EstadoVenta
    )
    SELECT
        dc.idCliente,
        dp.idProducto,
        dt.idTiempo,
        dcat.idCategoria,
        CAST(sd.cantidad AS INT),
        CAST(sd.precio_unitario AS DECIMAL(10,2)),
        CAST(sd.total_linea AS DECIMAL(12,2)),
        CAST(sv.venta_id AS INT),
        sv.estado
    FROM stg_detalle_ventas sd
    -- Join con staging ventas para obtener cliente y fecha
    INNER JOIN stg_ventas sv
        ON sd.venta_id = sv.venta_id
           AND sv.procesado = 0
    -- Join con dimensión cliente
    INNER JOIN DimCliente dc
        ON dc.ClienteIDOrigen = CAST(sv.cliente_id AS INT)
    -- Join con dimensión producto
    INNER JOIN DimProducto dp
        ON dp.ProductoIDOrigen = CAST(sd.producto_id AS INT)
    -- Join con dimensión tiempo via fecha de venta
    INNER JOIN DimTiempo dt
        ON dt.Fecha = CAST(sv.fecha_venta AS DATE)
    -- Join con dimensión categoría via producto
    INNER JOIN DimCategoria dcat
        ON dcat.NombreCategoria = dp.Categoria
    WHERE sd.procesado = 0
      AND TRY_CAST(sd.cantidad AS INT) IS NOT NULL
      AND TRY_CAST(sd.total_linea AS DECIMAL(12,2)) IS NOT NULL
      -- Evitar duplicados
      AND NOT EXISTS (
          SELECT 1 FROM FactVentas f
          WHERE f.VentaIDOrigen = CAST(sv.venta_id AS INT)
            AND f.idProducto    = dp.idProducto
      );

    -- Marcar staging como procesado
    UPDATE stg_ventas         SET procesado = 1 WHERE procesado = 0;
    UPDATE stg_detalle_ventas SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ FactVentas cargada.';
END
GO


-- =====================================================================
--         CONSULTAS ANALÍTICAS DE EJEMPLO (DASHBOARDS / BI)
-- =====================================================================

-- ─── KPI 1: Ventas totales por mes y año ─────────────────────────────
-- SELECT
--     t.Anio, t.Mes, t.NombreMes,
--     COUNT(f.idHecho)       AS NumeroVentas,
--     SUM(f.Cantidad)        AS UnidadesTotales,
--     SUM(f.TotalVenta)      AS IngresoTotal,
--     AVG(f.TotalVenta)      AS TicketPromedio
-- FROM FactVentas f
-- INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
-- GROUP BY t.Anio, t.Mes, t.NombreMes
-- ORDER BY t.Anio, t.Mes;

-- ─── KPI 2: Top 10 clientes por ingreso ──────────────────────────────
-- SELECT TOP 10
--     c.NombreCompleto, c.Pais, c.Segmento,
--     COUNT(f.idHecho)   AS NumeroCompras,
--     SUM(f.TotalVenta)  AS TotalGastado
-- FROM FactVentas f
-- INNER JOIN DimCliente c ON f.idCliente = c.idCliente
-- GROUP BY c.NombreCompleto, c.Pais, c.Segmento
-- ORDER BY TotalGastado DESC;

-- ─── KPI 3: Ventas por categoría ─────────────────────────────────────
-- SELECT
--     cat.NombreCategoria,
--     COUNT(f.idHecho)       AS NumeroVentas,
--     SUM(f.Cantidad)        AS UnidadesVendidas,
--     SUM(f.TotalVenta)      AS IngresoTotal,
--     AVG(f.PrecioUnitario)  AS PrecioPromedio
-- FROM FactVentas f
-- INNER JOIN DimCategoria cat ON f.idCategoria = cat.idCategoria
-- GROUP BY cat.NombreCategoria
-- ORDER BY IngresoTotal DESC;

-- ─── KPI 4: Ventas por trimestre ─────────────────────────────────────
-- SELECT
--     t.Anio,
--     t.Trimestre,
--     SUM(f.TotalVenta)  AS IngresoTrimestral,
--     SUM(f.Cantidad)    AS UnidadesTrimestral
-- FROM FactVentas f
-- INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
-- GROUP BY t.Anio, t.Trimestre
-- ORDER BY t.Anio, t.Trimestre;

-- ─── KPI 5: Productos más vendidos ───────────────────────────────────
-- SELECT TOP 10
--     p.NombreProducto, p.Categoria, p.Precio,
--     SUM(f.Cantidad)    AS UnidadesVendidas,
--     SUM(f.TotalVenta)  AS IngresoTotal
-- FROM FactVentas f
-- INNER JOIN DimProducto p ON f.idProducto = p.idProducto
-- GROUP BY p.NombreProducto, p.Categoria, p.Precio
-- ORDER BY UnidadesVendidas DESC;

PRINT '✅ VentasAnalisisDB (Data Warehouse) — estructura base creada.';
GO

-- =====================================================================
--   EXTENSIÓN: WATERMARK · SCD2 · NUEVAS DIMENSIONES · NUEVOS HECHOS
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- CTRL-1. ETL_DimensionWatermark — Control de carga incremental
-- Registra la última marca temporal procesada por entidad.
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'ETL_DimensionWatermark')
BEGIN
    CREATE TABLE ETL_DimensionWatermark (
        EntityName       NVARCHAR(100) NOT NULL,
        LastExtractedAt  DATETIME2     NOT NULL,
        UpdatedAt        DATETIME2     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_Watermark PRIMARY KEY (EntityName)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-5. DimClientesSCD2 — Historial de cambios (SCD Tipo 2)
-- SK  : ClienteSK (IDENTITY)
-- BK  : ClienteID
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimClientesSCD2')
BEGIN
    CREATE TABLE DimClientesSCD2 (
        ClienteSK   INT IDENTITY(1,1) NOT NULL,
        ClienteID   INT               NOT NULL,
        Nombre      NVARCHAR(100)     NOT NULL DEFAULT N'DESCONOCIDO',
        Apellido    NVARCHAR(100)     NOT NULL DEFAULT N'DESCONOCIDO',
        Email       NVARCHAR(200)     NOT NULL DEFAULT N'N/A',
        Ciudad      NVARCHAR(100)     NOT NULL DEFAULT N'DESCONOCIDO',
        Pais        NVARCHAR(100)     NOT NULL DEFAULT N'DESCONOCIDO',
        Segmento    NVARCHAR(50)      NOT NULL DEFAULT N'GENERAL',
        StartDate   DATETIME2         NOT NULL DEFAULT GETDATE(),
        EndDate     DATETIME2         NOT NULL DEFAULT '9999-12-31',
        IsCurrent   BIT               NOT NULL DEFAULT 1,
        LoadedAt    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimClientesSCD2 PRIMARY KEY (ClienteSK)
    );
    CREATE NONCLUSTERED INDEX IX_DimSCD2_ClienteID_Current
        ON DimClientesSCD2 (ClienteID, IsCurrent);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-6. DimEstadosPedido — Catálogo controlado de estados
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimEstadosPedido')
BEGIN
    CREATE TABLE DimEstadosPedido (
        EstadoID  INT IDENTITY(1,1) NOT NULL,
        Estado    NVARCHAR(50)      NOT NULL,
        CONSTRAINT PK_DimEstadosPedido PRIMARY KEY (EstadoID),
        CONSTRAINT UQ_DimEstadosPedido_Estado UNIQUE (Estado)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- FACT-2. FactPedidos — Hecho a nivel cabecera de pedido
-- PedidoID (SK técnico), OrderNaturalKey (BK origen)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'FactPedidos')
BEGIN
    CREATE TABLE FactPedidos (
        PedidoID        INT IDENTITY(1,1) NOT NULL,
        OrderNaturalKey NVARCHAR(50)      NOT NULL,
        ClienteID       INT               NOT NULL,
        Fecha           DATE              NOT NULL,
        EstadoID        INT               NOT NULL DEFAULT 1,
        Total           DECIMAL(12,2)     NOT NULL DEFAULT 0,
        FechaCargaDW    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_FactPedidos PRIMARY KEY (PedidoID),
        CONSTRAINT FK_FactPedidos_Estado
            FOREIGN KEY (EstadoID) REFERENCES DimEstadosPedido(EstadoID)
    );
    CREATE UNIQUE NONCLUSTERED INDEX UQ_FactPedidos_NaturalKey
        ON FactPedidos (OrderNaturalKey);
    CREATE NONCLUSTERED INDEX IX_FactPedidos_Fecha
        ON FactPedidos (Fecha);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- FACT-3. FactDetallesPedido — Líneas de pedido por producto
-- Usa PedidoID técnico (SK), no OrderID natural.
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'FactDetallesPedido')
BEGIN
    CREATE TABLE FactDetallesPedido (
        DetalleID      INT IDENTITY(1,1) NOT NULL,
        PedidoID       INT               NOT NULL,
        ProductoID     INT               NOT NULL,
        Cantidad       INT               NOT NULL,
        PrecioUnitario DECIMAL(10,2)     NOT NULL,
        Total          DECIMAL(12,2)     NOT NULL,
        FechaCargaDW   DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_FactDetallesPedido PRIMARY KEY (DetalleID),
        CONSTRAINT FK_FDetalle_Pedido
            FOREIGN KEY (PedidoID) REFERENCES FactPedidos(PedidoID)
    );
    CREATE NONCLUSTERED INDEX IX_FDetalle_Pedido
        ON FactDetallesPedido (PedidoID, ProductoID);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- FACT-4. FactFacturacion — Total facturado por pedido
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'FactFacturacion')
BEGIN
    CREATE TABLE FactFacturacion (
        FacturaID      INT IDENTITY(1,1) NOT NULL,
        PedidoID       INT               NOT NULL,
        TotalFacturado DECIMAL(12,2)     NOT NULL,
        FechaCargaDW   DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_FactFacturacion PRIMARY KEY (FacturaID),
        CONSTRAINT UQ_FactFacturacion_Pedido UNIQUE (PedidoID),
        CONSTRAINT FK_FFacturacion_Pedido
            FOREIGN KEY (PedidoID) REFERENCES FactPedidos(PedidoID)
    );
END
GO

-- =====================================================================
--  NUEVOS STORED PROCEDURES
-- =====================================================================

-- ETL-6. sp_CargarDimEstadosPedido — MERGE de estados base
IF OBJECT_ID('sp_CargarDimEstadosPedido', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimEstadosPedido;
GO
CREATE PROCEDURE sp_CargarDimEstadosPedido
AS
BEGIN
    SET NOCOUNT ON;
    MERGE DimEstadosPedido AS target
    USING (SELECT Estado FROM (VALUES
        (N'UNKNOWN'),(N'PENDING'),(N'SHIPPED'),(N'DELIVERED'),(N'CANCELLED')
    ) v(Estado)) AS source (Estado)
    ON target.Estado = source.Estado
    WHEN NOT MATCHED THEN INSERT (Estado) VALUES (source.Estado);
    PRINT '  DimEstadosPedido OK.';
END
GO

-- ETL-7. sp_CargarFactPedidos
-- INSERT + UPDATE usando OrderNaturalKey como BK.
-- Lee procesado = 0 porque se ejecuta ANTES de que sp_CargarFactVentas
-- marque stg_ventas como procesado = 1.
-- El marcado final (procesado = 1) lo realiza sp_CargarFactVentas.
IF OBJECT_ID('sp_CargarFactPedidos', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarFactPedidos;
GO
CREATE PROCEDURE sp_CargarFactPedidos
AS
BEGIN
    SET NOCOUNT ON;

    -- INSERT pedidos nuevos (procesado = 0: aún no marcados)
    INSERT INTO FactPedidos (OrderNaturalKey, ClienteID, Fecha, EstadoID, Total)
    SELECT
        sv.venta_id,
        CAST(sv.cliente_id AS INT),
        CAST(sv.fecha_venta AS DATE),
        ISNULL(ep.EstadoID, 1),
        ISNULL(agg.TotalPedido, 0)
    FROM stg_ventas sv
    LEFT JOIN DimEstadosPedido ep
        ON ep.Estado = UPPER(LTRIM(RTRIM(ISNULL(sv.estado, N'UNKNOWN'))))
    LEFT JOIN (
        SELECT venta_id, SUM(CAST(total_linea AS DECIMAL(12,2))) AS TotalPedido
        FROM stg_detalle_ventas
        WHERE TRY_CAST(total_linea AS DECIMAL(12,2)) IS NOT NULL
        GROUP BY venta_id
    ) agg ON agg.venta_id = sv.venta_id
    WHERE sv.procesado = 0
      AND TRY_CAST(sv.cliente_id AS INT) IS NOT NULL
      AND TRY_CAST(sv.fecha_venta AS DATE) IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM FactPedidos fp WHERE fp.OrderNaturalKey = sv.venta_id);

    -- UPDATE pedidos existentes que cambiaron de estado
    UPDATE fp SET
        fp.EstadoID     = ISNULL(ep.EstadoID, 1),
        fp.FechaCargaDW = GETDATE()
    FROM FactPedidos fp
    INNER JOIN stg_ventas sv ON fp.OrderNaturalKey = sv.venta_id
    LEFT JOIN DimEstadosPedido ep
        ON ep.Estado = UPPER(LTRIM(RTRIM(ISNULL(sv.estado, N'UNKNOWN'))))
    WHERE sv.procesado = 0
      AND fp.EstadoID <> ISNULL(ep.EstadoID, 1);

    PRINT '  FactPedidos OK.';
END
GO

-- ETL-8. sp_CargarFactDetallesPedido
-- Mapea OrderID natural → PedidoID técnico (SK). Calcula PrecioUnitario.
IF OBJECT_ID('sp_CargarFactDetallesPedido', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarFactDetallesPedido;
GO
CREATE PROCEDURE sp_CargarFactDetallesPedido
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO FactDetallesPedido (PedidoID, ProductoID, Cantidad, PrecioUnitario, Total)
    SELECT
        fp.PedidoID,
        CAST(sd.producto_id AS INT),
        CAST(sd.cantidad AS INT),
        CASE WHEN CAST(sd.cantidad AS DECIMAL(10,2)) > 0
             THEN CAST(sd.total_linea AS DECIMAL(12,2)) / CAST(sd.cantidad AS DECIMAL(10,2))
             ELSE 0 END,
        CAST(sd.total_linea AS DECIMAL(12,2))
    FROM stg_detalle_ventas sd
    INNER JOIN FactPedidos fp ON fp.OrderNaturalKey = sd.venta_id
    WHERE sd.procesado = 1
      AND TRY_CAST(sd.producto_id AS INT) IS NOT NULL
      AND TRY_CAST(sd.cantidad AS INT) > 0
      AND TRY_CAST(sd.total_linea AS DECIMAL(12,2)) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM FactDetallesPedido fd
          WHERE fd.PedidoID   = fp.PedidoID
            AND fd.ProductoID = CAST(sd.producto_id AS INT)
            AND fd.Cantidad   = CAST(sd.cantidad AS INT)
            AND fd.Total      = CAST(sd.total_linea AS DECIMAL(12,2))
      );

    PRINT '  FactDetallesPedido OK.';
END
GO

-- ETL-9. sp_CargarFactFacturacion
-- Total facturado por pedido, reutiliza FactPedidos ya cargado.
IF OBJECT_ID('sp_CargarFactFacturacion', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarFactFacturacion;
GO
CREATE PROCEDURE sp_CargarFactFacturacion
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO FactFacturacion (PedidoID, TotalFacturado)
    SELECT fp.PedidoID, fp.Total
    FROM FactPedidos fp
    WHERE NOT EXISTS (
        SELECT 1 FROM FactFacturacion ff WHERE ff.PedidoID = fp.PedidoID
    );

    PRINT '  FactFacturacion OK.';
END
GO

-- =====================================================================
--  sp_EjecutarETLCompleto — VERSION FINAL (9 PASOS)
-- =====================================================================
IF OBJECT_ID('sp_EjecutarETLCompleto', 'P') IS NOT NULL
    DROP PROCEDURE sp_EjecutarETLCompleto;
GO

CREATE PROCEDURE sp_EjecutarETLCompleto
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '══════════════════════════════════════════════════════════';
    PRINT '  🔄 INICIO DEL PROCESO ETL → DATA WAREHOUSE (v2)';
    PRINT '══════════════════════════════════════════════════════════';

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ─── DIMENSIONES ──────────────────────────────────────────
        PRINT '  [1/9] DimEstadosPedido (MERGE estados base)...';
        EXEC sp_CargarDimEstadosPedido;

        PRINT '  [2/9] DimCategoria (Data Cleansing)...';
        EXEC sp_CargarDimCategoria;

        PRINT '  [3/9] DimCliente (SCD Tipo 1 + Data Cleansing)...';
        EXEC sp_CargarDimCliente;

        PRINT '  [4/9] DimProducto (SCD Tipo 1 + Data Cleansing)...';
        EXEC sp_CargarDimProducto;

        -- ─── HECHOS ───────────────────────────────────────────────
        PRINT '  [5/9] FactVentas (granularidad línea de detalle)...';
        EXEC sp_CargarFactVentas;

        PRINT '  [6/9] FactPedidos (cabecera + lookup estado)...';
        EXEC sp_CargarFactPedidos;

        PRINT '  [7/9] FactDetallesPedido (líneas → SK técnico)...';
        EXEC sp_CargarFactDetallesPedido;

        PRINT '  [8/9] FactFacturacion (total facturado por pedido)...';
        EXEC sp_CargarFactFacturacion;

        COMMIT TRANSACTION;

        -- ─── LIMPIEZA DE STAGING (fuera de transacción) ───────────
        PRINT '  [9/9] Limpiando staging procesado (> 30 días)...';
        DELETE FROM stg_clientes        WHERE procesado = 1 AND fecha_carga < DATEADD(DAY, -30, GETDATE());
        DELETE FROM stg_productos       WHERE procesado = 1 AND fecha_carga < DATEADD(DAY, -30, GETDATE());
        DELETE FROM stg_ventas          WHERE procesado = 1 AND fecha_carga < DATEADD(DAY, -30, GETDATE());
        DELETE FROM stg_detalle_ventas  WHERE procesado = 1 AND fecha_carga < DATEADD(DAY, -30, GETDATE());
        DELETE FROM stg_api_comentarios WHERE procesado = 1 AND fecha_carga < DATEADD(DAY, -30, GETDATE());

        PRINT '══════════════════════════════════════════════════════════';
        PRINT '  ✅ ETL COMPLETADO EXITOSAMENTE (9/9 pasos)';
        PRINT '══════════════════════════════════════════════════════════';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT '  ❌ ERROR EN ETL: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
GO

PRINT '✅ VentasAnalisisDB — extensión completa aplicada correctamente.';
GO

GO


