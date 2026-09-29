# Oil & Gas Upstream Production Pipeline (Medallion Lakehouse Architecture)

Este proyecto implementa una arquitectura **Lakehouse de Big Data de extremo a extremo** para el monitoreo en tiempo real de la producción diaria de pozos petroleros (*Upstream*). El sistema está diseñado para capturar telemetría SCADA/IoT, evaluar la calidad de los datos y detectar de forma predictiva caídas críticas de presión para evitar paradas de planta no planificadas.

## 🏗️ Arquitectura del Pipeline (Pattern Medallion)

El pipeline está completamente modularizado dentro de **Databricks** utilizando **Delta Lake** y estructurado secuencialmente de la siguiente manera:

*   **`00_Setup_Estructura`**: Configuración e inicialización del entorno SQL, esquemas de bases de datos y creación de tablas Delta optimizadas con propiedades transaccionales ACID.
*   **`01_Ingesta_Bronce`**: 
    *   `01_Simulador_Pozos`: Emulador robusto en Python que recrea la telemetría física de sensores e inyecta de forma intencional fallas comunes de campo (valores nulos, texto malformado, presiones negativas).
    *   `02_Ingesta_Raw_Stream`: Motor de streaming continuo que utiliza **Auto Loader (`cloudFiles`)** para procesar archivos JSON de manera elástica y volcarlos de forma inmutable en la capa Bronce.
*   **`02_Procesamiento_Plata`**: Notebook enfocado en la transformación masiva, limpieza y gobernanza de datos mediante **Spark SQL**. Aplica funciones de `TRY_CAST` para mitigar valores corruptos de campo sin detener el flujo y ejecuta un **`MERGE INTO` (Upsert)** bajo clave compuesta (`id_pozo` + `fecha_hora`) garantizando **Idempotencia Absoluta** (cero duplicados ante reintentos).
*   **`03_Analisis_EDA`**: Análisis Exploratorio de Datos (EDA) en SQL para auditar las métricas de calidad de datos (*Data Quality Metrics*) y establecer estadísticamente los límites operativos y umbrales críticos de las anomalías físicas.
*   **`04_Monitoreo_Oro` / `05_Views_Reportes`**: Capa de negocio y semántica. Clasifica los niveles de riesgo operativo (ALTO, MEDIO, BAJO) e indexa las alertas mediante vistas desacopladas (`Views`) listas para ser consumidas por herramientas de Business Intelligence y tableros interactivos.

## 🛠️ Tecnologías y Conceptos Clave
*   **Plataforma:** Databricks Lakehouse Platform.
*   **Motores de Procesamiento:** PySpark (Structured Streaming & Auto Loader) y Spark SQL.
*   **Formato de Almacenamiento:** Delta Lake (Transacciones ACID, particionamiento de archivos por activos, inmutabilidad).
*   **Principios de Ingeniería de Datos:** Idempotencia, tolerancia a fallas de red, gobernanza de calidad del dato (*Data Quality*) y desacoplamiento de capas semánticas.
