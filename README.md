# Prueba Técnica de Data Engineer: Migración de Datos Financieros

Este repositorio contiene la solución completa para el desafío de migración de datos financieros. El proyecto implementa un pipeline ETL utilizando Python y Pandas, y se ejecuta en un entorno controlado y reproducible gestionado por **Docker y Docker Compose**.

## Arquitectura de la Solución

El proyecto utiliza la imagen oficial de Apache Airflow y Docker Compose para orquestar los servicios necesarios:

1.  **`airflow-scheduler`**: El cerebro de Airflow, responsable de monitorear los DAGs y disparar las tareas según sus dependencias y horarios.
2.  **`airflow-webserver`**: Proporciona la interfaz de usuario web para visualizar, monitorear y gestionar los pipelines.
3.  **`airflow-init`**: Un servicio de un solo uso que inicializa la base de datos de metadatos de Airflow y crea el usuario administrador por defecto.
4.  **`postgres`**: Una base de datos PostgreSQL que actúa como el backend de metadatos de Airflow, una opción más robusta y escalable que SQLite.

## Guía de Configuración y Ejecución

### Prerrequisitos
*   **Docker:** Se requiere [Docker Desktop](https://www.docker.com/products/docker-desktop/) (que incluye Docker Compose) instalado y en ejecución.
*   **Git:** Necesario para clonar el repositorio.

### Paso 1: Clonar el Repositorio

Abre tu terminal y clona este proyecto en tu máquina local.

```bash
git clone https://github.com/andleod/rappi_test
cd rappi_test
```

### Paso 2: Preparar los Archivos de Docker

Dentro del repositorio, encontrarás los siguientes archivos clave que gestionan el entorno de Airflow.

#### `docker-compose.yaml`
Este archivo define y configura todos los servicios de Airflow. Incluye la configuración de red, volúmenes para persistir datos y dependencias entre servicios. *No es necesario modificar este archivo para la ejecución estándar.*

### Paso 3: Construir e Iniciar los Servicios de Airflow

Con Docker Desktop en ejecución, ejecuta el siguiente comando desde la raíz del proyecto. Este comando construirá las imágenes si es necesario, creará los contenedores y los iniciará.

```bash
docker-compose up airflow-init
docker-compose up -d
```

Puedes verificar que los contenedores están corriendo con el comando `docker ps`.

### Paso 4: Acceder a la Interfaz de Airflow

1.  Espera aproximadamente un minuto para que todos los servicios se inicien y estabilicen completamente.
2.  Abre tu navegador web y navega a la siguiente URL: `http://localhost:8080`.
3.  Inicia sesión con las credenciales por defecto:
    *   **Usuario:** `airflow`
    *   **Contraseña:** ``

### Paso 5: Ejecutar el DAG de Migración

1.  En la página principal de Airflow, busca el DAG llamado `financial_data_migration_dag`.
2.  Para habilitarlo, activa el interruptor (toggle) que se encuentra a la izquierda del nombre del DAG.
3.  Para ejecutar el pipeline, haz clic en el botón de "Play" que se encuentra en la columna "Actions" a la derecha.

Puedes hacer clic en el nombre del DAG para monitorear el progreso detallado de la ejecución en la vista de "Grid". Las tareas se colorearán de verde a medida que se completen con éxito.

## Resultados Obtenidos

Una vez que el DAG se complete exitosamente, los resultados se generarán y guardarán en la carpeta `output/` de este proyecto.

*   **`output/final_report.txt`**: Este archivo de texto contiene el reporte final con dos secciones:
    1.  Una lista de las `transaction_id` que están desbalanceadas (débito != crédito).
    2.  Un resumen del saldo final para cada cuenta contable (considerando solo transacciones válidas), ordenado de mayor a menor.

## Gestión del Entorno Docker

Para interactuar con tu entorno de Airflow, puedes usar los siguientes comandos desde la raíz del proyecto:

*   **Para detener todos los servicios de Airflow:**
    ```bash
    docker-compose down
    ```
*   **Para ver los logs de un servicio específico (ej. el scheduler):**
    ```bash
    docker-compose logs --follow airflow-scheduler
    ```
*   **Para limpiar todo el entorno (contenedores, volúmenes y redes):**
    *Aviso: Este comando eliminará permanentemente la base de datos de metadatos de Airflow, perdiendo todo el historial de ejecuciones.*
    ```bash
    docker-compose down --volumes
    ```