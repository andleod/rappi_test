import sqlite3
import pandas as pd
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = '/tmp/financial_data.db'
ACCOUNTS_CSV = 'data/accounts.csv'
JOURNAL_CSV = 'data/journal_entries.csv'
TRANSFORMED_TABLE_NAME = 'transformed_journal_entries'


def setup_database():
    """Carga los archivos CSV a una base de datos SQLite."""
    try:
        logging.info(f"Creando base de datos en {DB_PATH}...")
        conn = sqlite3.connect(DB_PATH)

        # Cargar datos con pandas
        df_accounts = pd.read_csv(ACCOUNTS_CSV)
        df_journal = pd.read_csv(JOURNAL_CSV)

        # Escribir dataframes a tablas SQL
        df_accounts.to_sql('accounts', conn, if_exists='replace', index=False)
        df_journal.to_sql('journal_entries', conn, if_exists='replace', index=False)

        logging.info("Tablas 'accounts' y 'journal_entries' creadas exitosamente.")
        conn.close()
    except Exception as e:
        logging.error(f"Error en la configuración de la base de datos: {e}")
        raise


def transform_records():
    """
    Ejecuta la transformación de datos y aplica la validación de calidad.
    Guarda los resultados en una nueva tabla.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info("Transformando registros...")

        query = """
        SELECT
            j.transaction_id, j.transaction_date, j.account_number, a.account_name,
            CASE WHEN j.amount > 0 THEN j.amount ELSE NULL END AS debit_amount,
            CASE WHEN j.amount < 0 THEN -j.amount ELSE NULL END AS credit_amount,
            CASE
                WHEN STRFTIME('%Y', j.transaction_date) = '2024' AND a.account_name IS NOT NULL THEN 1
                ELSE 0
            END AS is_valid_transaction
        FROM journal_entries j
        LEFT JOIN accounts a ON j.account_number = a.account_number
        """

        df_transformed = pd.read_sql_query(query, conn)

        # --- Alerta de Calidad ---
        total_rows = len(df_transformed)
        if total_rows > 0:
            # En pandas, 'is_valid_transaction' es 0 para FALSO
            invalid_count = df_transformed[df_transformed['is_valid_transaction'] == 0].shape[0]
            invalid_percentage = (invalid_count / total_rows) * 100

            logging.info(f"Porcentaje de transacciones inválidas: {invalid_percentage:.2f}%")

            if invalid_percentage > 5:
                # Falla la tarea si se supera el umbral
                error_msg = f"Alerta de Calidad: {invalid_percentage:.2f}% de transacciones son inválidas, superando el umbral del 5%."
                logging.error(error_msg)
                raise ValueError(error_msg)

        # Guardar los datos transformados en una nueva tabla para uso posterior
        df_transformed.to_sql(TRANSFORMED_TABLE_NAME, conn, if_exists='replace', index=False)
        logging.info(f"Datos transformados guardados en la tabla '{TRANSFORMED_TABLE_NAME}'.")

        conn.close()
    except Exception as e:
        logging.error(f"Error durante la transformación de registros: {e}")
        raise


def get_imbalanced_transactions():
    """Obtiene transacciones desbalanceadas y las retorna como un DataFrame."""
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info("Validando saldos de transacciones...")

        query = """
        SELECT transaction_id
        FROM (
            SELECT
                transaction_id,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_debit,
                SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) AS total_credit
            FROM journal_entries
            GROUP BY transaction_id
        )
        WHERE total_debit <> total_credit
        LIMIT 10;
        """
        df_imbalanced = pd.read_sql_query(query, conn)
        conn.close()
        return df_imbalanced
    except Exception as e:
        logging.error(f"Error obteniendo transacciones desbalanceadas: {e}")
        raise


def get_account_summary():
    """Calcula el resumen de cuentas y lo retorna como un DataFrame."""
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info("Generando resumen de cuentas...")

        # Esta consulta ahora lee de la tabla transformada y pre validada
        query = f"""
        SELECT
            account_name,
            COALESCE(SUM(debit_amount), 0) - COALESCE(SUM(credit_amount), 0) AS final_balance
        FROM
            {TRANSFORMED_TABLE_NAME}
        WHERE
            is_valid_transaction = 1
        GROUP BY
            account_name
        ORDER BY
            final_balance DESC;
        """
        df_summary = pd.read_sql_query(query, conn)
        conn.close()
        return df_summary
    except Exception as e:
        logging.error(f"Error generando el resumen de cuentas: {e}")
        raise