import pandas as pd
from pathlib import Path
import duckdb


# Getting the data
def translate_english_degrees(row):
    """
    Translate the "nivel global" column to English degree categories.

    Args:
        row (pd.Series): A row from a DataFrame containing the "nivel global" column.

    Returns:
        str: The English translation of the degree category.
        - "Grads" for "Posgrado"
        - "Undergrads" for "Pregrado"
        - "Non-Degree" for other values
    """
    if row["nivel global"] == "Posgrado":
        return "Grads"
    elif row["nivel global"] == "Pregrado":
        return "Undergrads"
    else:
        return "Non-Degree"


def translate_area_del_conocimiento(row):
    """
    Translate the "área del conocimiento" (Study field) column to English field categories.

    Args:
        row (pd.Series): A row from a DataFrame containing the "área del conocimiento" column.

    Returns:
        str: The English translation of the field category based on a predefined dictionary.
    """
    area = row["área del conocimiento"]

    translation_dict = {
        "Salud": "Health",
        "Tecnología": "Technology",
        "Ciencias Básicas": "Basic Sciences",
        "Administración y Comercio": "Business and Commerce",
        "Agropecuaria": "Agricultural Sciences",
        "Ciencias Sociales": "Social Sciences",
        "Arte y Arquitectura": "Arts and Architecture",
        "Educación": "Education",
        "Derecho": "Law",
        "Humanidades": "Humanities",
    }
    return translation_dict[area]


def translates_degree(row):
    """
    Translate the "carrera clasificación nivel 2" column to English degree types.

    Args:
        row (pd.Series): A row from a DataFrame containing the "carrera clasificación nivel 2" column.

    Returns:
        str: The English translation of the degree type.
        - "Professionals (Undergrad)" for "Carreras Profesionales"
        - "Technical (Undergrad)" for "Carreras Técnicas"
        - "PhD (Grad)" for "Doctorado"
        - "Non-Degree (Grad)" for "Postítulo"
        - "Masters (Grad)" for other values
    """
    value = row["carrera clasificación nivel 2"]
    if value == "Carreras Profesionales":
        return "Professionals (Undergrad)"
    elif value == "Carreras Técnicas":
        return "Technical (Undergrad)"
    elif value == "Doctorado":
        return "PhD (Grad)"
    elif value == "Postítulo":
        return "Non-Degree (Grad)"
    else:
        return "Masters (Grad)"


def translates_institution(row):
    """
    Translate the "clasificación institución nivel 1" column to English institution types.

    Args:
        row (pd.Series): A row from a DataFrame containing the "clasificación institución nivel 1" column.

    Returns:
        str: The English translation of the institution type.
        - "Universities" for "Universidades"
        - "Technical Colleges" for "Centros de Formación Técnica"
        - "Professional Institutes" for other values
    """
    value = row["clasificación institución nivel 1"]
    if value == "Universidades":
        return "Universities"
    elif value == "Centros de Formación Técnica":
        return "Technical Colleges"
    else:
        return "Professional Institutes"


def loads_enrolled_graduated(path_csv: Path) -> pd.DataFrame:
    """
    Load enrollment and graduation data from a CSV file.

    Args:
        path_csv (Path): The path to the CSV file containing enrollment and graduation data.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.
    """
    return pd.read_csv(path_csv, encoding="latin1", sep=None, engine="python")


def loads_cae(cae_path: Path) -> pd.DataFrame:
    """
    Load and clean CAE data from a .txt file.

    Args:
        cae_path (Path): The path to the CAE .txt file.

    Returns:
        pd.DataFrame: A DataFrame containing the cleaned CAE data with:
        - Trailing and leading whitespaces removed from column names.
        - Trailing and leading whitespaces removed from string/object cells.
    """
    cae = pd.read_csv(cae_path, sep=";", engine="pyarrow")
    # Clean trailing and leading whitespaces from column names
    cae.columns = cae.columns.str.strip()

    # Strip leading/trailing whitespace from all string/object cells
    str_cols = cae.select_dtypes(include=["object", "string"]).columns
    if len(str_cols) > 0:
        # .str methods preserve NaN values
        cae[str_cols] = cae[str_cols].apply(lambda col: col.str.strip())
    return cae


def loads_cae_db(
    cae_path: Path, db_path: Path = Path("../data/raw/cae.duckdb")
) -> duckdb.DuckDBPyConnection:
    """
    Load and clean CAE data, then save it to a DuckDB database.

    Args:
        cae_path (Path): The path to the CAE CSV file.
        db_path (Path, optional): The path to the DuckDB database file. Defaults to "../data/raw/cae.duckdb".

    Returns:
        duckdb.DuckDBPyConnection: A connection to the DuckDB database containing the CAE data.

    Behavior:
        - If the database file exists, it connects to the existing database.
        - If the database file does not exist, it:
            1. Loads and cleans the CAE data using `loads_cae`.
            2. Filters rows to include only specific beneficiary types.
            3. Converts year columns to nullable integers after handling floating-point precision.
            4. Creates a new DuckDB database and saves the cleaned data as a table.
    """
    # Load and clean data in streaming mode (pandas only used temporarily)
    if db_path.exists():
        conn = duckdb.connect(str(db_path))
        return conn
    else:
        cae_df = loads_cae(cae_path)
        beneficiarios = ["BENEFICIARIO RENOVANTE", "NUEVO BENEFICIARIO"]
        cae_df = cae_df[cae_df["TIPO_BENEFICIARIO"].isin(beneficiarios)]

        columns = [col.lower() for col in cae_df.columns]
        cae_df.columns = columns

        cae_df["año_operacion"] = (
            pd.to_numeric(cae_df["año_operacion"], errors="coerce")
            .mul(1000)
            .round()
            .astype("Int64")
        )
        cae_df["año_licitacion"] = (
            pd.to_numeric(cae_df["año_licitacion"], errors="coerce")
            .mul(1000)
            .round()
            .astype("Int64")
        )

        # Create DuckDB database and persist table
        conn = duckdb.connect(str(db_path))
        conn.register("cae_df", cae_df)
        conn.execute("CREATE OR REPLACE TABLE cae AS SELECT * FROM cae_df")

        print(f"✅ DuckDB database saved at {db_path}")
        return conn
