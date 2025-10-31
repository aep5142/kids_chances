import pandas as pd
from pathlib import Path
import duckdb
import os

# Global Variables
sa_countries = ["Argentina", "Chile", "Peru", "Colombia", "Bolivia",
                "Eduador", "Venezuela", "Paraguay", "Uruguay", "Brazil"]

def filters_countries_cols(all_data: pd.DataFrame,
                      cols:list,
                      countries:list = sa_countries,
                      ) -> pd.DataFrame:
    filtered_countries = all_data[all_data["country"].isin(countries)].copy()
    filtered_countries = filtered_countries[cols]
    return filtered_countries

# Getting the data
def translate_english_degrees(row):
    if row["nivel global"] == "Posgrado":
        return "Grads"
    elif row["nivel global"] == "Pregrado":
        return "Undergrads"
    else:
        return "Non-Degree"
    
def translate_area_del_conocimiento(row):
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
        "Humanidades": "Humanities"
    }
    return translation_dict[area]


def translates_degree(row):
    value = row["carrera clasificación nivel 2"]
    if value == 'Carreras Profesionales':
        return "Professionals (Undergrad)"
    elif value == 'Carreras Técnicas':
        return "Technical (Undergrad)"
    elif value == "Doctorado":
        return "PhD (Grad)"
    elif value == "Postítulo":
        return "Non-Degree (Grad)"
    else:
        return "Masters (Grad)"

def translates_institution(row):
    value = row["clasificación institución nivel 1"]
    if value == "Universidades":
        return "Universities"
    elif value == "Centros de Formación Técnica":
        return "Technical Colleges"
    else:
        return "Professional Institutes"
    
def loads_enrolled_graduated(path_csv: Path) -> pd.DataFrame:
    return pd.read_csv(path_csv, encoding="latin1", sep=None, engine="python")

def loads_cae(cae_path: Path) -> pd.DataFrame:
    cae = pd.read_csv(cae_path,
                      sep=";",
                      engine='pyarrow')
    # Clean trailing and leading whitespaces from column names
    cae.columns = cae.columns.str.strip()

    # Strip leading/trailing whitespace from all string/object cells
    str_cols = cae.select_dtypes(include=["object", "string"]).columns
    if len(str_cols) > 0:
        # .str methods preserve NaN values
        cae[str_cols] = cae[str_cols].apply(lambda col: col.str.strip())
    return cae

def loads_cae_db(cae_path: Path, db_path: Path = Path("../data/raw/cae.duckdb")) -> duckdb.DuckDBPyConnection:
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

        # Safe conversion from floats like 2.014 -> 2014
        # - coerce non-numeric to NaN
        # - multiply by 1000
        # - round() to avoid float precision truncation (e.g. 2013.9999)
        # - convert to pandas nullable integer type (Int64) to preserve NaNs if any
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



