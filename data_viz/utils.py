import pandas as pd
from pathlib import Path
import duckdb

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
    return cae

def loads_and_clean_cae_db(cae_path: Path, db_path: Path = Path("cae.duckdb")) -> duckdb.DuckDBPyConnection:
    # Load and clean data in streaming mode (pandas only used temporarily)
    cae_df = loads_cae(cae_path)
    cae_df.columns = cae_df.columns.str.strip()
    beneficiarios = ["BENEFICIARIO RENOVANTE                       ", "NUEVO BENEFICIARIO                           "]
    cae_df = cae_df[cae_df["TIPO_BENEFICIARIO"].isin(beneficiarios)]
    cae_df["AÑO_OPERACION"] = (cae_df["AÑO_OPERACION"].astype(float) * 1000).astype(int)

    # Create DuckDB database and persist table
    conn = duckdb.connect(str(db_path))
    conn.register("cae_df", cae_df)
    conn.execute("CREATE OR REPLACE TABLE cae AS SELECT * FROM cae_df")

    print(f"✅ DuckDB database saved at {db_path}")
    return conn



