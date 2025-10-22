import pandas as pd

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
        return "Diplomas"
    
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
        return "Diplomas (Grad)"
    else:
        return "Masters (Grad)"
