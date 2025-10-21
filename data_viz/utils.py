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
    

