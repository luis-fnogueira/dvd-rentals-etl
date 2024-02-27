from typing import List

import pandas as pd


class DVDWrangler:

    @staticmethod
    def drop_columns(df: pd.DataFrame, columns: list):

        df = df.drop(columns, axis=1)

        return df

    @staticmethod
    def create_rentals_fact(dataframes: List[pd.DataFrame]):

        pass
