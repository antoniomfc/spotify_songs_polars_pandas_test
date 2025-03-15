import dotenv
import os
import polars as pl
import time
import pandas as pd
from deltalake import write_deltalake
import numpy as np

dotenv.load_dotenv()
def test_polars():
    
    # 1. Definimos las credenciales de acceso
    storage_options = {
        "account_name": os.getenv("ACCOUNT_NAME"),
        "account_key": os.getenv("ACCOUNT_KEY")
    }

    # 2. Definimos el path del archivo
    path = "abfs://spotify-songs/"

    # 3. Marcamos el inicio del tiempo
    start = time.perf_counter()

    # 4. Nos quedamos con las columnas que nos interesan
    songs_df = (
        pl.scan_csv(path, storage_options=storage_options)
            .collect()
            .select(
                "album_name",
                "name",
                "artists",
                "album_release_date",
                "daily_rank",
                "daily_movement",
                "weekly_movement",
                pl.when(pl.col("country") == "").then(pl.lit("WO")).otherwise(pl.col("country")).alias("country"),
                "snapshot_date",
                "popularity",
                (pl.col("duration_ms") / 1000).cast(pl.Int32).alias("duration_seconds")
            )
    )

    # 5. Total de apariciones en el top 50 de España
    total_aparences_top_50_es_df = (
        songs_df
            .filter(pl.col("country") == "ES")
            .group_by("album_name", "name", "artists")
            .agg(pl.count("name").alias("total_appearances"))
            .sort("total_appearances", descending=True)
    )

    # 6. Canciones que han sido número 1 en España, con su fecha
    top_first_per_day_es_df = (
        songs_df
            .filter((pl.col("country") == "ES") & (pl.col("daily_rank") == 1))
            .select(
                "album_name",
                "name",
                "artists",
                "snapshot_date"
            )
            .sort("snapshot_date")
    )

    # 7. Escribimos los resultados en nuestro Data Lake (Delta Lake)
    path_songs = "abfs://spotify-delta-lake/polars/top_fifty_songs_daily"
    songs_df.write_delta(path_songs, mode="overwrite",  storage_options=storage_options)

    path_total_aparences = "abfs://spotify-delta-lake/polars/total_aparences_es"
    total_aparences_top_50_es_df.write_delta(path_total_aparences, mode="overwrite",  storage_options=storage_options)


    path_top_first_per_day = "abfs://spotify-delta-lake/polars/top_first_per_day_es"
    top_first_per_day_es_df.write_delta(path_top_first_per_day, mode="overwrite", storage_options=storage_options)


    # 7. Cogemos el tiempo final
    end = time.perf_counter()
    print(f"Tiempo de ejecución para Polars: {end - start:.6f} segundos")


def test_pandas():

    # 1. Definimos las credenciales de acceso
    storage_options = {
        "account_name": os.getenv("ACCOUNT_NAME"),
        "account_key": os.getenv("ACCOUNT_KEY")
    }

    # 2. Definimos el path del archivo
    path = "abfs://spotify-songs/universal_top_spotify_songs.csv"

    # 3. Marcamos el inicio del tiempo
    start = time.perf_counter()

    # 4. Leemos el archivo CSV y seleccionamos las columnas que nos interesan
    songs_df = pd.read_csv(path, storage_options=storage_options)
    songs_df['country'] = songs_df['country'].replace("", "WO")
    songs_df['duration_seconds'] = (songs_df['duration_ms'] / 1000).astype(int)
    songs_df = songs_df[[
        "album_name",
        "name",
        "artists",
        "album_release_date",
        "daily_rank",
        "daily_movement",
        "weekly_movement",
        "country",
        "snapshot_date",
        "popularity",
        "duration_seconds"
    ]]

    # 5. Total de apariciones en el top 50 de España
    total_aparences_top_50_es_df = (
        songs_df[songs_df['country'] == "ES"]
            .groupby(["album_name", "name", "artists"])
            .size()
            .reset_index(name='total_appearances')
            .sort_values(by='total_appearances', ascending=False)
    )

    # 6. Canciones que han sido número 1 en España, con su fecha
    top_first_per_day_es_df = (
        songs_df[(songs_df['country'] == "ES") & (songs_df['daily_rank'] == 1)]
            .sort_values(by='snapshot_date')
            .loc[:, ["album_name", "name", "artists", "snapshot_date"]]
    )

    # 7. Guardamos el código en el lago
        # 7. Escribimos los resultados en nuestro Data Lake (Delta Lake)
    path_songs = "abfs://spotify-delta-lake/pandas/top_fifty_songs_daily"
    write_deltalake(path_songs, songs_df, mode="overwrite", storage_options=storage_options)

    path_total_aparences = "abfs://spotify-delta-lake/pandas/total_aparences_es"
    write_deltalake(path_total_aparences, total_aparences_top_50_es_df, mode="overwrite", storage_options=storage_options)


    path_top_first_per_day = "abfs://spotify-delta-lake/pandas/top_first_per_day_es"
    write_deltalake(path_top_first_per_day, top_first_per_day_es_df, mode="overwrite", storage_options=storage_options)

    # 7. Cogemos el tiempo final
    end = time.perf_counter()
    print(f"Tiempo de ejecución para Pandas: {end - start:.6f} segundos")


if __name__ == "__main__":
    test_pandas()
    test_polars()
