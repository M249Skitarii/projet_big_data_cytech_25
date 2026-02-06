from pyspark.sql.functions import col, to_timestamp, year, month


def assert_no_invalid(condition, message, df):
    """
    Vérifie que la condition est vraie.

    Args:
        condition (bool): La condition à tester.
        message (str): Message d'erreur si la condition est fausse.

    """
    invalid_count = df.filter(condition).count()
    assert invalid_count == 0, f"{message} (invalid rows: {invalid_count})"


def validate_nyc_taxi_df(df):
    """
    Valide un DataFrame NYC Taxi selon toutes les règles métier.

    Lève une AssertionError si une règle n'est pas respectée.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame Spark contenant les données NYC Taxi à valider.


    """

    # VendorID
    assert_no_invalid(
        ~col("VendorID").isin(1, 2),
        "VendorID invalide",
        df
    )

    # Dates valides
    assert_no_invalid(
        to_timestamp(
            col("tpep_pickup_datetime"),
            "yyyy-MM-dd HH:mm:ss"
            ).isNull() |
        to_timestamp(col(
            "tpep_dropoff_datetime"),
            "yyyy-MM-dd HH:mm:ss"
            ).isNull(),
        "Datetime pickup/dropoff invalide ou null",
        df
    )

    # Cohérence temporelle
    assert_no_invalid(
        (year(col("tpep_pickup_datetime")) != 2025) |
        (~month(col("tpep_pickup_datetime")).isin(9, 10, 11)) |
        (col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")),
        "Chronologie ou période invalide",
        df
    )

    # Locations
    assert_no_invalid(
        ~col("PULocationID").between(1, 265) |
        ~col("DOLocationID").between(1, 265),
        "LocationID invalide",
        df
    )

    # Distance
    assert_no_invalid(
        col("trip_distance") <= 0,
        "Trip distance invalide",
        df
    )

    # RateCodeID
    assert_no_invalid(
        ~col("RateCodeID").between(1, 6),
        "RateCodeID invalide",
        df
    )

    # Store and forward flag
    assert_no_invalid(
        ~col("store_and_fwd_flag").isin("Y", "N"),
        "Store_and_fwd_flag invalide",
        df
    )

    # Payment type
    assert_no_invalid(
        ~col("payment_type").between(1, 6),
        "Payment_type invalide",
        df
    )

    # Passenger count
    assert_no_invalid(
        col("passenger_count") < 0,
        "Passenger_count négatif",
        df
    )

    # Montants financiers
    for c in [
        "fare_amount",
        "extra",
        "mta_tax",
        "improvement_surcharge",
        "tip_amount",
        "tolls_amount",
        "total_amount",
    ]:
        assert_no_invalid(
            col(c) < 0,
            f"Montant négatif détecté : {c}",
            df
        )

    return True


def validate_streamlit_input(df):
    """
    Valide les inputs selon toutes les règles de l'interface.
    Lève une AssertionError si une règle n'est pas respectée.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame Spark contenant les données NYC Taxi à valider.

    """

    # VendorID
    assert_no_invalid(
        ~col("VendorID").isin(1, 2),
        "VendorID invalide",
        df
    )

    # heure valide
    assert_no_invalid(
        (col("pickup_hour") < 0) | (col("pickup_hour") > 23),
        "pickup_hour invalide (doit être entre 0 et 23)",
        df
    )
    # Jour Valide
    assert_no_invalid(
        (col("pickup_dayofweek") < 1) | (col("pickup_dayofweek") > 7),
        "pickup_day invalide (doit être entre 1 et 7)",
        df
    )
    # Locations
    assert_no_invalid(
        ~col("PULocationID").between(1, 265) |
        ~col("DOLocationID").between(1, 265),
        "LocationID invalide",
        df
    )

    # Distance
    assert_no_invalid(
        col("trip_distance") <= 0,
        "Trip distance invalide",
        df
    )

    # Passenger count
    assert_no_invalid(
        col("passenger_count") < 0,
        "Passenger_count négatif",
        df
    )
    return True
