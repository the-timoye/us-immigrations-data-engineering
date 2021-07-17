class InsertQueries:
    us_cities = """
        SELECT DISTINCT 
            city_id,
            city,
            state_code
        FROM staging_cities
    """

    us_states = """
        SELECT DISTINCT
            state_code,
            state
        FROM staging_cities
    """

    us_geography = """
        SELECT DISTINCT
            city_id,
            male_population,
            female_population,
            total_population,
            num_of_veterans,
            no_of_immigrants,
            avg_household_size
        FROM staging_cities
    """

    visa_types = """
        SELECT DISTINCT
            visa_code,
            visa_type
        FROM staging_immigrations
    """

    travels_info = """
        SELECT
            arrival_date,
            departure_date,
            airline,
            immigrant_id,
            visa_type,
            mode
        FROM staging_immigrations
    """

    transport_modes = """
        SELECT DISTINCT
            mode,
            transport_mode
        FROM staging_immigrations
    """

    immigrants = """
        SELECT DISTINCT
            immigrant_id,
            age,
            birth_year,
            gender,
            resident_country,
            address
        FROM staging_immigrations
    """

    immigrations_facts = """
        SELECT DISTINCT
            immigrant_id,
            visa_code,
            mode,
            month, 
            year
        FROM staging_immigrations
    """