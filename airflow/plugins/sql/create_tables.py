class CreateTables:
    staging_immigrations = """
        DROP TABLE IF EXISTS public.staging_immigrations;
        CREATE TABLE public.staging_immigrations (
            year INTEGER,
            month INTEGER,
            resident_country_code INTEGER,
            arrival_date DATE,
            address VARCHAR,
            departure_date DATE,
            age INTEGER,
            visa_code INTEGER,
            birth_year INTEGER,
            gender VARCHAR,
            airline VARCHAR,
            mode VARCHAR,
            resident_country VARCHAR,
            visa_type VARCHAR,
            state_address VARCHAR,
            transport_mode VARCHAR,
            immigrant_id BIGINT NOT NULL
        );
    """
    staging_cities = """
        DROP TABLE IF EXISTS public.staging_cities;
        CREATE TABLE public.staging_cities (
            city VARCHAR, 
            state VARCHAR,
            median_age REAL,
            male_population BIGINT,
            female_population BIGINT,
            total_population BIGINT,
            num_of_veterans BIGINT,
            no_of_immigrants BIGINT,
            avg_household_size REAL,
            state_code VARCHAR,
            race VARCHAR,
            city_id BIGINT NOT NULL
        );
    """

    us_cities = """
        DROP TABLE IF EXISTS public.us_cities;
        CREATE TABLE public.us_cities (
            city_id BIGINT NOT NULL,
            city VARCHAR NOT NULL,
            state_code VARCHAR NOT NULL
        );
    """

    us_states = """
        DROP TABLE IF EXISTS public.us_states;
        CREATE TABLE public.us_states (
            state_code VARCHAR NOT NULL,
            state VARCHAR NOT NULL
        );
    """

    us_geography = """
        DROP TABLE IF EXISTS public.us_geography;
        CREATE TABLE public.us_geography (
            city_id BIGINT NOT NULL,
            male_population BIGINT,
            female_population BIGINT,
            total_population BIGINT,
            num_of_veterans BIGINT,
            no_of_immigrants BIGINT,
            avg_household_size REAL
        );
    """

    visa_types = """
        DROP TABLE IF EXISTS public.visa_types;
        CREATE TABLE public.visa_types (
            visa_code INTEGER,
            visa_type VARCHAR
        );
    """
    transport_modes = """
        DROP TABLE IF EXISTS public.transport_modes;
        CREATE TABLE public.transport_modes (
            mode VARCHAR,
            transport_mode VARCHAR
        );
    """

    travel_info = """
        DROP TABLE IF EXISTS public.travel_info;
        CREATE TABLE public.travel_info (
            arrival_date DATE,
            departure_date DATE,
            airline VARCHAR,
            immigrant_id BIGINT,
            visa_type VARCHAR,
            mode VARCHAR
        );
    """

    immigrants ="""
        DROP TABLE IF EXISTS public.immigrants;
        CREATE TABLE public.immigrants (
            immigrant_id BIGINT,
            age INTEGER,
            birth_year INTEGER,
            gender VARCHAR,
            resident_country VARCHAR,
            address VARCHAR
        );
    """

    immigrants_facts = """
        DROP TABLE IF EXISTS public.immigrants_facts;
        CREATE TABLE public.immigrants_facts (
            immigrant_id BIGINT,
            visa_code INTEGER,
            mode VARCHAR,
            month INTEGER, 
            year INTEGER
        );
    """

