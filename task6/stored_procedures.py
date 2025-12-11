
from dotenv import load_dotenv
import os
from urllib.parse import urlparse
import psycopg2


load_dotenv()
db_url = os.environ.get('DATABASE_URL')
result = urlparse(db_url)
conn = psycopg2.connect(
    host=result.hostname,
    port=result.port,
    database=result.path[1:],
    user=result.username,
    password=result.password
)
cur = conn.cursor()

get_gender = """
CREATE FUNCTION get_gender(p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN CASE WHEN (p_seed + p_batch + p_index) % 2 = 0 THEN 'male' ELSE 'female' END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""

name = """
CREATE FUNCTION get_name(p_locale text, p_gender text, p_seed int, p_batch int, p_index int)
RETURNS TABLE(fname text, lname text) AS $$
BEGIN
    RETURN QUERY
    SELECT n.first_name, n.last_name
    FROM names n
    WHERE n.locale = p_locale AND n.gender = p_gender
    ORDER BY md5(n.first_name || n.last_name || p_seed || p_batch || p_index)
    LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;
"""

title = """
CREATE FUNCTION pick_title(p_locale text, p_gender text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT title
        FROM titles
        WHERE locale = p_locale AND gender = p_gender
        ORDER BY md5(title || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

region = """
CREATE FUNCTION pick_region(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS json AS $$
BEGIN
    RETURN (
        SELECT data
        FROM regions
        WHERE locale = p_locale
        ORDER BY md5(data::text || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

location = """
CREATE FUNCTION get_location(p_region text, p_seed int, p_batch int, p_index int)
RETURNS json AS $$
BEGIN
    RETURN (
        SELECT data
        FROM geo_location
        WHERE data->>'region' = p_region
        ORDER BY md5(data::text || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

det_random = """
CREATE FUNCTION det_rand(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS double precision AS $$
DECLARE
    hash_int int;
BEGIN
    hash_int := ('x' || substr(md5(p_locale || p_seed || p_batch || p_index), 1, 8))::bit(32)::int;
    RETURN hash_int::double precision / 4294967295.0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""

eye = """
CREATE FUNCTION get_eye_color(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT eye_color
        FROM eye_colors
        WHERE locale = p_locale
        ORDER BY md5(eye_color || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

email_domain = """
CREATE FUNCTION get_email_domain(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT domain
        FROM email_domains
        WHERE locale = p_locale
        ORDER BY md5(domain || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;"""

email_pattern = """
CREATE FUNCTION get_email_pattern(p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT pattern
        FROM email_patterns
        ORDER BY md5(pattern || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

phone_pattern = """
CREATE FUNCTION get_phone_pattern(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT pattern
        FROM phone_number_patterns
        WHERE locale = p_locale OR locale IS NULL
        ORDER BY md5(pattern || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

address_suffix = """
CREATE FUNCTION address_suf(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT suffix
        FROM suffixes
        WHERE locale = p_locale
        ORDER BY md5(suffix || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""
word="""
CREATE FUNCTION get_word(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS text AS $$
BEGIN
    RETURN (
        SELECT word
        FROM words
        WHERE locale = p_locale
        ORDER BY md5(word || p_seed || p_batch || p_index)
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""



# Generator
generator = """
CREATE FUNCTION generate_fake_user(p_locale text, p_seed int, p_batch int, p_index int)
RETURNS json AS $$
DECLARE
    user_gender text;
    first_name text;
    last_name text;
    title text;
    r double precision;
    u1 double precision;
    u2 double precision;
    full_name text;
    region_data json;
    location_data json;
    eye_color text;
    height_mean real;
    weight_mean real;
    height numeric;
    weight numeric;
    email_domain text;
    email_pattern text;
    phone_pattern text;
    suffix text;
    word text;
    address text;
    email text;
    phone text;
    lat numeric;
    lon numeric;
    region_name text;
BEGIN
    -- deterministic random numbers
    r := det_rand(p_locale, p_seed, p_batch, p_index);
    u1 := det_rand(p_locale, p_seed, p_batch, p_index);
    u1 := LEAST(GREATEST(u1, 1e-10), 1-1e-10);
    IF u1 = 0 THEN u1 := 1e-10; END IF;
    u2 := (u1 + 0.5) - floor(u1 + 0.5);

    -- get gender and names
    user_gender := get_gender(p_seed, p_batch, p_index);
    SELECT n.fname, n.lname INTO first_name, last_name
    FROM get_name(p_locale, user_gender, p_seed, p_batch, p_index) AS n;

    -- other attributes
    title := pick_title(p_locale, user_gender, p_seed, p_batch, p_index);
    region_data := pick_region(p_locale, p_seed, p_batch, p_index);
    region_name:=  CASE 
        WHEN p_locale = 'de_DE' THEN region_data->>'city'
        ELSE region_data->>'state'  
    END;
    location_data := get_location(
    CASE 
        WHEN p_locale = 'de_DE' THEN region_data->>'city'
        ELSE region_data->>'state'
    END,
    p_seed, p_batch, p_index
);
    eye_color := get_eye_color(p_locale, p_seed, p_batch, p_index);

    SELECT pa.height, pa.weight INTO height_mean, weight_mean
    FROM physical_attributes AS pa
    WHERE pa.gender = user_gender AND pa.locale = p_locale;

    email_domain := get_email_domain(p_locale, p_seed, p_batch, p_index);
    email_pattern := get_email_pattern(p_seed, p_batch, p_index);
    phone_pattern := get_phone_pattern(p_locale, p_seed, p_batch, p_index);
    suffix := address_suf(p_locale, p_seed, p_batch, p_index);
    word := get_word(p_locale, p_seed, p_batch, p_index);

    -- full name
    IF r > 0.6 THEN
        full_name := title || ' ' || first_name || ' ' || last_name;
    ELSE
        full_name := first_name || ' ' || last_name;
    END IF;

    -- physical attributes
    height := round(greatest(height_mean + 5 * sqrt(-2 * ln(u1)) * cos(2 * pi() * u2),160));
    weight := round(greatest(weight_mean + CASE WHEN p_locale = 'en_US' THEN 13 ELSE 9 END * sqrt(-2 * ln(u1)) * cos(2 * pi() * u2),50));

    -- email & phone
    email := replace(replace(replace(replace(replace(email_pattern,
        '{first}', first_name),
        '{last}', last_name),
        '{f}', left(first_name,1)),
        '{l}', left(last_name,1)),
        '{domain}', email_domain);
    email := replace(email, '{random}', lpad(floor(r * 1000)::text, 3, '0'));

    phone := replace(replace(replace(phone_pattern,
        '{intl}', CASE WHEN p_locale = 'en_US' THEN '+1' ELSE '+49' END),
        '{area}', region_data->>'area_code'),
        '{subscriber}', lpad(floor(u2 * 1000000)::text, 6, '0'));

    -- lat/lon
    lon := round(
        ((location_data->>'lon_min')::numeric + r * ((location_data->>'lon_max')::numeric - (location_data->>'lon_min')::numeric))::numeric,
        6
    );
    lat := round(
        ((location_data->>'lat_min')::numeric + r * ((location_data->>'lat_max')::numeric - (location_data->>'lat_min')::numeric))::numeric,
        6
    );

    -- address
    address :=  CASE 
    WHEN p_locale = 'en_US' THEN
        word || ' ' || suffix || ', ' || (region_data->>'zip_prefix') || '-' || lpad(floor(u1 * 1000)::text, 3, '0')
    ELSE
        word || ' ' || suffix || ', ' || (region_data->>'zip_prefix') || '-' || lpad(floor(u1 * 10000)::text, 4, '0')
END;

    RETURN json_build_object(
        'full_name', full_name,
        'gender', user_gender,
        'eye_color', eye_color,
        'height', height,
        'weight', weight,
        'email', email,
        'phone', phone,
        'address', address,
        'lat', lat,
        'lon', lon,
        'region',region_name
    );
END;
$$ LANGUAGE plpgsql STABLE;
"""

conn.rollback()  # reset any aborted transaction

drop_functions = [
    "generate_fake_user(text, int, int, int)",
    "get_name(text, text, int, int, int)",
    "get_eye_color(text, int, int, int)",
    "get_email_domain(text, int, int, int)",
    "get_gender(int, int, int)",
    "pick_title(text, text, int, int, int)",
    "pick_region(text, int, int, int)",
    "get_location(text, int, int, int)",
    "det_rand(text, int, int, int)",
    "get_email_pattern(int, int, int)",
    "get_phone_pattern(text, int, int, int)",
    "address_suf(text, int, int, int)",
    "get_word(text, int, int, int)"
]

cur.execute("BEGIN;")
for f in drop_functions:
    cur.execute(f"DROP FUNCTION IF EXISTS {f};")
cur.execute("COMMIT;")



sql_functions = [get_gender, name, title, region, location, det_random,word,
                 eye, email_domain, email_pattern, phone_pattern, address_suffix, generator
                 ]

try:
    cur.execute("Begin;")
    for sql in sql_functions:
        cur.execute(sql)
    cur.execute("Commit;")
    print("Functions created")
except Exception as e:
    cur.execute('Rollback;')
    print("Failed",e)
