from faker import Faker
from dotenv import load_dotenv
import os
from urllib.parse import urlparse
import psycopg2
import json

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
# Creating Lookup Tables
names = """create table names(
id serial primary key,
locale text,
first_name text,
last_name text,
gender text)
"""
titles = """create table titles(
id serial primary key,
title text,
locale text,
gender text)"""
address_suf = """create table suffixes(
id serial primary key,
locale text,
suffix text
)"""
address_region = """create table regions(
id serial primary key,
locale text,
data json
)"""
address_words = """create table words(
id serial primary key,
locale text,
word text)"""
physical_attributes = """create table physical_attributes(
id serial primary key,
height real,
weight real,
gender text,
locale text)"""
eye_color = """create table eye_colors(
id serial primary key,
locale text,
eye_color text,
probability real
)"""
email_domains = """create table email_domains(
id serial primary key,
locale text,
domain text,
probability real)"""
email_pattern = """create table email_patterns(
id serial primary key,
pattern text)"""
geo_location = """create table geo_location(
id serial primary key,
data json)"""
phone_number_patterns = """create table phone_number_patterns(
id serial primary key,
pattern text,
locale text)"""
tables = [names, titles, address_suf, address_region, address_words, physical_attributes,
          eye_color, email_domains, email_pattern, geo_location, phone_number_patterns]

try:
    for table in tables:
        cur.execute(table)
except Exception as e:
    print("Error:",e)
print("Tables created")
# insert data
sql_patterns = {
    "names": """
        INSERT INTO names (locale, first_name, last_name, gender)
        VALUES (%s, %s, %s, %s)
    """,
    "titles": """
        INSERT INTO titles (title, locale,gender)
        VALUES (%s, %s,%s)
    """,
    "suffixes": """
        INSERT INTO suffixes (locale, suffix)
        VALUES (%s, %s)
    """,
    "regions": """
        INSERT INTO regions (locale,data)
        VALUES (%s, %s)
    """,
    "words": """
        INSERT INTO words (locale, word)
        VALUES (%s, %s)
    """,
    "physical_attributes": """
        INSERT INTO physical_attributes (height, weight, gender, locale)
        VALUES (%s, %s, %s, %s)
    """,
    "eye_colors": """
        INSERT INTO eye_colors (locale, eye_color, probability)
        VALUES (%s, %s, %s)
    """,
    "email_domains": """
        INSERT INTO email_domains (locale, domain, probability)
        VALUES (%s, %s, %s)
    """,
    "email_patterns": """
        INSERT INTO email_patterns (pattern)
        VALUES (%s)
    """,
    "phone_number_patterns": """
        INSERT INTO phone_number_patterns (pattern,locale)
        VALUES (%s,%s)
    """,
    "geo_location": """
        INSERT INTO geo_location (data)
        VALUES (%s)
    """
}
locales = ["en_US", "de_DE"]

try:
    def generate_unique_names(generator, count):
        names = set()
        while len(names) < count:
            names.add(generator())
        return list(names)


    def generate_people(locale):
        fake = Faker(locale)
        male_first = generate_unique_names(fake.first_name_male, 200)
        female_first = generate_unique_names(fake.first_name_female, 200)
        surnames = generate_unique_names(fake.last_name, 400)
        male_surnames = surnames[:200]
        female_surnames = surnames[200:]
        male_people = list(zip(male_first, male_surnames))
        female_people = list(zip(female_first, female_surnames))

        return male_people, female_people


    for locale in locales:
        male_people, female_people = generate_people(locale)
        for first_name, last_name in male_people:
            cur.execute(sql_patterns['names'], (locale, first_name, last_name, 'male'))
        for first_name, last_name in female_people:
            cur.execute(sql_patterns['names'], (locale, first_name, last_name, 'female'))
    print("Names inserted!")
    # titles
    titles_by_locale_gender = {
        "en_US": {
            "male": ["Mr.", "Dr.", "Prof."],
            "female": ["Ms.", "Mrs.", "Dr.", "Prof."]
        },
        "de_DE": {
            "male": ["Herr", "Dr.", "Prof."],
            "female": ["Frau", "Dr.", "Prof."]
        }
    }
    for locale, genders in titles_by_locale_gender.items():
        for gender, titles in genders.items():
            for title in titles:
                cur.execute(sql_patterns['titles'], (title, locale, gender))
    print("Titles inserted")
    # address suffixes
    def street_suffix_us():
        fake = Faker('en_US')
        suffix = set()
        while len(suffix) < 15:
            suffix.add(fake.street_suffix())
        return list(suffix)


    us_suffix = street_suffix_us()
    for s in us_suffix:
        cur.execute(sql_patterns['suffixes'], ('en_US', s))
    cur.execute(sql_patterns['suffixes'], ('de_DE', 'Street'))
    print("Suffixes inserted")
    with open("germany_regions.json") as g:
        g_data=json.load(g)
    with open("us_states.json") as us:
        us_data=json.load(us)
    us_rows_region = [("en_US",json.dumps(item)) for item in us_data]
    de_rows_region = [('de_DE',json.dumps(item)) for item in g_data]
    cur.executemany(sql_patterns['regions'], us_rows_region)
    cur.executemany(sql_patterns['regions'], de_rows_region)
    print("Regions Inserted")
    def generate_address_words(locale, count=50):
        fake = Faker(locale)
        words_set = set()
        while len(words_set) < count:
            name = fake.street_name()
            for w in name.replace("-", " ").split():
                if len(words_set) < count:
                    words_set.add(w)
                else:
                    break
        return list(words_set)


    for locale in locales:
        words = generate_address_words(locale, count=50)
        for w in words:
            cur.execute(sql_patterns['words'], (locale, w))
    print("Words inserted")
    physical_summary = [
        (175, 88, 'male', 'en_US'),
        (162, 75, 'female', 'en_US'),
        (180, 85, 'male', 'de_DE'),
        (165, 70, 'female', 'de_DE')
    ]
    cur.executemany(sql_patterns['physical_attributes'], physical_summary)
    eye_colors = {
        "en_US": [
            ("Brown", 0.55),
            ("Blue", 0.27),
            ("Hazel", 0.10),
            ("Green", 0.08)
        ],
        "de_DE": [
            ("Blue", 0.50),
            ("Brown", 0.30),
            ("Green", 0.15),
            ("Hazel", 0.05)
        ]
    }
    for locale, colors in eye_colors.items():
        cur.executemany(sql_patterns['eye_colors'], [(locale, color, prob) for color, prob in colors])

    print("Physical Attr Inserted")
    email_domains_data = {
        "de_DE": [
            ("gmx.de", 27.34), ("web.de", 26.44), ("t-online.de", 11.63),
            ("outlook.de", 8.15), ("aol.de", 5.17), ("freenet.de", 4.37),
            ("gmail.com", 4.08), ("1und1.de", 2.68), ("yahoo.de", 1.19),
            ("icloud.com", 0.40)
        ],
        "en_US": [
            ("gmail.com", 35), ("outlook.com", 30), ("yahoo.com", 25),
            ("icloud.com", 20), ("protonmail.com", 15), ("zoho.com", 10),
            ("aol.com", 8), ("fastmail.com", 5), ("neo.com", 3), ("mail.com", 1)
        ]
    }

    for locale, domains in email_domains_data.items():
        cur.executemany(sql_patterns['email_domains'], [(locale, domain, prob) for domain, prob in domains])

    email_patterns = [
        "{first}.{last}@{domain}",
        "{f}{last}@{domain}",
        "{first}{l}@{domain}",
        "{last}{first}@{domain}",
        "{first}{random}@{domain}",
        "{first}_{last}@{domain}"
    ]
    cur.executemany(sql_patterns['email_patterns'], [(p,) for p in email_patterns])
    phone_patterns = [
        ("{intl} ({area}) {subscriber}", None),
        ("({area}) {subscriber}", None),
        ("{area}-{subscriber}", None),
        ("{area} {subscriber}", None),
        ("{intl}-{area}-{subscriber}", None),
        ("0{area} {subscriber}", "de_DE")
    ]
    cur.executemany(sql_patterns['phone_number_patterns'], phone_patterns)
    print("Emails Inserted")
    with open("geo_locations.json") as f:
        geo_data = json.load(f)
    rows = [(json.dumps(item),) for item in geo_data]
    cur.executemany(sql_patterns['geo_location'], rows)
except Exception as e:
    conn.rollback()
finally:
    conn.commit()
    cur.close()
    conn.close()
