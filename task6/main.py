import folium
from folium import Map, Marker
from flask import Flask, render_template, request
import psycopg2
from urllib.parse import urlparse
import os
from dotenv import load_dotenv


load_dotenv()
app = Flask(__name__)

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

BATCH_SIZE = 10

def generate_fake_users(locale, seed, batch_number):
    users = []
    start_index = (batch_number - 1) * BATCH_SIZE
    for i in range(BATCH_SIZE):
        p_index = start_index + i
        cur.execute(
            "SELECT generate_fake_user(%s, %s, %s, %s);",
            (locale, seed, batch_number, p_index)
        )
        users.append(cur.fetchone()[0])
    return users


def create_map(users):
    if not users:
        m = folium.Map(location=[0, 0], zoom_start=2)
    else:
        avg_lat = sum(u['lat'] for u in users) / len(users)
        avg_lon = sum(u['lon'] for u in users) / len(users)
        m = folium.Map(location=[avg_lat, avg_lon], zoom_start=4)

    # Add markers
    for u in users:
        Marker(
            location=[u['lat'], u['lon']],
            popup=f"{u['full_name']}<br>{u['address']}"
        ).add_to(m)

    return m._repr_html_()

@app.route("/", methods=["GET", "POST"])
def index():
    locale = "en_US"
    seed = 1
    page = 1
    if request.method == "POST":
        locale = request.form.get("Locale")
        seed = int(request.form.get("seed"))
        page = int(request.form.get("page", 1))
    users = generate_fake_users(locale, seed, page)
    map_html = create_map(users)
    return render_template("index.html", users=users, locale=locale, seed=seed, page=page,map_html=map_html)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
