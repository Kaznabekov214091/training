import time
import psycopg2
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

NUM_USERS = 500
BATCH_SIZE = 10
locale = 'en_US'
seed = 1
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
start_time = time.time()
users = []
for batch_number in range(1, NUM_USERS // BATCH_SIZE + 1):
    start_index = (batch_number - 1) * BATCH_SIZE
    end_index = start_index + BATCH_SIZE - 1
    cur.execute(
        """
        SELECT generate_fake_user(%s, %s, %s, gs)
        FROM generate_series(%s, %s) AS gs;
        """,
        (locale, seed, batch_number, start_index, end_index)
    )
    users.extend([row[0] for row in cur.fetchall()])

end_time = time.time()
elapsed = end_time - start_time
print(f"Generated {NUM_USERS} users in {elapsed:.2f} seconds ({NUM_USERS/elapsed:.2f} users/sec)")