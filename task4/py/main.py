import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd



# ---------- LOAD CLEANED DATA ----------
def load_dataset(path, file):
    orders = pd.read_csv(f"{path}orders_clean{file}.csv")
    books = pd.read_csv(f"{path}books_clean{file}.csv")
    users = pd.read_csv(f"{path}users_clean{file}.csv")

    # parse timestamp
    orders["timestamp"] = pd.to_datetime(orders["timestamp"], errors="coerce")

    # computed daily revenue
    daily = (
        orders.groupby(orders["timestamp"].dt.date)["paid_price"]
        .sum()
        .reset_index()
        .rename(columns={"timestamp": "date", "paid_price": "revenue"})
    )
    daily["date"] = pd.to_datetime(daily["date"]).dt.strftime("%Y-%m-%d")

    # top 5 revenue days
    top5 = daily.sort_values("revenue", ascending=False).head(5).sort_values('date')

    # unique users
    unique_users = users["cluster"].nunique()

    # unique sets of authors
    unique_author_sets = books["author_set_str"].nunique()

    # most popular authors
    author_grouped=books.groupby('author_set_str')['total_sold'].sum().reset_index()
    popular_authors=author_grouped.sort_values('total_sold',ascending=False).head(1)['author_set_str'].values[0]
    popular_author_sold=author_grouped.sort_values('total_sold',ascending=False).head(1)['total_sold'].values[0]

    # best buyer ids
    merged = orders.merge(users, left_on="user_id", right_on="id", how="left")
    user_spending = (
        merged.groupby("cluster")["paid_price"]
        .sum()
        .reset_index()
    )

    # Best cluster
    best_cluster = user_spending.sort_values("paid_price", ascending=False).head(1)
    best_cluster_id = int(best_cluster["cluster"].values[0])

    # all aliases (user ids belonging to that cluster)
    aliases = users[users["cluster"] == best_cluster_id]["id"].tolist()
    representative_name = (
        users[users["cluster"] == best_cluster_id]["name"]
        .iloc[0]
        .title()
    )
    total_spent = float(best_cluster["paid_price"].values[0])

    return {
        "orders": orders,
        "books": books,
        "users": users,
        "daily": daily,
        "top5": top5,
        "unique_users": unique_users,
        "unique_author_sets": unique_author_sets,
        "popular_authors": popular_authors,
        "popular_author_sold": popular_author_sold,
        "best_cluster": best_cluster_id,
        "aliases": aliases,
        "representative_name": representative_name,
        "total_spent": total_spent
    }


# Load all 3 datasets
DATA1 = load_dataset("../DATA1/",1)
DATA2 = load_dataset("../DATA2/",2)
DATA3 = load_dataset("../DATA3/",3)

app = dash.Dash(__name__)
app.title = "Bookstore Dashboard"


# ---------- LAYOUT ----------
def make_tab(dataset, label):
    return dcc.Tab(
        label=label,
        children=[
            html.Br(),

            # KPI Row
            html.Div([
                html.Div([
                    html.H3("Unique Users"),
                    html.H1(dataset["unique_users"]),
                ], style={"width": "20%", "display": "inline-block"}),

                html.Div([
                    html.H3("Unique Author Sets"),
                    html.H1(dataset["unique_author_sets"]),
                ], style={"width": "20%", "display": "inline-block"}),

                html.Div([
                    html.H2(dataset["popular_authors"], style={"margin-bottom": "5px"}),
                    html.H4(f"Sold: {dataset['popular_author_sold']}")
                ], style={"width": "40%", "display": "inline-block"}),

                html.Div([
                    html.H4(dataset["representative_name"], style={"margin-bottom": "8px"}),
                    html.P(f"Cluster: {dataset['best_cluster']}"),
                    html.P(f"Aliases: {dataset['aliases']}"),
                    html.P(f"Total Spent: ${dataset['total_spent']:.2f}")
                ], style={"width": "20%", "display": "inline-block"}),
            ], style={"textAlign": "center"}),

            html.Hr(),

            # Daily revenue chart
            html.H2("Daily Revenue"),
            dcc.Graph(
                figure=px.line(
                    dataset["daily"],
                    x="date",
                    y="revenue",
                    markers=True,
                    title="Daily Revenue",
                )
            ),

            # Top 5 days
            html.H2("Top 5 Days by Revenue"),
            dcc.Graph(
                figure=px.line(
                    dataset["top5"],
                    x="date",
                    y="revenue",
                    title="Top 5 Revenue Days (YYYY-MM-DD)",
                )
            ),

        ],
    )


app.layout = html.Div([
    html.H1("Bookstore Dashboard", style={"textAlign": "center"}),

    dcc.Tabs([
        make_tab(DATA1, "DATASET 1"),
        make_tab(DATA2, "DATASET 2"),
        make_tab(DATA3, "DATASET 3"),
    ])
])
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
