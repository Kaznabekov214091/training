import io
import dash
from dash import html, dcc, Input, Output, State
import pandas as pd
import gspread
from dash.exceptions import PreventUpdate
from oauth2client.service_account import ServiceAccountCredentials
import helper_func
import plotly.express as px
import numpy as np
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Image, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


scope = [ "https://spreadsheets.google.com/feeds",
          "https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive" ]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)


def get_data():
    sheet = client.open("task5_gs").worksheet("Data")
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    return df


df = get_data()
numeric_cols = df.columns[1:]
basic_stats = helper_func.compute_stats(df)


def make_mine_stats(stats_data):
    blocks = []
    for mine, row in stats_data.items():
        block = html.Div([
            html.H3(mine, style={"margin-bottom": "8px"}),
            html.P(f"Mean: {row['Mean']:.2f}"),
            html.P(f"Std Dev: {row['Std Dev']:.2f}"),
            html.P(f"Median: {row['Median']:.2f}"),
            html.P(f"IQR: {row['IQR']:.2f}"),
            html.P(f"Zscore Outliers: {row['Zscore_Outliers_Count']}"),
            html.P(f"IQR Outliers: {row['IQR_Outliers_Count']}"),
            html.P(f"MA Outliers: {row['MA_Outliers_Count']}"),
            html.P(f"Grubbs Outliers: {row['Grubbs_Outliers_Count']}")
        ], style={
            "display": "inline-block",
            "padding": "10px",
            "border": "1px solid #ddd",
            "border-radius": "6px",
            "margin": "5px",
            "background": "#fafafa"
        })
        blocks.append(block)
    return html.Div(blocks, style={"textAlign": "center", "margin": "20px 0"})


app = dash.Dash(__name__)


app.layout = html.Div([
    html.H1("Mines Stats & Anomalies"),
    html.Div([
        html.H4("IQR Multiplier"),
        dcc.Slider(id='iqr_mult', min=0.5, max=5, step=0.1, value=1.5,
                   marks={0.5: '0.5', 1.5: '1.5', 3: '3', 5: '5'}),
        html.H4("Z-score Threshold"),
        dcc.Input(id='z_thresh', type='number', value=3, step=0.1),
        html.H4("Moving Average Window"),
        dcc.Input(id='ma_window', type='number', value=5, step=1),
        html.H4("Moving Avg Percent Threshold"),
        dcc.Input(id='ma_percent', type='number', value=20, step=1),
        html.H4("Grubbs Alpha"),
        dcc.Input(id='grubbs_alpha', type='number', value=0.05, step=0.01),
    ], style={"padding": "20px", "border": "1px solid #ddd", "margin-bottom": "20px"}),

    html.Div(id='stats_container', children=make_mine_stats(basic_stats)),
    html.H4("Chart Type"),
    dcc.Dropdown(
        id='chart_type',
        options=[
            {'label': 'Line', 'value': 'line'},
            {'label': 'Violin', 'value': 'violin'},
            {'label': 'Histogram', 'value': 'histogram'}
        ],
        value='line'
    ),
    html.H4("Trendline Degree"),
    dcc.Dropdown(
        id='trendline_degree',
        options=[{'label': f'{i}', 'value': i} for i in range(1, 5)],
        value=1
    ),
    html.H4('Outlier Marker'),
    dcc.Dropdown(
        id='outlier_type',
        options=[
            {'label': 'None', 'value': 'none'},
            {'label': 'IQR', 'value': 'IQR'},
            {'label': 'Z-score', 'value': 'Zscore'},
            {'label': 'Moving Average', 'value': 'MA'},
            {'label': 'Grubbs', 'value': 'Grubbs'}
        ],
        value='none'
    ),
    dcc.Graph(id='main_chart'),
    dcc.Store(id='stats_store'),
    html.Button("Generate PDF Report", id="generate_pdf_btn",
                style={"backgroundColor": "#e67e22", "color": "white", "padding": "10px 15px",
                       "border": "none", "borderRadius": "5px", "cursor": "pointer"}),
    dcc.Download(id="download_pdf"),
    html.Button(
        "Refresh Data",
        id="refresh_btn",
        style={
            "backgroundColor": "#27ae60",
            "color": "white",
            "padding": "10px 15px",
            "border": "none",
            "borderRadius": "5px",
            "cursor": "pointer",
            "marginBottom": "20px"
        }
    ),dcc.Store(id="df_store")

])


@app.callback(
    Output('stats_container', 'children'),
    Output('stats_store', 'data'),
    Input("df_store", "data"),
    Input('iqr_mult', 'value'),
    Input("z_thresh", "value"),
    Input("ma_window", "value"),
    Input("ma_percent", "value"),
    Input("grubbs_alpha", "value"),
)
def update_stats(df_data, iqr_mult, z_thresh, ma_window, ma_percent, grubbs_alpha):
    if df_data is None: df_local = df
    else:
        df_local = pd.DataFrame(df_data)

    stats_data = helper_func.compute_stats(
        df_local, iqr_mult, z_thresh, ma_window, ma_percent, grubbs_alpha
    )

    return make_mine_stats(stats_data), stats_data


@app.callback(
    Output('main_chart', 'figure'),
    Input('stats_store', 'data'),
    Input('df_store', 'data'),
    Input('chart_type', 'value'),
    Input('trendline_degree', 'value'),
    Input('outlier_type', 'value')
)
def update_chart(stats_data, df_data, chart_type, trendline_degree, outlier_type):
    if df_data is None:
        df_plot = df.copy()
    else:
        df_plot = pd.DataFrame(df_data)
    numeric_cols = df_plot.columns[1:]
    if chart_type == 'line':
        fig = px.line(df_plot, x='Date', y=numeric_cols, title="Mine Outputs Over Time")
        for col in numeric_cols:
            z = np.polyfit(range(len(df_plot)), df_plot[col], trendline_degree)
            p = np.poly1d(z)
            fig.add_scatter(x=df_plot['Date'], y=p(range(len(df_plot))),
                            mode='lines', line=dict(dash='dash'), name=f"{col} Trendline")
    elif chart_type == 'histogram':
        fig = px.histogram(df_plot.melt(id_vars='Date', value_vars=numeric_cols),
                           x='value', color='variable', barmode='overlay',
                           title="Histogram of Mine Outputs")
    elif chart_type == 'violin':
        fig = px.violin(df_plot.melt(id_vars='Date', value_vars=numeric_cols),
                        x='variable', y='value', box=True, points='all',
                        title="Violin Plot of Mine Outputs")
    elif chart_type == 'bar':
        fig = px.bar(df_plot, x='Date', y=numeric_cols, title="Mine Outputs Over Time")
    elif chart_type == 'stacked':
        fig = px.bar(df_plot, x='Date', y=numeric_cols, barmode='stack', title="Mine Outputs Over Time")
    else:
        fig = px.line(df_plot, x='Date', y=numeric_cols, title="Mine Outputs Over Time")

    data = stats_data if stats_data is not None else basic_stats
    spike_legend_added = False
    drop_legend_added = False
    if outlier_type != 'none' and data is not None and chart_type == 'line':
        for col in numeric_cols:
            row = data.get(col, {})
            outlier_vals = row.get(f"{outlier_type}_Outliers", [])
            if len(outlier_vals) == 0:
                continue
            mask = pd.Series(False, index=df_plot.index)
            if len(outlier_vals) == len(df_plot):
                mask = pd.Series(outlier_vals, index=df_plot.index, dtype=bool)
            else:
                mask.iloc[outlier_vals] = True

            for i, is_outlier in enumerate(mask):
                if is_outlier:
                    y_val = df_plot[col][i]
                    if y_val > df_plot[col].median():
                        fig.add_scatter(x=[df_plot['Date'][i]], y=[y_val],
                                        mode='markers', marker=dict(color='green', size=10),
                                        name='Spike', showlegend=not spike_legend_added)
                        spike_legend_added = True
                    else:
                        fig.add_scatter(x=[df_plot['Date'][i]], y=[y_val],
                                        mode='markers', marker=dict(color='red', size=10),
                                        name='Drop', showlegend=not drop_legend_added)
                        drop_legend_added = True

    return fig




@app.callback(
Output('download_pdf', 'data'),
Input('generate_pdf_btn', 'n_clicks'),
State('stats_store', 'data'),
State('chart_type', 'value'),
State('trendline_degree', 'value'),
State('outlier_type', 'value')
)
def generate_pdf(n_clicks, stats_data, chart_type, trendline_degree, outlier_type):
    if not n_clicks:
        raise PreventUpdate
    if stats_data is None:
        stats_data = helper_func.compute_stats(df)
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    styleH = styles["Heading1"]
    numeric_cols = list(stats_data.keys())
    for mine in numeric_cols:
        mine_data = stats_data[mine]
        elements.append(Paragraph(f"Mine: {mine}", styleH))
        elements.append(Spacer(1, 12))
        table_data = [["Metric", "Value"]]
        for key in ["Mean","Std Dev","Median","IQR",
                    "IQR_Outliers_Count","Zscore_Outliers_Count",
                    "MA_Outliers_Count","Grubbs_Outliers_Count"]:
            val = mine_data.get(key, "N/A")
            if isinstance(val, (float,int)):
                val = round(val,2)
            table_data.append([key,str(val)])
        t = Table(table_data, hAlign='CENTER', colWidths=[150,100])
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#3498db')),
            ('TEXTCOLOR',(0,0),(-1,0),colors.white),
            ('ALIGN',(0,0),(-1,-1),'CENTER'),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,0),11),
            ('BOTTOMPADDING',(0,0),(-1,0),4),
            ('GRID',(0,0),(-1,-1),0.5,colors.grey)
        ]))
        elements.append(t)
        elements.append(Spacer(1,12))
        df_plot = df.copy()
        cols = [mine] if mine in df_plot.columns else df_plot.columns[1:]
        fig, ax = plt.subplots(figsize=(12,6))
        if chart_type == "line":
            for col in cols:
                ax.plot(df_plot['Date'], df_plot[col], label=col)
                x_numeric = np.arange(len(df_plot))
                z = np.polyfit(x_numeric, df_plot[col], trendline_degree)
                p = np.poly1d(z)
                ax.plot(df_plot['Date'], p(x_numeric), linestyle='--', label=f"{col} Trendline")
                if outlier_type != "none":
                    out_vals = mine_data.get(f"{outlier_type}_Outliers", [])
                    mask = pd.Series(False, index=df_plot.index)
                    if len(out_vals) == len(df_plot):
                        mask = pd.Series(out_vals, index=df_plot.index, dtype=bool)
                    else:
                        mask.iloc[out_vals] = True
                    first_spike, first_drop = True, True
                    for i, is_out in enumerate(mask):
                        if is_out:
                            y_val = df_plot[col].iloc[i]
                            color = 'green' if y_val > df_plot[col].median() else 'red'
                            label = None
                            if color == 'green' and first_spike:
                                label = 'Spike'
                                first_spike = False
                            elif color == 'red' and first_drop:
                                label = 'Drop'
                                first_drop = False
                            ax.scatter(df_plot['Date'].iloc[i], y_val, color=color, s=50, label=label)

            ax.set_xlabel("Date")
            ax.set_ylabel("Value")
            ax.set_title(f"{mine} Output")
            ax.legend()
            fig.autofmt_xdate()
            fig.tight_layout()
        elif chart_type == "histogram":
            for col in cols:
                ax.hist(df_plot[col], bins=15, alpha=0.5, label=col)
            ax.set_xlabel("Value")
            ax.set_ylabel("Count")
            ax.set_title(f"{mine} Histogram")
            ax.legend()
            fig.tight_layout()

        elif chart_type == "violin":
            data = [df_plot[col].values for col in cols]
            ax.violinplot(data, showmeans=True)
            ax.set_xticks(np.arange(1, len(cols)+1))
            ax.set_xticklabels(cols)
            ax.set_ylabel("Value")
            ax.set_title(f"{mine} Violin Plot")
            fig.tight_layout()

        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='PNG', bbox_inches='tight')
        plt.close(fig)
        img_buffer.seek(0)
        elements.append(Image(img_buffer, width=500, height=350))
        elements.append(PageBreak())

    doc.build(elements)
    buffer.seek(0)
    return dcc.send_bytes(buffer.getvalue(), "Mine_Report.pdf")



@app.callback(
    Output("df_store", "data"),
    Input("refresh_btn", "n_clicks")
)
def refresh_data(n):
    if not n:
        raise PreventUpdate

    df_new = get_data()
    return df_new.to_dict("records")

if __name__ == "__main__":
    app.run(debug=True)
