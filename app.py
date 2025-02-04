import streamlit as st
import pymysql
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import time

# [설정]
DB_HOST = "localhost"
DB_USER = "alswnddldy"
DB_PASSWORD = "1234"
DB_NAME = "alswnddldy"
TABLE_NAME = "upbit_data"

# 데이터베이스에서 데이터 가져오기
def fetch_data_from_db():
    connection = pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )
    query = f"""
    SELECT date, code, opening_price, closing_price, prev_closing_price, volume 
    FROM {TABLE_NAME}
    WHERE date >= '2024-12-10'
    ORDER BY date ASC;
    """
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = ["date", "code", "opening_price", "closing_price", "prev_closing_price", "volume"]
    connection.close()
    return pd.DataFrame(rows, columns=columns)

# Streamlit 앱 시작
st.set_page_config(layout="wide")  # 넓은 레이아웃 설정
st.title("실시간 코인 비교 시각화 (모든 데이터)")

# 자동 새로고침 기능
refresh_interval = 3600  # 1시간 간격 (초 단위)
if "last_update" not in st.session_state:
    st.session_state.last_update = datetime.now()

# 데이터 가져오기
data = fetch_data_from_db()

if data.empty:
    st.warning("데이터가 없습니다. 데이터를 수집 중인지 확인해주세요.")
else:
    # 데이터 처리
    data["date"] = pd.to_datetime(data["date"])
    data["change_rate"] = ((data["closing_price"] - data["opening_price"]) / data["opening_price"]) * 100
    data["price_change"] = data["closing_price"] - data["prev_closing_price"]
    data["cumulative_volume"] = data.groupby("code")["volume"].cumsum()  # 누적 거래량 계산

    coin_list = ["전체 보기"] + list(data["code"].unique())

    # 선택 가능한 코인 필터링 추가
    selected_coin = st.selectbox("시각화할 코인을 선택하세요:", options=coin_list)

    # 1. 거래량 변화 시계열 그래프
    st.subheader(f"{selected_coin} 거래량 변화 시계열" if selected_coin != "전체 보기" else "모든 코인의 거래량 변화 시계열")
    fig_volume = go.Figure()

    if selected_coin == "전체 보기":
        for coin in data["code"].unique():
            coin_data = data[data["code"] == coin]
            fig_volume.add_trace(go.Scatter(
                x=coin_data["date"],
                y=coin_data["volume"],
                mode='lines',
                name=coin
            ))
    else:
        coin_data = data[data["code"] == selected_coin]
        fig_volume.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["volume"],
            mode='lines',
            name=selected_coin
        ))

    fig_volume.update_layout(
        title="거래량 변화 시계열",
        xaxis_title="시간",
        yaxis_title="거래량",
        height=500,
    )
    st.plotly_chart(fig_volume, use_container_width=True)

    # 2. 누적 거래량 시각화
    st.subheader(f"{selected_coin} 누적 거래량 시각화" if selected_coin != "전체 보기" else "모든 코인의 누적 거래량 시각화")
    fig_cumulative = go.Figure()

    if selected_coin == "전체 보기":
        for coin in data["code"].unique():
            coin_data = data[data["code"] == coin]
            fig_cumulative.add_trace(go.Scatter(
                x=coin_data["date"],
                y=coin_data["cumulative_volume"],
                mode='lines',
                name=coin
            ))
    else:
        coin_data = data[data["code"] == selected_coin]
        fig_cumulative.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["cumulative_volume"],
            mode='lines',
            name=selected_coin
        ))

    fig_cumulative.update_layout(
        title="누적 거래량 시각화",
        xaxis_title="시간",
        yaxis_title="누적 거래량",
        height=500,
    )
    st.plotly_chart(fig_cumulative, use_container_width=True)

    # 3. 일일 변동률 분석
    st.subheader(f"{selected_coin} 일일 가격 변동률 분석" if selected_coin != "전체 보기" else "모든 코인의 일일 가격 변동률 분석")
    fig_change_rate = go.Figure()

    if selected_coin == "전체 보기":
        for coin in data["code"].unique():
            coin_data = data[data["code"] == coin]
            fig_change_rate.add_trace(go.Scatter(
                x=coin_data["date"],
                y=coin_data["change_rate"],
                mode='lines',
                name=f"{coin} 변동률"
            ))
    else:
        coin_data = data[data["code"] == selected_coin]
        fig_change_rate.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["change_rate"],
            mode='lines',
            name=f"{selected_coin} 변동률"
        ))

    fig_change_rate.update_layout(
        title="일일 가격 변동률",
        xaxis_title="날짜",
        yaxis_title="변동률 (%)",
        height=500,
    )
    st.plotly_chart(fig_change_rate, use_container_width=True)

    # 4. 코인별 상관관계 히트맵
    st.subheader("코인별 가격 상관관계 히트맵")

    # 데이터 피벗: 날짜별로 코인 가격을 열로 만듭니다.
    pivot_data = data.pivot(index="date", columns="code", values="closing_price")

    # 상관관계 계산
    correlation_matrix = pivot_data.corr()

    # 히트맵 시각화 (상관계수 수치 추가)
    fig_correlation = go.Figure(data=go.Heatmap(
        z=correlation_matrix.values,
        x=correlation_matrix.columns,
        y=correlation_matrix.columns,
        text=correlation_matrix.round(2).values,  # 상관계수 값을 텍스트로 표시
        texttemplate="%{text}",  # 텍스트 포맷 설정
        colorscale="Viridis",
        colorbar_title="상관계수"
    ))

    fig_correlation.update_layout(
        title="코인별 가격 상관관계",
        xaxis_title="코인",
        yaxis_title="코인",
        height=600,
    )

    st.plotly_chart(fig_correlation, use_container_width=True)

    # 5. 이동평균선 시각화
    if selected_coin != "전체 보기":
        st.subheader(f"{selected_coin} 이동평균선 시각화 (5일 vs 0일)")
        coin_data = data[data["code"] == selected_coin].sort_values("date")
        coin_data["short_ma"] = coin_data["closing_price"].rolling(window=5).mean()
        coin_data["long_ma"] = coin_data["closing_price"].rolling(window=10).mean()

        fig_ma = go.Figure()

        # 원래 종가
        fig_ma.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["closing_price"],
            mode="lines",
            name="종가",
            line=dict(color="blue")
        ))

        # 단기 이동평균선
        fig_ma.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["short_ma"],
            mode="lines",
            name="단기 이동평균선 (5일)",
            line=dict(color="green", dash="dot")
        ))

        # 장기 이동평균선
        fig_ma.add_trace(go.Scatter(
            x=coin_data["date"],
            y=coin_data["long_ma"],
            mode="lines",
            name="장기 이동평균선 (20일)",
            line=dict(color="red", dash="dash")
        ))

        fig_ma.update_layout(
            title="이동평균선",
            xaxis_title="날짜",
            yaxis_title="가격",
            height=500,
        )

        st.plotly_chart(fig_ma, use_container_width=True)

    # 데이터 요약
    if selected_coin == "전체 보기":
        st.subheader("전체 데이터 요약")
        st.dataframe(data)
    else:
        st.subheader(f"{selected_coin} 데이터 요약")
        recent_data = data[data["code"] == selected_coin]
        st.dataframe(recent_data)

    # 새로고침 시간 확인 및 자동 새로고침
    time_diff = (datetime.now() - st.session_state.last_update).total_seconds()
    if time_diff > refresh_interval:
        st.session_state.last_update = datetime.now()
        st.experimental_rerun()
