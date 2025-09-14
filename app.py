# app.py
from flask import Flask, render_template, request
import psycopg2
from config.database import DATABASE_CONFIG
import json

app = Flask(__name__)

# DB 연결 함수
def get_db_connection():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    return conn



@app.route('/search', methods=['GET'])
def search():
    keyword = request.args.get('keyword', '').strip()
    model = request.args.get('model', '').strip()
    region = request.args.get('region', '').strip()

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
    SELECT DISTINCT cs."csID", cs."csName", cs."address", cs."region", cs."ns", cs."sw"
    FROM "ChargingStation" cs
    LEFT JOIN "Station_ChargerModel" scm ON cs."csID" = scm."csID"
    LEFT JOIN "ChargerModel" cm ON scm."modelID" = cm."modelID"
    LEFT JOIN "Station_ChargingType" sct ON cs."csID" = sct."csID"
    LEFT JOIN "ChargingType" ct ON sct."typeID" = ct."typeID"
    WHERE (%s IS NULL OR cs."csName" ILIKE %s OR cs."address" ILIKE %s)
      AND (%s IS NULL OR ct."typeName" = %s)
      AND (%s IS NULL OR cs."region" = %s)
    """

    cur.execute(query, (
        keyword if keyword else None,
        f'%{keyword}%' if keyword else None,
        f'%{keyword}%' if keyword else None,
        model if model else None,
        model if model else None,
        region if region else None,
        region if region else None,
    ))

    rows = cur.fetchall()

    # 기존 결과용 데이터
    # results = [(row[1], row[2], row[3]) for row in rows]
    results = [(row[0], row[1], row[2], row[3]) for row in rows]

    # 지도용 데이터 → 검색된 충전소만 마커 찍기용
    station_data = [
        {"csName": row[1], "ns": row[4], "sw": row[5]} for row in rows if row[4] is not None and row[5] is not None
    ]

    cur.close()
    conn.close()

    # search.html 렌더링 시 → station_data 같이 넘기기
    return render_template('search.html', results=results, keyword=keyword, model=model, region=region, station_data=station_data)



# 3️⃣ 즐겨찾기 확인 기능
@app.route('/favorites/<int:userID>')
def favorites(userID):
    conn = get_db_connection()
    cur = conn.cursor()

    query = """
    SELECT cs."csName", cs."address"
    FROM "ChargingStation" cs
    JOIN "Favorite" f ON cs."csID" = f."csID"
    WHERE f."userID" = %s
    """

    cur.execute(query, (userID,))
    favorites = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('favorites.html', favorites=favorites)

# 4️⃣ 충전 방법 필터링 기능
@app.route('/filter')
def filter_charging():
    typeName = request.args.get('typeName', '').strip()

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        SELECT cs."csName", cs."address"
        FROM "Station_ChargingType" sct
        JOIN "ChargingStation" cs ON sct."csID" = cs."csID"
        JOIN "ChargingType" ct ON sct."typeID" = ct."typeID"
        WHERE (%s IS NULL OR TRIM(ct."typeName") = %s)
    """

    typeName_param = typeName if typeName else None
    cur.execute(query, (typeName_param, typeName_param))

    stations = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('filter.html', stations=stations, typeName=typeName)

# 5️⃣ 지역별 충전소 필터링 기능
@app.route('/region')
def region_search():
    region = request.args.get('region', '').strip()
    print(f"🔍 region param = [{region}]")  # 추가

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
    SELECT cs."csName", cs."address"
    FROM "ChargingStation" cs
    WHERE cs."region" ILIKE %s
       OR cs."address" ILIKE %s
    """

    cur.execute(query, (f'%{region}%', f'%{region}%'))

    stations = cur.fetchall()
    print(f"✅ stations found = {len(stations)} rows")  # 추가

    cur.close()
    conn.close()

    return render_template('region.html', stations=stations, region=region)

# 5️⃣ 상세 정보 기능
@app.route('/detail/<int:csID>')
# def detail(csID):
#     conn = get_db_connection()
#     cur = conn.cursor()

#     query = """
#         SELECT cs."csID", cs."csName", cs."region", cs."address",
#                d."year", d."agency", d."facility"
#         FROM "ChargingStation" cs
#         LEFT JOIN "Detail" d ON cs."csID" = d."csID"
#         WHERE cs."csID" = %s
#     """
#     cur.execute(query, (csID,))
#     result = cur.fetchone()

#     cur.close()
#     conn.close()

#     return render_template('detail.html', result=result)
def detail(csID):
    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        SELECT cs."csID", cs."csName", cs."region", cs."address",
               d."year", d."agency", d."facility"
        FROM "ChargingStation" cs
        LEFT JOIN "Detail" d ON cs."csID" = d."csID"
        WHERE cs."csID" = %s
    """
    cur.execute(query, (csID,))
    result = cur.fetchone()

    cur.close()
    conn.close()

    if result:
        return render_template('detail.html', result=result)
    else:
        return render_template('detail.html', result=None)
    
@app.route('/mypage/<int:userID>')
def mypage(userID):
    conn = get_db_connection()
    cur = conn.cursor()

    # 사용자 정보 가져오기
    cur.execute("""
        SELECT "userName", "email"
        FROM "users"
        WHERE "userID" = %s
    """, (userID,))
    user_info = cur.fetchone()

    # 즐겨찾기 충전소 가져오기
    cur.execute("""
        SELECT cs."csName", cs."address"
        FROM "ChargingStation" cs
        JOIN "Favorite" f ON cs."csID" = f."csID"
        WHERE f."userID" = %s
    """, (userID,))
    favorites = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('mypage.html', user_info=user_info, favorites=favorites)

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT "csName", "ns", "sw"
        FROM "ChargingStation"
        WHERE "ns" IS NOT NULL AND "sw" IS NOT NULL
    """)
    rows = cur.fetchall()

    station_data = [
        {"csName": row[0], "ns": row[1], "sw": row[2]} for row in rows
    ]

    print(f"📍 station_data rows = {len(station_data)}")

    cur.close()
    conn.close()

    return render_template('index.html', station_data=station_data, keyword='', model='', region='')


if __name__ == '__main__':
    app.run(debug=True)