import pandas as pd
import psycopg2
#from psycopg2.extras import execute_values
from config.database import DATABASE_CONFIG
#import os

def create_tables(cur):
    """테이블 생성 + 데이터 insert"""

    # 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "ChargingStation" (
            "csID" integer PRIMARY KEY,
            "csName" varchar(100),
            "region" varchar(50),
            "address" text,
            "ns" double precision,
            "sw" double precision
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Detail" (
            "csID" integer UNIQUE,
            "year" integer,
            "agency" varchar(100),
            "facility" varchar(100)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "ChargerModel" (
            "modelID" integer PRIMARY KEY,
            "modelName" varchar(50)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Station_ChargerModel" (
            "csID" integer,
            "modelID" integer,
            PRIMARY KEY ("csID", "modelID")
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "ChargingType" (
            "typeID" integer PRIMARY KEY,
            "typeName" varchar(50)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Station_ChargingType" (
            "csID" integer,
            "typeID" integer,
            PRIMARY KEY ("csID", "typeID")
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "users" (
            "userID" integer PRIMARY KEY,
            "userName" varchar(50),
            "email" varchar(100),
            "password" varchar(100)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Favorite" (
            "favoriteID" integer PRIMARY KEY,
            "userID" integer,
            "csID" integer,
            "favoriteDate" timestamp
        );
    """)

    # ALTER TABLE (Foreign Key 추가)
    cur.execute("""ALTER TABLE IF EXISTS "Favorite" ADD CONSTRAINT fk_favorite_user FOREIGN KEY ("userID") REFERENCES "users" ("userID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Favorite" ADD CONSTRAINT fk_favorite_station FOREIGN KEY ("csID") REFERENCES "ChargingStation" ("csID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Station_ChargerModel" ADD CONSTRAINT fk_scm_station FOREIGN KEY ("csID") REFERENCES "ChargingStation" ("csID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Station_ChargerModel" ADD CONSTRAINT fk_scm_model FOREIGN KEY ("modelID") REFERENCES "ChargerModel" ("modelID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Station_ChargingType" ADD CONSTRAINT fk_sct_station FOREIGN KEY ("csID") REFERENCES "ChargingStation" ("csID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Station_ChargingType" ADD CONSTRAINT fk_sct_type FOREIGN KEY ("typeID") REFERENCES "ChargingType" ("typeID") ON DELETE CASCADE;""")
    cur.execute("""ALTER TABLE IF EXISTS "Detail" ADD CONSTRAINT fk_detail_station FOREIGN KEY ("csID") REFERENCES "ChargingStation" ("csID") ON DELETE CASCADE;""")

    print("✅ 테이블 생성 완료!")

    # # 데이터 insert
    # df_station = pd.read_csv("ChargingStation.csv")
    # for row in df_station.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "ChargingStation"("csID", "csName", "region", "address", "ns", "sw")
    #         VALUES (%s, %s, %s, %s, %s, %s)
    #         ON CONFLICT ("csID") DO NOTHING
    #     """, row)

    # df_detail = pd.read_csv("Detail.csv")
    # for row in df_detail.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "Detail"("csID", "year", "agency", "facility")
    #         VALUES (%s, %s, %s, %s)
    #         ON CONFLICT ("csID") DO NOTHING
    #     """, row)

    # df_charger_model = pd.read_csv("ChargerModel.csv")
    # for row in df_charger_model.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "ChargerModel"("modelID", "modelName")
    #         VALUES (%s, %s)
    #         ON CONFLICT ("modelID") DO NOTHING
    #     """, row)

    # df_station_charger_model = pd.read_csv("Station_ChargerModel.csv")
    # for row in df_station_charger_model.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "Station_ChargerModel"("csID", "modelID")
    #         VALUES (%s, %s)
    #         ON CONFLICT DO NOTHING
    #     """, row)

    # df_charging_type = pd.read_csv("ChargingType.csv")
    # for row in df_charging_type.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "ChargingType"("typeID", "typeName")
    #         VALUES (%s, %s)
    #         ON CONFLICT ("typeID") DO NOTHING
    #     """, row)

    # df_station_charging_type = pd.read_csv("Station_ChargingType.csv")
    # for row in df_station_charging_type.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "Station_ChargingType"("csID", "typeID")
    #         VALUES (%s, %s)
    #         ON CONFLICT DO NOTHING
    #     """, row)

    # df_users = pd.read_csv("users.csv")
    # for row in df_users.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "users"("userID", "userName", "email", "password")
    #         VALUES (%s, %s, %s, %s)
    #         ON CONFLICT ("userID") DO NOTHING
    #     """, row)

    # df_favorite = pd.read_csv("Favorite.csv")
    # for row in df_favorite.itertuples(index=False):
    #     cur.execute("""
    #         INSERT INTO "Favorite"("favoriteID", "userID", "csID", "favoriteDate")
    #         VALUES (%s, %s, %s, %s)
    #         ON CONFLICT ("favoriteID") DO NOTHING
    #     """, row)

    # print("✅ 데이터 insert 완료!")

def main():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur = conn.cursor()
        create_tables(cur)

        
        # CSV 읽기
        df = pd.read_csv("EcarDBfile.csv")
        # ns, sw 컬럼 → 문자열 변환 후 쉼표 제거 → float 변환
        # df["ns"] = df["ns"].astype(str).str.replace(",", "").astype(float)
        # df["sw"] = df["sw"].astype(str).str.replace(",", "").astype(float)
        df["ns"] = pd.to_numeric(df["ns"].astype(str).str.replace(",", ""), errors='coerce')
        df["sw"] = pd.to_numeric(df["sw"].astype(str).str.replace(",", ""), errors='coerce')



        # csID 자동 생성
        df["csID"] = range(1, len(df) + 1)

        # modelID 자동 생성
        unique_models = df["충전기타입"].unique()
        model_id_map = {name: idx + 1 for idx, name in enumerate(unique_models)}
        df["modelID"] = df["충전기타입"].map(model_id_map)

        # typeID 자동 생성
        unique_types = df["typeName"].unique()
        type_id_map = {name: idx + 1 for idx, name in enumerate(unique_types)}
        df["typeID"] = df["typeName"].map(type_id_map)

        # ChargingStation insert
        for row in df.itertuples(index=False):
            cur.execute("""
                INSERT INTO "ChargingStation"("csID", "csName", "region", "address", "ns", "sw")
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ("csID") DO NOTHING
            """, (row.csID, row.csName, row.legion, row.address, row.ns, row.sw))

        # Detail insert
        for row in df.itertuples(index=False):
            cur.execute("""
                INSERT INTO "Detail"("csID", "year", "agency", "facility")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ("csID") DO NOTHING
            """, (row.csID, row.year, row.agency, row.facility))

        # ChargingType insert
        for typeName, typeID in type_id_map.items():
            cur.execute("""
                INSERT INTO "ChargingType"("typeID", "typeName")
                VALUES (%s, %s)
                ON CONFLICT ("typeID") DO NOTHING
            """, (typeID, typeName))

        # Station_ChargingType insert
        for row in df.itertuples(index=False):
            cur.execute("""
                INSERT INTO "Station_ChargingType"("csID", "typeID")
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row.csID, row.typeID))

        # ChargerModel insert
        for modelName, modelID in model_id_map.items():
            cur.execute("""
                INSERT INTO "ChargerModel"("modelID", "modelName")
                VALUES (%s, %s)
                ON CONFLICT ("modelID") DO NOTHING
            """, (modelID, modelName))

        # Station_ChargerModel insert
        for row in df.itertuples(index=False):
            cur.execute("""
                INSERT INTO "Station_ChargerModel"("csID", "modelID")
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (row.csID, row.modelID))

        # users insert
        df_users = pd.read_csv("users.csv")
        for row in df_users.itertuples(index=False):
            cur.execute("""
                INSERT INTO "users"("userID", "userName", "email", "password")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ("userID") DO NOTHING
            """, (row.userID, row.userName, row.email, row.password))

        # Favorite insert
        df_favorite = pd.read_csv("Favorite.csv")
        for row in df_favorite.itertuples(index=False):
            cur.execute("""
                INSERT INTO "Favorite"("favoriteID", "userID", "csID", "favoriteDate")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ("favoriteID") DO NOTHING
            """, (row.favoriteID, row.userID, row.csID, row.favoriteDate))

        # Commit
        conn.commit()
        print("✅ Commit 성공!")

    except Exception as e:
        print("❌ Error:", e)
        conn.rollback()

    finally:
        cur.close()
        conn.close()
        print("🔌 Connection closed.")

if __name__ == "__main__":
    main()