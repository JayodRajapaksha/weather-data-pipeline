import os
import requests
import psycopg2

# Configuration - pulling from the environment
# The names in quotes must match the names you set in GitHub Secrets later
DB_PASS = os.getenv("DB_PASS")
PROJECT_REF = "ufsfjmxremmjelgsbhcn"

# We construct these using the PROJECT_REF and other standard Supabase details
DB_USER = f"postgres.{PROJECT_REF}"
DB_HOST = "aws-1-ap-northeast-2.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

def run_weather_pipeline():
    try:
        # 1. EXTRACTION (Open-Meteo)
        print("📡 Fetching weather data...")
        weather_url = "https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current=temperature_2m,wind_speed_10m,weather_code"
        response = requests.get(weather_url).json()
        
        temp = response['current']['temperature_2m']
        wind = response['current']['wind_speed_10m']
        code = response['current']['weather_code']

        # 2. LOADING (Supabase)
        print("🚀 Connecting to Supabase...")
        
        # We pass the variables we gathered at the top of the script
        conn = psycopg2.connect(
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS, 
            host=DB_HOST, 
            port=DB_PORT
        )
        cur = conn.cursor()

        insert_query = """
            INSERT INTO weather_log (city, temperature_c, wind_speed_kmh, weather_code)
            VALUES (%s, %s, %s, %s);
        """
        cur.execute(insert_query, ("London", temp, wind, code))
        
        conn.commit()
        print(f"✅ Data Saved! Temp: {temp}°C | Wind: {wind}km/h")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")

if __name__ == "__main__":
    run_weather_pipeline()
