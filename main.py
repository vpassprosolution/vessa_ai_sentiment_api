from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import redis
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect Redis
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=0,
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)

# Connect PostgreSQL
def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT")
    )

# METALS Sentiment API
@app.get("/sentiment/metals")
def get_metal_sentiment(symbol: str):
    cache_key = f"sentiment_metals_{symbol}"
    cached = redis_client.get(cache_key)

    if cached:
        return {"symbol": symbol, "sentiment": cached, "cached": True}

    try:
        conn = connect_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT result, generated_at 
            FROM ai_sentiment_output 
            WHERE symbol = %s 
            ORDER BY generated_at DESC LIMIT 1
        """, (symbol,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            result, timestamp = row
            redis_client.setex(cache_key, 600, result)  
            return {"symbol": symbol, "sentiment": result, "last_updated": str(timestamp), "cached": False}
        else:
            raise HTTPException(status_code=404, detail="Sentiment not found.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sentiment/others")
def get_other_sentiment(symbol: str):
    cache_key = f"sentiment_others_{symbol}"
    cached = redis_client.get(cache_key)

    if cached:
        return {"symbol": symbol, "sentiment": cached, "cached": True}

    try:
        conn = connect_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT result, generated_at 
            FROM ai_sentiment_output 
            WHERE symbol = %s 
            ORDER BY generated_at DESC LIMIT 1
        """, (symbol,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            result, timestamp = row
            redis_client.setex(cache_key, 600, result)  
            return {"symbol": symbol, "sentiment": result, "last_updated": str(timestamp), "cached": False}
        else:
            raise HTTPException(status_code=404, detail="Sentiment not found.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
