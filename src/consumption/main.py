import json
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
import logging

app = FastAPI()

@app.post("/consume")
async def consume(request: Request):
    try:
        request_body = await request.json()
        message_data = json.loads(request_body['message']['data'])

        source = message_data.get("source")

        if source in ["ohlc_clean", "fear_greed_clean"]:
            logging.info(f"Triggered consumption from source: {source}")

        #![TODO]
        # Logic:
        #  use sql to combine the tables
        #![TODO]


        else:
            logging.warning("Message source not relevant for consumption.")
            return {"status": "ignored"}

    except Exception as e:
        logging.error(f"Error during consumption process: {e}")
        raise HTTPException(status_code=500, detail="Data consumption failed")


if __name__ == "__main__":
    consume()