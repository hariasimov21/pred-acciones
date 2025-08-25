from fastapi import FastAPI

app = FastAPI(title="pred-acciones-api")

@app.get("/health")
def health():
    return {"status": "ok"}
