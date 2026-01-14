from fastapi import FastAPI

app = FastAPI(title="Fleet Dynamic Pring System")

@app.get("/health")
def health():
    return {"status": "ok"}
