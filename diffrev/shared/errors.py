from fastapi import HTTPException


def error_envelope(code: str, message: str) -> dict:
    return {"error": {"code": code, "message": message}}


def raise_error(status_code: int, code: str, message: str):
    raise HTTPException(status_code=status_code, detail=error_envelope(code, message))
