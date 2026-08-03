from fastapi import HTTPException


def error_envelope(code, message):
    return {"error": {"code": code, "message": message}}


def raise_error(status_code, code, message):
    raise HTTPException(status_code=status_code, detail=error_envelope(code, message))
