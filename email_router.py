# app/email_router.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from bs4 import BeautifulSoup

from .auth import require_api_key
from .graph_client import fetch_recent_messages
from .database import SessionLocal

router = APIRouter(prefix="/email", tags=["email"])


# --------- DB DEPENDENCY ---------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --------- UTILS ---------

def html_to_text(html: str) -> str:
    """Convierte HTML a texto plano con saltos de línea razonables."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator="\n")


# --------- 1️⃣ OBTENER CORREOS RECIENTES ---------

@router.get("/recent")
def get_recent_emails(
    limit: int = 5,
    api_key: str = Depends(require_api_key),
):
    """
    Return last N emails from ServiceHub mailbox.
    Each email includes an ID and plain-text body.

    Fields:
      - id: Graph message ID
      - internetMessageId: global RFC822 ID (used for tracking / duplicates)
      - inReplyTo: internetMessageId of the parent email (if it’s a reply)
      - from, to, subject, bodyText
    """
    try:
        raw_messages = fetch_recent_messages(top=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    processed = []
    for m in raw_messages:
        processed.append({
            "id": m.get("id"),
            "internetMessageId": m.get("internetMessageId"),
            "inReplyTo": m.get("inReplyTo"),
            "from": m.get("from"),
            "to": m.get("to", []),
            "subject": m.get("subject"),
            "bodyText": html_to_text(m.get("body_html") or "")
        })

    return {"ok": True, "data": processed}


# --------- 2️⃣ CHECK: ¿EMAIL YA FUE PROCESADO? ---------

@router.get("/was_processed")
def was_processed(
    internetMessageId: str,
    db: Session = Depends(get_db),
    api_key: str = Depends(require_api_key),
):
    """
    Check if an email (identified by InternetMessageID) was already processed
    and linked to a quote.
    """
    sql = text("""
        SELECT 
            fldQuoteID    AS quoteId,
            fldQuoteNo    AS quoteNo,
            fldCustomerID AS customerId,
            fldAssetID    AS assetId
        FROM tblEmailQuoteTracking
        WHERE InternetMessageID = :imid
    """)

    row = db.execute(sql, {"imid": internetMessageId}).mappings().first()

    if row:
        return {
            "processed": True,
            "quoteId": row["quoteId"],
            "quoteNo": row["quoteNo"],
            "customerId": row["customerId"],
            "assetId": row["assetId"],
        }

    return {"processed": False}


# --------- 3️⃣ TRACK: GUARDAR QUE SE PROCESÓ UN EMAIL ---------

@router.post("/track")
def track_email(
    data: dict,
    db: Session = Depends(get_db),
    api_key: str = Depends(require_api_key),
):
    """
    Store that an email (InternetMessageID) has been processed and
    linked to a quote. GPT should call this AFTER a quote is created.

    Expected JSON:
    {
      "internetMessageId": "...",   # REQUIRED
      "forwardedEmailId": "...",    # optional
      "subject": "...",
      "from": "sender@domain.com",
      "customerId": 123,
      "assetId": 456,
      "quoteId": 789,
      "quoteNo": "AUK25Q419935",
      "notes": "Created quote for ..."
    }
    """
    # Validación mínima
    if not data.get("internetMessageId"):
        raise HTTPException(status_code=400, detail="internetMessageId is required")

    sql = text("""
        INSERT INTO tblEmailQuoteTracking
        (
            InternetMessageID,
            ForwardedEmailID,
            Subject,
            FromAddress,
            fldCustomerID,
            fldAssetID,
            fldQuoteID,
            fldQuoteNo,
            fldNotes
        )
        VALUES
        (
            :imid,
            :fid,
            :subject,
            :fromAddr,
            :customerId,
            :assetId,
            :quoteId,
            :quoteNo,
            :notes
        )
    """)

    db.execute(sql, {
        "imid": data.get("internetMessageId"),
        "fid": data.get("forwardedEmailId"),
        "subject": data.get("subject"),
        "fromAddr": data.get("from"),
        "customerId": data.get("customerId"),
        "assetId": data.get("assetId"),
        "quoteId": data.get("quoteId"),
        "quoteNo": data.get("quoteNo"),
        "notes": data.get("notes"),
    })

    db.commit()
    return {"ok": True}

