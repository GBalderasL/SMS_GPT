from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from .database import SessionLocal
from .auth import require_api_key


app = FastAPI(title="SMS API")


# --------- MODELOS ---------

class QueryRequest(BaseModel):
    queryType: str
    params: Optional[Dict[str, Any]] = None

    # Compatibilidad cuando GPT no envía params
    name: Optional[str] = None
    limit: Optional[int] = None
    customerName: Optional[str] = None
    branch: Optional[str] = None
    status: Optional[str] = None

    # assets
    customerId: Optional[int] = None
    assetTypeId: Optional[int] = None
    assetType: Optional[str] = None
    vesselName: Optional[str] = None
    country: Optional[str] = None
    interCo: Optional[bool] = None
    blocked: Optional[bool] = None
    assetDeleted: Optional[bool] = None

    # creación de cotizaciones (uspCreateQuoteAPI)
    assetId: Optional[int] = None
    createdBy: Optional[str] = None
    isAlatas: Optional[bool] = None
    relationshipId: Optional[int] = None
    notes: Optional[str] = None

    # meetings
    meetingId: Optional[int] = None


# --------- DEPENDENCIAS ---------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --------- ENDPOINTS ---------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/query")
def run_query(
    body: QueryRequest,
    db: Session = Depends(get_db),
    api_key: str = Depends(require_api_key),
):
    qt = body.queryType
    params: Dict[str, Any] = body.params or {}

    # Compatibilidad: mover campos sueltos a params
    for field in [
        "name", "limit", "customerName", "branch", "status",
        "customerId", "assetTypeId", "assetType",
        "vesselName", "country", "interCo", "blocked", "assetDeleted",
        # creación de cotización
        "assetId", "createdBy", "isAlatas", "relationshipId", "notes",
        # meetings
        "meetingId",
    ]:
        value = getattr(body, field)
        if value is not None and field not in params:
            params[field] = value

    # ---- CONSULTAS / OPERACIONES ----
    if qt == "customers_search":
        data = search_customers(db, params)

    elif qt == "quotes_by_customer":
        data = get_quotes_by_customer(db, params)

    elif qt == "quotes_count_by_branch_status":
        data = get_quotes_count_by_branch_status(db, params)

    elif qt == "assets_by_customer":
        data = get_assets_by_customer(db, params)

    elif qt == "create_quote_from_asset":
        data = create_quote_from_asset(db, params)

    elif qt == "customer_contacts":
        data = get_customer_contacts(db, params)

    # -------- MEETINGS --------
    elif qt == "meetings_by_customer":
        data = get_meetings_by_customer(db, params)

    elif qt == "meeting_key_topics":
        data = get_meeting_key_topics(db, params)

    elif qt == "meeting_spec_ops":
        data = get_meeting_spec_ops(db, params)

    elif qt == "meeting_actions":
        data = get_meeting_actions(db, params)

    else:
        raise HTTPException(status_code=400, detail="queryType no soportado")

    return {"ok": True, "data": data}


# --------- CONSULTAS SQL ---------

def search_customers(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    name = params.get("name")
    limit = int(params.get("limit", 20))

    sql = f"""
        SELECT TOP ({limit})
               fldCustomerID AS id,
               fldCustomerName AS name,
               fldEmail AS email
        FROM tblCustomer
        WHERE 1 = 1
    """

    sql_params: Dict[str, Any] = {}
    if name:
        sql += " AND fldCustomerName LIKE :name"
        sql_params["name"] = f"%{name}%"

    sql += " ORDER BY fldCustomerID DESC"

    rows = db.execute(text(sql), sql_params).mappings().all()
    return [dict(r) for r in rows]


def get_quotes_by_customer(db: Session, params: Dict[str, Any]):
    customer_name = params.get("customerName")
    if not customer_name:
        raise HTTPException(status_code=400, detail="customerName es obligatorio")

    limit = int(params.get("limit", 20))

    sql = f"""
        SELECT TOP ({limit})
               fldQuoteID AS id,
               fldQuoteNo AS quoteNumber,
               Branch AS branch,
               fldQCreatedDate AS createdOn,
               fldUSDValue AS totalAmount,
               fldCustomerName AS customerName,
               fldQStatus AS quoteStatus
        FROM vwGlobalQuotes
        WHERE fldCustomerName LIKE :c
        ORDER BY fldQCreatedDate DESC
    """

    rows = db.execute(
        text(sql),
        {"c": f"%{customer_name}%"},
    ).mappings().all()

    return [dict(r) for r in rows]


def get_quotes_count_by_branch_status(db: Session, params: Dict[str, Any]):
    branch = params.get("branch")
    status = params.get("status")

    if not branch or not status:
        raise HTTPException(status_code=400, detail="branch y status son obligatorios")

    sql = """
        SELECT
            :branch AS branch,
            :status AS status,
            COUNT(*) AS quotesCount,
            COALESCE(SUM(fldUSDValue), 0) AS totalAmount
        FROM vwGlobalQuotes
        WHERE Branch = :branch AND fldQStatus = :status
    """

    row = db.execute(
        text(sql),
        {"branch": branch, "status": status},
    ).mappings().first()

    return (
        [dict(row)]
        if row
        else [{"branch": branch, "status": status, "quotesCount": 0, "totalAmount": 0}]
    )


# --------- ASSETS ---------

def get_assets_by_customer(db: Session, params: Dict[str, Any]):

    limit = int(params.get("limit", 50))

    customer_id = params.get("customerId")
    vessel_name = params.get("vesselName")

    # Requerimos al menos UNO
    if not customer_id and not vessel_name:
        raise HTTPException(
            status_code=400, detail="Debes enviar customerId o vesselName"
        )

    # 1️⃣ MATCH EXACTO
    if vessel_name:
        exact_sql = f"""
            SELECT TOP ({limit})
                   fldAssetID AS assetId,
                   fldAssetIdentifier AS assetIdentifier,
                   fldAssetType AS assetType,
                   fldAssetTypeID AS assetTypeId,
                   fldParentAssetID AS parentAssetId,
                   fldCustomerID AS customerId,
                   fldCustomerName AS customerName,
                   fldVName AS vesselName,
                   Address AS address,
                   Port AS port,
                   Terminal AS terminal,
                   PortofTerminal AS portOfTerminal,
                   ParentPort AS parentPort,
                   fldCountry AS country,
                   fldCustType AS customerType,
                   fldInterCo AS interCompanyFlag,
                   fldBlocked AS blocked,
                   fldDeleted AS customerDeleted,
                   AssetDeleted AS assetDeleted
            FROM vwCustomerAssetAffiliation
            WHERE fldVName = :exact_name
        """

        sql_params: Dict[str, Any] = {"exact_name": vessel_name}
        if customer_id:
            exact_sql += " AND fldCustomerID = :cid"
            sql_params["cid"] = customer_id

        rows = db.execute(text(exact_sql), sql_params).mappings().all()
        if rows:
            return [dict(r) for r in rows]

    # 2️⃣ MATCH LIKE
    sql = f"""
        SELECT TOP ({limit})
               fldAssetID AS assetId,
               fldAssetIdentifier AS assetIdentifier,
               fldAssetType AS assetType,
               fldAssetTypeID AS assetTypeId,
               fldParentAssetID AS parentAssetId,
               fldCustomerID AS customerId,
               fldCustomerName AS customerName,
               fldVName AS vesselName,
               Address AS address,
               Port AS port,
               Terminal AS terminal,
               PortofTerminal AS portOfTerminal,
               ParentPort AS parentPort,
               fldCountry AS country,
               fldCustType AS customerType,
               fldInterCo AS interCompanyFlag,
               fldBlocked AS blocked,
               fldDeleted AS customerDeleted,
               AssetDeleted AS assetDeleted
        FROM vwCustomerAssetAffiliation
        WHERE 1 = 1
    """

    sql_params2: Dict[str, Any] = {}

    if customer_id:
        sql += " AND fldCustomerID = :cid"
        sql_params2["cid"] = customer_id

    if vessel_name:
        sql += " AND fldVName LIKE :vesselName"
        sql_params2["vesselName"] = f"%{vessel_name}%"

    # Filtros opcionales
    if params.get("assetTypeId") is not None:
        sql += " AND fldAssetTypeID = :assetTypeId"
        sql_params2["assetTypeId"] = params["assetTypeId"]

    if params.get("assetType"):
        sql += " AND fldAssetType LIKE :assetType"
        sql_params2["assetType"] = f"%{params['assetType']}%"

    if params.get("country"):
        sql += " AND fldCountry = :country"
        sql_params2["country"] = params["country"]

    if params.get("interCo") is not None:
        sql += " AND fldInterCo = :interCo"
        sql_params2["interCo"] = 1 if params["interCo"] else 0

    if params.get("blocked") is not None:
        sql += " AND fldBlocked = :blocked"
        sql_params2["blocked"] = 1 if params["blocked"] else 0

    if params.get("assetDeleted") is not None:
        sql += " AND AssetDeleted = :assetDeleted"
        sql_params2["assetDeleted"] = 1 if params["assetDeleted"] else 0

    sql += " ORDER BY fldAssetID DESC"

    rows = db.execute(text(sql), sql_params2).mappings().all()
    return [dict(r) for r in rows]


# --------- CONTACTOS POR CLIENTE ---------

def get_customer_contacts(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    customer_id = params.get("customerId")
    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail="customerId es obligatorio para obtener los contactos",
        )

    limit = int(params.get("limit", 50))

    sql = f"""
        SELECT TOP ({limit})
               fldCustContactID     AS contactId,
               fldCJobTitle         AS jobTitle,
               fldFullName          AS fullName,
               fldEmail             AS email,
               fldMobileNo          AS mobileNo,
               fldPhoneNo           AS phoneNo,
               fldContactSalutation AS salutation,
               fldBody              AS body,
               fldBodyEnd           AS bodyEnd,
               fldContCreatedDate   AS createdOn,
               fldContCreatedBy     AS createdBy,
               fldCustomerID        AS customerId
        FROM vwCustContact
        WHERE fldCustomerID = :cid
        ORDER BY fldFullName
    """

    rows = db.execute(
        text(sql),
        {"cid": customer_id},
    ).mappings().all()

    return [dict(r) for r in rows]


# --------- MEETINGS POR CLIENTE ---------

def get_meetings_by_customer(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    customer_id = params.get("customerId")
    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail="customerId es obligatorio para obtener los meetings",
        )

    limit = int(params.get("limit", 50))
    status = params.get("status")  # opcional

    sql = f"""
        SELECT TOP ({limit})
               fldCustMeetingID   AS meetingId,
               fldCustomerID      AS customerId,
               fldCustMeetingDate AS meetingDate,
               fldCreatedBy       AS createdBy,
               fldCreatedOn       AS createdOn,
               fldStatus          AS status,
               fldReportSentOn    AS reportSentOn,
               fldAssetID         AS assetId
        FROM tblCustMeeting
        WHERE fldCustomerID = :cid
    """

    sql_params = {"cid": customer_id}

    if status:
        sql += " AND fldStatus = :status"
        sql_params["status"] = status

    sql += " ORDER BY fldCustMeetingDate DESC, fldCustMeetingID DESC"

    rows = db.execute(text(sql), sql_params).mappings().all()
    return [dict(r) for r in rows]


# --------- DETALLES DE MEETING ---------
# Basado en:
#   tblCustMeetingKeyTopic
#   tblCustMeetingSpecOp
#   vwCustMeetingActionRespConcat
# Uso SELECT * para respetar tu esquema actual; luego puedes añadir alias si quieres un contrato de API más “bonito”.

def get_meeting_key_topics(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    meeting_id = params.get("meetingId")
    if not meeting_id:
        raise HTTPException(
            status_code=400,
            detail="meetingId es obligatorio para obtener los key topics",
        )

    sql = """
        SELECT *
        FROM tblCustMeetingKeyTopic
        WHERE fldCustMeetingID = :mid
        ORDER BY fldCustMeetingKeyTopicID
    """

    rows = db.execute(
        text(sql),
        {"mid": meeting_id},
    ).mappings().all()

    return [dict(r) for r in rows]


def get_meeting_spec_ops(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    meeting_id = params.get("meetingId")
    if not meeting_id:
        raise HTTPException(
            status_code=400,
            detail="meetingId es obligatorio para obtener los spec ops",
        )

    sql = """
        SELECT *
        FROM tblCustMeetingSpecOp
        WHERE fldCustMeetingID = :mid
        ORDER BY fldCustMeetingSpecOpID
    """

    rows = db.execute(
        text(sql),
        {"mid": meeting_id},
    ).mappings().all()

    return [dict(r) for r in rows]


def get_meeting_actions(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    meeting_id = params.get("meetingId")
    if not meeting_id:
        raise HTTPException(
            status_code=400,
            detail="meetingId es obligatorio para obtener las action items",
        )

    sql = """
        SELECT *
        FROM vwCustMeetingActionRespConcat
        WHERE fldCustMeetingID = :mid
    """

    rows = db.execute(
        text(sql),
        {"mid": meeting_id},
    ).mappings().all()

    return [dict(r) for r in rows]


# --------- CREAR COTIZACIÓN ---------

def create_quote_from_asset(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    customer_id = params.get("customerId")
    asset_id = params.get("assetId")

    if not customer_id or not asset_id:
        raise HTTPException(
            status_code=400,
            detail="customerId y assetId son obligatorios para crear la cotización",
        )

    branch = params.get("branch")
    created_by = params.get("createdBy") or "GPT_API"
    relationship_id = params.get("relationshipId")
    notes = params.get("notes")

    sql = text("""
        DECLARE @NewQuoteID INT, @NewQuoteNo NVARCHAR(50);

        EXEC dbo.uspCreateQuoteAPI
            @CustomerID     = :customer_id,
            @AssetID        = :asset_id,
            @Branch         = :branch,
            @CreatedBy      = :created_by,
            @RelationshipID = :relationship_id,
            @Notes          = :notes,
            @NewQuoteID     = @NewQuoteID OUTPUT,
            @NewQuoteNo     = @NewQuoteNo OUTPUT;

        SELECT @NewQuoteID AS NewQuoteID, @NewQuoteNo AS NewQuoteNo;
    """)

    try:
        row = db.execute(
            sql,
            {
                "customer_id": customer_id,
                "asset_id": asset_id,
                "branch": branch,
                "created_by": created_by,
                "relationship_id": relationship_id,
                "notes": notes,
            },
        ).mappings().first()

        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el resultado de la creación de la cotización",
        )

    return {
        "quoteId": row["NewQuoteID"],
        "quoteNo": row["NewQuoteNo"],
        "customerId": customer_id,
        "assetId": asset_id,
        "branch": branch,
    }
