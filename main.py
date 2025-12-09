# app/main.py
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .database import SessionLocal
from .auth import require_api_key

# 👉 Importamos el nuevo router para emails
from . import email_router


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
    meetingDate: Optional[str] = None  # formato 'YYYY-MM-DD'

    # meeting actions
    description: Optional[str] = None
    position: Optional[int] = None
    employeeId: Optional[int] = None
    
    # meeting topics / spec ops
    keyTopic: Optional[str] = None
    specOp: Optional[str] = None
    
    # meeting attendance
    contactId: Optional[int] = None
    
    
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
        "assetId", "createdBy", "relationshipId", "notes",
        # meetings / actions / topics / spec ops / attendance
        "meetingId", "meetingDate", "description", "position",
        "employeeId", "keyTopic", "specOp", "contactId",
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

    elif qt == "assets_search_global":
        data = search_assets_global(db, params)

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

    elif qt == "create_meeting":
        data = create_meeting(db, params)

    elif qt == "create_meeting_key_topic":
        data = create_meeting_key_topic(db, params)

    elif qt == "create_meeting_spec_op":
        data = create_meeting_spec_op(db, params)

    elif qt == "create_meeting_action":
        data = create_meeting_action(db, params)

    elif qt == "create_meeting_alatas_attendance":
        data = create_meeting_alatas_attendance(db, params)

    elif qt == "create_meeting_cust_attendance":
        data = create_meeting_cust_attendance(db, params)

    else:
        raise HTTPException(status_code=400, detail="queryType no soportado")

    return {"ok": True, "data": data}

@app.get("/meeting/report_data")
def meeting_report_data(
    meetingId: int,
    db: Session = Depends(get_db),
    api_key: str = Depends(require_api_key),
):
    """
    Devuelve todos los datos estructurados de un meeting dado su ID,
    para que el GPT pueda generar un Meeting Report.

    Respuesta:
    {
      "ok": true,
      "data": {
        "meeting": { ... },
        "keyTopics": [ ... ],
        "specialOps": [ ... ],
        "actions": [ ... ]
      }
    }
    """
    data = get_meeting_report_data(db, meetingId)
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

    # Filters
# Filters
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


# --------- ASSETS ---------

def search_assets_global(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Global asset search, NOT restricted to a specific customer.
    Used when GPT finds a vessel/asset name in an email but there is
    no asset linked yet to the selected customer.
    We treat vessel and asset as the same logical entity.
    """
    limit = int(params.get("limit", 50))
    vessel_name = params.get("vesselName")

    if not vessel_name:
        raise HTTPException(
            status_code=400,
            detail="You must send vesselName to search global assets"
        )

    sql = f"""
        SELECT TOP ({limit})
               fldAssetID         AS assetId,
               fldAssetIdentifier AS assetIdentifier,
               fldAssetType       AS assetType,
               fldAssetTypeID     AS assetTypeId,
               fldParentAssetID   AS parentAssetId,
               fldCustomerID      AS customerId,
               fldCustomerName    AS customerName,
               fldVName           AS vesselName,
               Address            AS address,
               Port               AS port,
               Terminal           AS terminal,
               PortofTerminal     AS portOfTerminal,
               ParentPort         AS parentPort,
               fldCountry         AS country,
               fldCustType        AS customerType,
               fldInterCo         AS interCompanyFlag,
               fldBlocked         AS blocked,
               fldDeleted         AS customerDeleted,
               AssetDeleted       AS assetDeleted
        FROM vwCustomerAssetAffiliation
        WHERE fldVName LIKE :vesselName
        ORDER BY fldAssetID DESC
    """

    rows = db.execute(
        text(sql),
        {"vesselName": f"%{vessel_name}%"},
    ).mappings().all()

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

# --------- CREAR MEETING ---------

def create_meeting(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un registro en tblCustMeeting y devuelve el ID del meeting.

    Parámetros esperados en params:
      - customerId (int)       → obligatorio
      - meetingDate (str)      → obligatorio, formato 'YYYY-MM-DD'
      - createdBy (str)        → opcional, por defecto 'GPT_API'
      - status (str)           → opcional, por defecto 'Planned'
      - assetId (int)          → opcional, se guarda en fldAssetID
    """

    customer_id = params.get("customerId")
    meeting_date = params.get("meetingDate")
    created_by = params.get("createdBy") or "GPT_API"
    status = params.get("status") or "Pending"
    asset_id = params.get("assetId")

    if not customer_id or not meeting_date:
        raise HTTPException(
            status_code=400,
            detail="customerId y meetingDate son obligatorios para crear el meeting",
        )

    sql = text("""
        INSERT INTO tblCustMeeting (
            fldCustomerID,
            fldCustMeetingDate,
            fldCreatedBy,
            fldCreatedOn,
            fldStatus,
            fldAssetID
        )
        OUTPUT INSERTED.fldCustMeetingID AS NewMeetingID
        VALUES (
            :customer_id,
            :meeting_date,
            :created_by,
            GETDATE(),
            :status,
            :asset_id
        );
    """)

    try:
        row = db.execute(
            sql,
            {
                "customer_id": customer_id,
                "meeting_date": meeting_date,
                "created_by": created_by,
                "status": status,
                "asset_id": asset_id,
            },
        ).mappings().first()

        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el ID del meeting creado",
        )

    new_meeting_id = row["NewMeetingID"]

    return {
        "meetingId": new_meeting_id,
        "customerId": customer_id,
        "meetingDate": meeting_date,
        "status": status,
        "assetId": asset_id,
    }

# --------- MEETINGS POR CLIENTE ---------
def get_meetings_by_customer(db: Session, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    customer_id = params.get("customerId")
    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail="customerId es obligatorio para obtener los meetings",
        )

    limit = int(params.get("limit", 50))
    status = params.get("status")

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

    rows = db.execute(text(sql), {"mid": meeting_id}).mappings().all()
    return [dict(r) for r in rows]

# --------- CREAR KEY TOPIC ---------

def create_meeting_key_topic(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un registro en tblCustMeetingKeyTopic.

    Parámetros esperados:
      - meetingId (int)                  → obligatorio
      - keyTopic (str)                   → obligatorio (texto del tópico)
      - position (int)                   → opcional → fldCustMeetingKeyTopicPos
      - createdBy (str)                  → opcional, por defecto 'GPT_API'
    """

    meeting_id = params.get("meetingId")
    key_topic = params.get("keyTopic")
    position = params.get("position")  # opcional
    created_by = params.get("createdBy") or "GPT_API"

    if not meeting_id or not key_topic:
        raise HTTPException(
            status_code=400,
            detail="meetingId y keyTopic son obligatorios para crear un key topic",
        )

    sql = text("""
        INSERT INTO tblCustMeetingKeyTopic (
            fldCustMeetingID,
            fldCustMeetingKeyTopic,
            fldCustMeetingKeyTopicPos,
            fldCreatedOn,
            fldCreatedBy
        )
        OUTPUT INSERTED.fldCustMeetingKeyTopicID AS NewKeyTopicID
        VALUES (
            :meeting_id,
            :key_topic,
            :position,
            GETDATE(),
            :created_by
        );
    """)

    try:
        row = db.execute(
            sql,
            {
                "meeting_id": meeting_id,
                "key_topic": key_topic,
                "position": position,
                "created_by": created_by,
            },
        ).mappings().first()

        db.commit()

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el ID del key topic creado",
        )

    return {
        "keyTopicId": row["NewKeyTopicID"],
        "meetingId": meeting_id,
        "keyTopic": key_topic,
        "position": position,
        "createdBy": created_by,
    }

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

    rows = db.execute(text(sql), {"mid": meeting_id}).mappings().all()
    return [dict(r) for r in rows]

# --------- CREAR SPEC OP ---------

def create_meeting_spec_op(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un registro en tblCustMeetingSpecOp.

    Parámetros esperados:
      - meetingId (int)              → obligatorio
      - specOp (str)                 → obligatorio (texto de la spec op)
      - position (int)               → opcional → fldCustMeetingSpecOpPos
      - createdBy (str)              → opcional, por defecto 'GPT_API'
    """

    meeting_id = params.get("meetingId")
    spec_op = params.get("specOp")
    position = params.get("position")  # opcional
    created_by = params.get("createdBy") or "GPT_API"

    if not meeting_id or not spec_op:
        raise HTTPException(
            status_code=400,
            detail="meetingId y specOp son obligatorios para crear una spec op",
        )

    sql = text("""
        INSERT INTO tblCustMeetingSpecOp (
            fldCustMeetingID,
            fldCustMeetingSpecOp,
            fldCustMeetingSpecOpPos,
            fldCreatedBy,
            fldCreatedOn
        )
        OUTPUT INSERTED.fldCustMeetingSpecOpID AS NewSpecOpID
        VALUES (
            :meeting_id,
            :spec_op,
            :position,
            :created_by,
            GETDATE()
        );
    """)

    try:
        row = db.execute(
            sql,
            {
                "meeting_id": meeting_id,
                "spec_op": spec_op,
                "position": position,
                "created_by": created_by,
            },
        ).mappings().first()

        db.commit()

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el ID de la Spec Op creada",
        )

    return {
        "specOpId": row["NewSpecOpID"],
        "meetingId": meeting_id,
        "specOp": spec_op,
        "position": position,
        "createdBy": created_by,
    }


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

    rows = db.execute(text(sql), {"mid": meeting_id}).mappings().all()
    return [dict(r) for r in rows]


# --------- CREAR ACTION ITEM ---------

def create_meeting_action(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea una acción de meeting en tblCustMeetingAction + un responsable en tblCustMeetingActionResp (opcional).

    Parámetros esperados (en params):
      - meetingId (int)      → obligatorio
      - description (str)    → obligatorio (fldCustMeetingAction)
      - position (int)       → opcional → fldCustMeetingActionPos
      - status (str)         → opcional, por defecto 'Open'
      - branch (str)         → opcional → fldBranch en Resp
      - employeeId (int)     → opcional → fldEmployeeID en Resp
      - createdBy (str)      → opcional, por defecto 'GPT_API'
    """

    meeting_id = params.get("meetingId")
    description = params.get("description")
    position = params.get("position")  # opcional
    status = params.get("status") or "Open"
    branch = params.get("branch")
    employee_id = params.get("employeeId")
    created_by = params.get("createdBy") or "GPT_API"

    if not meeting_id or not description:
        raise HTTPException(
            status_code=400,
            detail="meetingId y description son obligatorios para crear una action",
        )

    # 1️⃣ INSERT en tblCustMeetingAction (tu estructura real):
    # fldCustMeetingActionID (IDENTITY PK)
    # fldCustMeetingID
    # fldCustMeetingAction
    # fldCustMeetingActionPos
    # fldCreatedBy
    # fldCreatedOn
    # fldStatus
    sql_action = text("""
        INSERT INTO tblCustMeetingAction (
            fldCustMeetingID,
            fldCustMeetingAction,
            fldCustMeetingActionPos,
            fldCreatedBy,
            fldCreatedOn,
            fldStatus
        )
        OUTPUT INSERTED.fldCustMeetingActionID AS NewActionID
        VALUES (
            :meeting_id,
            :description,
            :position,
            :created_by,
            GETDATE(),
            :status
        );
    """)

    try:
        action_row = db.execute(
            sql_action,
            {
                "meeting_id": meeting_id,
                "description": description,
                "position": position,
                "created_by": created_by,
                "status": status,
            },
        ).mappings().first()

        if not action_row:
            db.rollback()
            raise HTTPException(
                status_code=500,
                detail="No se pudo obtener el ID de la acción creada",
            )

        new_action_id = action_row["NewActionID"]

        # 2️⃣ INSERT en tblCustMeetingActionResp (responsable) si tenemos datos
        # Estructura:
        #   fldCustMeetingActionRespID (IDENTITY PK)
        #   fldCustMeetingActionID
        #   fldBranch
        #   fldEmployeeID
        #   fldCreatedBy
        #   fldCreatedOn
        #   fldEmployeeID_B4Merging
        new_resp_id = None
        if branch is not None and employee_id is not None:
            sql_resp = text("""
                INSERT INTO tblCustMeetingActionResp (
                    fldCustMeetingActionID,
                    fldBranch,
                    fldEmployeeID,
                    fldCreatedBy,
                    fldCreatedOn,
                    fldEmployeeID_B4Merging
                )
                OUTPUT INSERTED.fldCustMeetingActionRespID AS NewRespID
                VALUES (
                    :action_id,
                    :branch,
                    :employee_id,
                    :created_by,
                    GETDATE(),
                    :employee_id_b4
                );
            """)

            resp_row = db.execute(
                sql_resp,
                {
                    "action_id": new_action_id,
                    "branch": branch,
                    "employee_id": employee_id,
                    "created_by": created_by,
                    "employee_id_b4": employee_id,
                },
            ).mappings().first()

            if not resp_row:
                db.rollback()
                raise HTTPException(
                    status_code=500,
                    detail="La acción se creó pero no se pudo crear el responsable",
                )

            new_resp_id = resp_row["NewRespID"]

        db.commit()

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "actionId": new_action_id,
        "meetingId": meeting_id,
        "description": description,
        "position": position,
        "status": status,
        "branch": branch,
        "employeeId": employee_id,
        "responsibleRecordId": new_resp_id,
        "createdBy": created_by,
    }

# --------- CREAR ASISTENTE ALATAS ---------

def create_meeting_alatas_attendance(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un asistente de Alatas para un meeting en tblCustMeetingAlatasAttendance.

    Parámetros esperados:
      - meetingId (int)   → obligatorio
      - employeeId (int)  → obligatorio (fldEmployeeID)
      - createdBy (str)   → opcional, por defecto 'GPT_API'
    """

    meeting_id = params.get("meetingId")
    employee_id = params.get("employeeId")
    created_by = params.get("createdBy") or "GPT_API"

    if not meeting_id or not employee_id:
        raise HTTPException(
            status_code=400,
            detail="meetingId y employeeId son obligatorios para crear un asistente Alatas",
        )

    sql = text("""
        INSERT INTO tblCustMeetingAlatasAttendance (
            fldCustMeetingID,
            fldEmployeeID,
            fldCreatedOn,
            fldCreatedBy,
            fldEmployeeID_B4Merging
        )
        OUTPUT INSERTED.fldCustMeetingAlatasAttendanceID AS NewAlatasAttendanceID
        VALUES (
            :meeting_id,
            :employee_id,
            GETDATE(),
            :created_by,
            :employee_id_b4
        );
    """)

    try:
        row = db.execute(
            sql,
            {
                "meeting_id": meeting_id,
                "employee_id": employee_id,
                "created_by": created_by,
                "employee_id_b4": employee_id,
            },
        ).mappings().first()

        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el ID del asistente Alatas creado",
        )

    return {
        "alatasAttendanceId": row["NewAlatasAttendanceID"],
        "meetingId": meeting_id,
        "employeeId": employee_id,
        "createdBy": created_by,
    }

# --------- CREAR ASISTENTE CLIENTE ---------

def create_meeting_cust_attendance(db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea un asistente del cliente para un meeting en tblCustMeetingAttendance.

    Parámetros esperados:
      - meetingId (int)   → obligatorio
      - contactId (int)   → obligatorio (fldCustContactID)
      - createdBy (str)   → opcional, por defecto 'GPT_API'
    """

    meeting_id = params.get("meetingId")
    contact_id = params.get("contactId")
    created_by = params.get("createdBy") or "GPT_API"

    if not meeting_id or not contact_id:
        raise HTTPException(
            status_code=400,
            detail="meetingId y contactId son obligatorios para crear un asistente cliente",
        )

    sql = text("""
        INSERT INTO tblCustMeetingAttendance (
            fldCustMeetingID,
            fldCustContactID,
            fldCreatedOn,
            fldCreatedBy
        )
        OUTPUT INSERTED.fldCustMeetingAttendanceID AS NewCustAttendanceID
        VALUES (
            :meeting_id,
            :contact_id,
            GETDATE(),
            :created_by
        );
    """)

    try:
        row = db.execute(
            sql,
            {
                "meeting_id": meeting_id,
                "contact_id": contact_id,
                "created_by": created_by,
            },
        ).mappings().first()

        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if not row:
        raise HTTPException(
            status_code=500,
            detail="No se pudo obtener el ID del asistente cliente creado",
        )

    return {
        "custAttendanceId": row["NewCustAttendanceID"],
        "meetingId": meeting_id,
        "contactId": contact_id,
        "createdBy": created_by,
    }

# --------- MEETING REPORT DATA (HÍBRIDO) ---------

def get_meeting_report_data(db: Session, meeting_id: int) -> Dict[str, Any]:
    """
    Devuelve un paquete estructurado con toda la info necesaria
    para que GPT redacte un Meeting Report:

      - meeting: cabecera del meeting + cliente/vessel si es posible
      - keyTopics: lista de key topics
      - specialOps: lista de special operations
      - actions: resumen de action items
    """

    # 1️⃣ Cabecera del meeting (con customer y asset info si se puede)
    header_sql = text("""
        SELECT TOP (1)
               m.fldCustMeetingID   AS meetingId,
               m.fldCustomerID      AS customerId,
               c.fldCustomerName    AS customerName,
               m.fldCustMeetingDate AS meetingDate,
               m.fldCreatedBy       AS createdBy,
               m.fldCreatedOn       AS createdOn,
               m.fldStatus          AS status,
               m.fldReportSentOn    AS reportSentOn,
               m.fldAssetID         AS assetId,
               a.fldVName           AS vesselName,
               a.fldAssetIdentifier AS assetIdentifier,
               a.fldAssetType       AS assetType
        FROM tblCustMeeting m
        LEFT JOIN tblCustomer c
               ON c.fldCustomerID = m.fldCustomerID
        LEFT JOIN vwCustomerAssetAffiliation a
               ON a.fldAssetID = m.fldAssetID
        WHERE m.fldCustMeetingID = :mid
    """)

    header_row = db.execute(header_sql, {"mid": meeting_id}).mappings().first()

    if not header_row:
        raise HTTPException(status_code=404, detail="Meeting not found")

    meeting_header = dict(header_row)

    # 2️⃣ Key topics, special ops, actions reutilizando tus funciones existentes
    key_topics = get_meeting_key_topics(db, {"meetingId": meeting_id})
    spec_ops   = get_meeting_spec_ops(db, {"meetingId": meeting_id})
    actions    = get_meeting_actions(db, {"meetingId": meeting_id})

    return {
        "meeting": meeting_header,
        "keyTopics": key_topics,
        "specialOps": spec_ops,
        "actions": actions,
    }


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


# ---------------------------
# 👉 INCLUIR EL NUEVO ROUTER
# ---------------------------
app.include_router(email_router.router)
