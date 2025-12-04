import os
import msal
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
MAILBOX = os.getenv("MAILBOX")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]


def get_token() -> str:
    """Authenticate using Microsoft identity platform and return an access token."""
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(result)
    return result["access_token"]


def fetch_recent_messages(top: int = 5):
    """Fetch the last N emails from the ServiceHub mailbox, including full body and header metadata."""
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # First request: metadata only, ordered by date
    url = (
        f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages"
        f"?$top={top}&$orderby=receivedDateTime desc"
    )

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    messages = []

    for m in data.get("value", []):
        msg_id = m["id"]

        # Second request: fetch full message body + headers
        full_url = f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages/{msg_id}"
        full_resp = requests.get(full_url, headers=headers)
        full_resp.raise_for_status()
        full = full_resp.json()

        body_html = full.get("body", {}).get("content", "") or ""

        messages.append({
            "id": msg_id,
            "internetMessageId": full.get("internetMessageId"),  # RFC822 ID
            "inReplyTo": full.get("inReplyTo"),                  # 🔥 helps detect replies / updates
            "from": full.get("from", {}).get("emailAddress", {}).get("address"),
            "subject": full.get("subject"),
            "body_html": body_html,
            "to": [r["emailAddress"]["address"] for r in full.get("toRecipients", [])],
            "cc": [r["emailAddress"]["address"] for r in full.get("ccRecipients", [])],
        })

    return messages


# -----------------------------
# TEST BLOCK (run manually)
# -----------------------------
if __name__ == "__main__":
    print("\nTesting email fetch...\n")

    try:
        msgs = fetch_recent_messages(5)
        print(f"Fetched {len(msgs)} messages.\n")

        for i, m in enumerate(msgs, start=1):
            print(f"--- Message {i} ---")
            print(f"ID: {m['id']}")
            print(f"InternetMessageID: {m.get('internetMessageId')}")
            print(f"InReplyTo: {m.get('inReplyTo')}")
            print(f"From: {m['from']}")
            print(f"To: {', '.join(m['to'])}")
            print(f"Subject: {m['subject']}")
            snippet = (m['body_html'] or "").replace("\n", " ")[:200]
            print(f"Body snippet: {snippet!r}")
            print()

    except Exception as err:
        print("❌ ERROR while fetching messages:")
        print(err)

