import os
import smtplib
from email.message import EmailMessage
from pymongo import MongoClient
from fastapi import FastAPI
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# Email Configuration
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_FROM = os.getenv("EMAIL_FROM")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize OpenAI client
ai_client = OpenAI(api_key=OPENAI_API_KEY)

# FastAPI app
app = FastAPI()

def validate_record(record):
    """Validates a single record and detects anomalies using LLM."""
    expected_total = record["new_accounts"] * record["account_points"]
    if record["total_account_points"] == expected_total:
        return None  # No issue found

    # Use AI model to evaluate anomaly
    prompt = f"""
    The following data might have an inconsistency. Can you evaluate if it's an anomaly?

    Tenant: {record['tenant_name']}
    Date: {record['date']}
    New Accounts: {record['new_accounts']}
    Account Points: {record['account_points']}
    Reported Total Points: {record['total_account_points']}
    Expected Total Points: {expected_total}

    Does this look like an error? Answer with 'Yes' or 'No' and explain briefly.
    """

    response = ai_client.completions.create(
        model="o1-preview",
        prompt=prompt,
        max_tokens=50
    )

    ai_decision = response.choices[0].text.strip()
    print(f"AI Decision: {ai_decision}")

    if "Yes" in ai_decision:
        return (record["tenant_name"], record["date"], ai_decision)

    return None

def send_email(discrepancies):
    """Sends an email notification for flagged records."""
    if not discrepancies:
        return

    message = EmailMessage()
    message["Subject"] = "CSV Validation Alert: Discrepancies Found"
    message["From"] = EMAIL_FROM
    message["To"] = EMAIL_TO

    email_body = "The following records have discrepancies:\n\n"
    for tenant, date, reason in discrepancies:
        email_body += f"Tenant: {tenant}, Date: {date}, Reason: {reason}\n"

    message.set_content(email_body)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_FROM, SMTP_PASSWORD)
        server.send_message(message)

    print("Email sent successfully.")

@app.get("/validate")
def process_records():
    """Fetches pending records from MongoDB, validates them, and updates status."""
    pending_records = collection.find({"status": "pending"})

    all_discrepancies = []
    for record in pending_records:
        discrepancy = validate_record(record)
        if discrepancy:
            all_discrepancies.append(discrepancy)

        # Update MongoDB status to completed
        collection.update_one({"_id": record["_id"]}, {"$set": {"status": "completed"}})

    # Send an email if any discrepancies were found
    send_email(all_discrepancies)

    return {"message": "Validation completed", "errors_found": len(all_discrepancies)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
