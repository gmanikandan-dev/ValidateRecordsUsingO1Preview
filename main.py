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
    
    print(f"Tenant: {record['tenant_name']}, "+
          f"New Accounts: {record['new_accounts']}, "+
          f"Account Points: {record['account_points']}, "+
          f"Total Account Points: {record['total_account_points']}"
    )

    # Define the AI prompt
    messages = [
        {"role": "user", "content": f"""
        Please verify if the reported total points follow this rule:

        **Rule:** Reported total points should be equal to (new_accounts * account_points).

        **Data:**
        Tenant: {record['tenant_name']}
        Date: {record['date']}
        New Accounts: {record['new_accounts']}
        Account Points: {record['account_points']}
        Reported Total Points: {record['total_account_points']}

        **Instructions:**
        - If the data follows the rule, respond **exactly** as: `true`
        - If the data does **not** follow the rule, respond **exactly** as: `false`
        - Do **not** provide any extra text, just return `true` or `false`.

        Your response:
        """}
    ]

    # Call OpenAI API
    response = ai_client.chat.completions.create(
        model="o1-preview",
        messages=messages,
        # max_completion_tokens=10
    )

    ai_decision = response.choices[0].message.content
    print(f"AI Decision: {ai_decision}")

    # If AI returns 'false', log the issue
    if ai_decision == "false":
        return (record["tenant_name"], record["date"],  "Validation failed")

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
