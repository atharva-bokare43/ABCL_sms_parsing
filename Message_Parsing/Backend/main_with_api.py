
"""
CSV Processing Utility - Processes financial SMS messages from CSV files
"""
import os
import re
import csv
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("financial_sms.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
try:
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-thinking-exp-01-21",
        temperature=0.7,
        max_tokens=None,
        google_api_key=os.getenv("API_KEY")
    )
except Exception as e:
    logger.error(f"Error initializing Gemini model: {e}")
    model = None

def sanitize_llm_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize LLM output to clean strings and convert values safely."""
    for key, value in data.items():
        if value is None:
            continue

        if key in ['account_number', 'card_number', 'folio_number', 'policy_number']:
            # Always treat these as strings
            data[key] = str(value).strip()

        elif isinstance(value, str):
            cleaned = value.strip()
            if cleaned.replace('.', '', 1).isdigit():
                try:
                    data[key] = float(cleaned) if '.' in cleaned else int(cleaned)
                except Exception:
                    data[key] = cleaned
            else:
                data[key] = cleaned

        elif isinstance(value, (int, float)):
            data[key] = value  # Keep as-is

        else:
            data[key] = str(value).strip()  # Fallback: string cast

    return data

# Initialize FastAPI app
app = FastAPI(title="Financial SMS Analyzer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class CSVUploadRequest(BaseModel):
    file_path: str
    delimiter: str = ","
    has_header: bool = True

class UploadResponse(BaseModel):
    status: str
    message: str
    records_processed: int = 0
    records_failed: int = 0
    
class ProcessingStatus(BaseModel):
    total: int
    processed: int
    succeeded: int
    failed: int
    status: str
    
class MessageResponse(BaseModel):
    message_type: str
    important_points: List[str]
    data: Optional[Dict[str, Any]] = None

# Global variable to track processing status
processing_status = {
    "total": 0,
    "processed": 0,
    "succeeded": 0,
    "failed": 0,
    "status": "idle"
}

# Date parsing utilities
def parse_date(date_str: str) -> Optional[str]:
    """
    Parse various date formats and convert to ISO format (YYYY-MM-DD)
    Handles formats like DD-MMM-YY, DD/MM/YYYY, YY-MM-DD, etc.
    """
    try:
        # Handle formats like "05-MAY-24" or "05 MAY 24"
        pattern1 = re.compile(r'(\d{1,2})[-\s/]([A-Za-z]{3})[-\s/](\d{2,4})')
        match1 = pattern1.search(date_str)
        if match1:
            day, month_str, year = match1.groups()
            month_map = {
                'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
            }
            month = month_map.get(month_str.upper(), '01')
            if len(year) == 2:
                year = f"20{year}"
            day = day.zfill(2)
            return f"{year}-{month}-{day}"

        # Handle formats like "05/11/2024" or "05-11-2024"
        pattern2 = re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})')
        match2 = pattern2.search(date_str)
        if match2:
            day, month, year = match2.groups()
            day = day.zfill(2)
            month = month.zfill(2)
            return f"{year}-{month}-{day}"

        # Handle formats like "2024-05-11"
        pattern3 = re.compile(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})')
        match3 = pattern3.search(date_str)
        if match3:
            year, month, day = match3.groups()
            day = day.zfill(2)
            month = month.zfill(2)
            return f"{year}-{month}-{day}"

        # Handle formats like "24-06-03" (YY-MM-DD)
        pattern4 = re.compile(r'(\d{2})[-/](\d{1,2})[-/](\d{1,2})')
        match4 = pattern4.search(date_str)
        if match4:
            year, month, day = match4.groups()
            year = f"20{year}"
            day = day.zfill(2)
            month = month.zfill(2)
            return f"{year}-{month}-{day}"

        # Fallback to datetime.strptime() with known formats
        for fmt in ["%d-%b-%y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return None

    except Exception as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return None


# Helper function to clean numeric strings
def clean_numeric_string(value_str):
    if not value_str:
        return None
        
    # Remove commas and any trailing periods
    cleaned = str(value_str).replace(',', '')
    
    # Remove trailing period if present
    if cleaned.endswith('.'):
        cleaned = cleaned[:-1]
        
    # Handle cases where there might be multiple dots
    if cleaned.count('.') > 1:
        # Keep only the first decimal point
        first_dot = cleaned.find('.')
        cleaned = cleaned[:first_dot+1] + cleaned[first_dot+1:].replace('.', '')
    
    return cleaned

# Message type classification
def classify_message_type(message: str) -> str:
    message_lower = message.lower()

    # Salary messages
    if "salary" in message_lower or "credited" in message_lower or "deposited" in message_lower:
        return "SALARY_CREDIT"

    # EMI/Loan messages
    if ("loan" in message_lower or "emi" in message_lower) and (
        "debited" in message_lower or "deducted" in message_lower or "due on" in message_lower):
        return "EMI_PAYMENT"

    # Credit Card transactions
    if "credit card" in message_lower or "creditcard" in message_lower or "card member" in message_lower:
        return "CREDIT_CARD_TRANSACTION"

    # SIP investments
    if "sip" in message_lower and any(keyword in message_lower for keyword in [
        "processed", "deducted", "under folio", "has been processed"
    ]):
        return "SIP_INVESTMENT"

    # Insurance messages
    if "insurance" in message_lower or "premium" in message_lower or "policy" in message_lower:
        return "INSURANCE_PAYMENT"

    # Generic transactions
    if "credited" in message_lower or "deposited" in message_lower:
        return "CREDIT_TRANSACTION"
    if "debited" in message_lower or "deducted" in message_lower:
        return "DEBIT_TRANSACTION"

    return "OTHER_FINANCIAL"


# Data extraction functions
def extract_financial_data(message_type: str, message: str) -> Dict[str, Any]:
    data = {}

    # Common patterns
    amount_pattern = r'(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)'
    account_pattern = r'(?:A/c(?:\s+no)?\.?|Ac(?:\s+no)?\.?|card ending|account)\s*[:\-]?\s*([A-Z0-9]+\d{4})'
    date_pattern = r'(\d{2}[-/]\d{2}[-/]\d{2,4}|\d{2}[-\s][A-Za-z]{3}[-\s]\d{2,4})'
    balance_pattern = r'(?:Avl bal|available balance|net available balance)[^0-9]*(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)'

    # Amount
    amount_match = re.search(amount_pattern, message)
    if amount_match:
        amount_str = clean_numeric_string(amount_match.group(1))
        if amount_str:
            try:
                data['amount'] = float(amount_str)
            except ValueError:
                logger.error(f"Failed to convert amount: {amount_str}")

    # Account/Card number
    account_match = re.search(account_pattern, message, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)

    # Date
    date_match = re.search(date_pattern, message)
    if date_match:
        date_str = date_match.group(1)
        parsed_date = parse_date(date_str)
        if parsed_date:
            data['transaction_date'] = parsed_date

    # Balance
    balance_match = re.search(balance_pattern, message, re.IGNORECASE)
    if balance_match:
        balance_str = clean_numeric_string(balance_match.group(1))
        if balance_str:
            try:
                data['available_balance'] = float(balance_str)
            except ValueError:
                logger.error(f"Failed to convert balance: {balance_str}")

    # Bank Name extraction (generic)
    bank_match = re.search(r'(?:from|to|by)?\s*([A-Z][A-Za-z\s]+)\s+(?:Bank|BANK|bank)', message)
    if bank_match:
        data['bank_name'] = bank_match.group(1).strip() + " Bank"

    # Message-specific extractions
    if message_type == "SALARY_CREDIT":
        employer_match = re.search(r'- ([A-Za-z\s]+) -', message)
        if employer_match:
            data['employer'] = employer_match.group(1).strip()
        else:
            data['employer'] = "General Transaction"


    elif message_type == "EMI_PAYMENT":
        loan_ref_match = re.search(r'([A-Z0-9]+\d{6,})', message)
        if loan_ref_match:
            data['loan_reference'] = loan_ref_match.group(1)

        loan_type_match = re.search(r'Loan\s+([A-Za-z]+)', message, re.IGNORECASE)
        if loan_type_match:
            data['loan_type'] = loan_type_match.group(1)
        else:
            data['loan_type'] = "Personal Loan"

    elif message_type == "CREDIT_CARD_TRANSACTION":
        merchant_match = re.search(r'at\s+([A-Za-z\s]+)\s+on', message, re.IGNORECASE)
        if merchant_match:
            data['merchant'] = merchant_match.group(1).strip()

        auth_code_match = re.search(r'Authorization code[-:]?\s*(\w+)', message)
        if auth_code_match:
            data['authorization_code'] = auth_code_match.group(1)

        outstanding_match = re.search(
            r'total outstanding is\s+(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)', message, re.IGNORECASE)
        if outstanding_match:
            outstanding_str = clean_numeric_string(outstanding_match.group(1))
            if outstanding_str:
                try:
                    data['total_outstanding'] = float(outstanding_str)
                except ValueError:
                    logger.error(f"Failed to convert outstanding: {outstanding_str}")

    elif message_type == "SIP_INVESTMENT":
        sip_data = extract_sip_data(message)
        data.update(sip_data)

    elif message_type == "INSURANCE_PAYMENT":
        policy_match = re.search(r'policy(?:\s+no\.?| number)?[:\-]?\s*([A-Z0-9]+)', message, re.IGNORECASE)
        if policy_match:
            data['policy_number'] = policy_match.group(1)

        # Try to find known insurance companies
        for company in ["LIC", "HDFC Life", "ICICI Prudential", "SBI Life", "Tata AIA"]:
            if company.lower() in message.lower():
                data['insurance_company'] = company
                break

        data['insurance_type'] = "Life Insurance"

    return data


# Specialized function for SIP message extraction
def extract_sip_data(message: str) -> Dict[str, Any]:
    data = {}
    
    # Extract amount - using both Rs and INR patterns
    amount_pattern = r'(?:Rs\.?|INR)\s*([\d,]+\.?\d*)'
    amount_match = re.search(amount_pattern, message)
    if amount_match:
        amount_str = clean_numeric_string(amount_match.group(1))
        if amount_str:
            try:
                data['amount'] = float(amount_str)
            except ValueError:
                logger.error(f"Failed to convert SIP amount: {amount_str}")
    
    # Extract date - handle multiple formats
    # First look for specific formats in the SIP message
    date_patterns = [
        r'SIP of (\d{2}/\d{2}/\d{4})',  # SIP of 11/05/2024
        r'SIP of (\d{2}-\d{2}-\d{4})',  # SIP of 11-05-2024
        r'SIP of (\d{2}-[A-Za-z]{3}-\d{2,4})'  # SIP of 11-MAY-24
    ]
    
    for pattern in date_patterns:
        date_match = re.search(pattern, message)
        if date_match:
            date_str = date_match.group(1)
            parsed_date = parse_date(date_str)
            if parsed_date:
                data['transaction_date'] = parsed_date
                break
    
    # Fallback to any date in the message if no specific SIP date found
    if 'transaction_date' not in data:
        general_date_pattern = r'(\d{2}[-/]\d{2}[-/]\d{4}|\d{2}[-\s][A-Za-z]{3}[-\s]\d{2,4})'
        date_match = re.search(general_date_pattern, message)
        if date_match:
            date_str = date_match.group(1)
            parsed_date = parse_date(date_str)
            if parsed_date:
                data['transaction_date'] = parsed_date
    
    # Extract folio number
    folio_pattern = r'[Ff]olio\s+([A-Z0-9]+)'
    folio_match = re.search(folio_pattern, message)
    if folio_match:
        data['folio_number'] = folio_match.group(1)
    
    # Extract fund name - more flexible pattern
    fund_pattern = r'in\s+([A-Za-z\s\-]+?)(?:Regular|has been)'
    fund_match = re.search(fund_pattern, message)
    if fund_match:
        data['fund_name'] = fund_match.group(1).strip()
    
    # Extract NAV with improved pattern
    nav_pattern = r'NAV of\s+([\d\.]+)'
    nav_match = re.search(nav_pattern, message)
    if nav_match:
        nav_str = clean_numeric_string(nav_match.group(1))
        if nav_str:
            try:
                data['nav_value'] = float(nav_str)
            except ValueError:
                logger.error(f"Failed to convert NAV: {nav_str}")
    
    return data

# Database connection utilities
async def get_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)
async def store_financial_data(message_type: str, data: Dict[str, Any], raw_message: str, customer_info: Dict[str, Any]):
    pool = await get_db_pool()
    raw_message_id = None

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                logger.info(f"[INSERT] Raw message: {raw_message}")
                logger.info(f"[INSERT] Customer info: {customer_info}")
                logger.info(f"[INSERT] Data: {data}")
                logger.info(f"[INSERT] Message type: {message_type}")

                await conn.execute("""
                    INSERT INTO customers(id, name, phone_number)
                    VALUES($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name, phone_number = EXCLUDED.phone_number
                """, customer_info["customer_id"], customer_info["customer_name"], str(customer_info["phone_number"]))
                logger.info("[✅] Customer data inserted/updated.")

                if 'transaction_date' in data and data['transaction_date']:
                    if not isinstance(data['transaction_date'], datetime):
                        try:
                            data['transaction_date'] = datetime.strptime(data['transaction_date'], '%Y-%m-%d').date()
                        except Exception:
                            data['transaction_date'] = None

                raw_message_id = await conn.fetchval("""
                    INSERT INTO raw_messages(message_text, message_type, processed)
                    VALUES($1, $2, $3) RETURNING id
                """, raw_message, message_type, True)
                logger.info(f"[✅] Raw message inserted with ID {raw_message_id}")

                phone_number = str(customer_info["phone_number"])

                if message_type == "SALARY_CREDIT":
                    employer = data.get('employer')
                    if not employer or employer == "General Transaction":
                        await conn.execute("""
                            INSERT INTO general_transactions(
                                customer_id, transaction_type, bank_name, account_number, amount, 
                                transaction_date, available_balance
                            )
                            VALUES (
                                (SELECT id FROM customers WHERE phone_number = $1),
                                $2, $3, $4, $5, $6, $7
                            )
                            ON CONFLICT ON CONSTRAINT unique_general_tx DO NOTHING
                        """, phone_number, message_type, data.get('bank_name'), data.get('account_number'),
                             data.get('amount'), data.get('transaction_date'), data.get('available_balance'))
                        logger.info("[✅] Salary message without employer inserted as general transaction.")
                    else:
                        await conn.execute("""
                            INSERT INTO salary_transactions(
                                customer_id, bank_name, account_number, amount, transaction_date, 
                                employer, available_balance
                            )
                            VALUES (
                                (SELECT id FROM customers WHERE phone_number = $1),
                                $2, $3, $4, $5, $6, $7
                            )
                            ON CONFLICT ON CONSTRAINT unique_salary_tx DO NOTHING
                        """, phone_number, data.get('bank_name'), data.get('account_number'), data.get('amount'),
                             data.get('transaction_date'), employer, data.get('available_balance'))
                        logger.info("[✅] Salary transaction inserted.")

                elif message_type == "EMI_PAYMENT":
                    await conn.execute("""
                        INSERT INTO emi_payments(
                            customer_id, bank_name, account_number, amount, payment_date,
                            loan_reference, loan_type, available_balance
                        )
                        VALUES (
                            (SELECT id FROM customers WHERE phone_number = $1),
                            $2, $3, $4, $5, $6, $7, $8
                        )
                        ON CONFLICT ON CONSTRAINT unique_emi_tx DO NOTHING
                    """, phone_number, data.get('bank_name'), data.get('account_number'), data.get('amount'),
                         data.get('transaction_date'), data.get('loan_reference'),
                         data.get('loan_type'), data.get('available_balance'))
                    logger.info("[✅] EMI payment inserted.")

                elif message_type == "CREDIT_CARD_TRANSACTION":
                    await conn.execute("""
                        INSERT INTO credit_card_transactions(
                            customer_id, bank_name, card_number, amount, merchant, transaction_date,
                            authorization_code, available_balance, total_outstanding
                        )
                        VALUES (
                            (SELECT id FROM customers WHERE phone_number = $1),
                            $2, $3, $4, $5, $6, $7, $8, $9
                        )
                        ON CONFLICT ON CONSTRAINT unique_credit_card_tx DO NOTHING
                    """, phone_number, data.get('bank_name'), data.get('account_number'), data.get('amount'),
                         data.get('merchant'), data.get('transaction_date'),
                         data.get('authorization_code'), data.get('available_balance'),
                         data.get('total_outstanding'))
                    logger.info("[✅] Credit card transaction inserted.")

                elif message_type == "SIP_INVESTMENT":
                    await conn.execute("""
                        INSERT INTO sip_investments(
                            customer_id, fund_name, folio_number, amount, investment_date, nav_value
                        )
                        VALUES (
                            (SELECT id FROM customers WHERE phone_number = $1),
                            $2, $3, $4, $5, $6
                        )
                        ON CONFLICT ON CONSTRAINT unique_sip_tx DO NOTHING
                    """, phone_number, data.get('fund_name'), data.get('folio_number'), data.get('amount'),
                         data.get('transaction_date'), data.get('nav_value'))
                    logger.info("[✅] SIP investment inserted.")

                elif message_type in ["CREDIT_TRANSACTION", "DEBIT_TRANSACTION"]:
                    await conn.execute("""
                        INSERT INTO general_transactions(
                            customer_id, transaction_type, bank_name, account_number, amount, 
                            transaction_date, available_balance
                        )
                        VALUES (
                            (SELECT id FROM customers WHERE phone_number = $1),
                            $2, $3, $4, $5, $6, $7
                        )
                        ON CONFLICT ON CONSTRAINT unique_general_tx DO NOTHING
                    """, phone_number, message_type, data.get('bank_name'), data.get('account_number'),
                         data.get('amount'), data.get('transaction_date'), data.get('available_balance'))
                    logger.info("[✅] General transaction inserted.")

                elif message_type == "INSURANCE_PAYMENT":
                    await conn.execute("""
                        INSERT INTO insurance_payments(
                            customer_id, policy_number, insurance_company, insurance_type,
                            amount, transaction_date
                        )
                        VALUES (
                            (SELECT id FROM customers WHERE phone_number = $1),
                            $2, $3, $4, $5, $6
                        )
                        ON CONFLICT ON CONSTRAINT unique_insurance_tx DO NOTHING
                    """, phone_number, data.get('policy_number'), data.get('insurance_company'),
                         data.get('insurance_type'), data.get('amount'), data.get('transaction_date'))
                    logger.info("[✅] Insurance payment inserted.")

    except Exception as e:
        logger.error(f"[❌] Database error: {str(e)}")
        logger.error(f"Problematic data: {data}")
        logger.error(f"Raw Message: {raw_message}")
        logger.error(f"Customer Info: {customer_info}")
    finally:
        await pool.close()

    return raw_message_id




# CSV processing functions
async def process_csv_file(file_path: str, delimiter: str = ",", has_header: bool = True):
    global processing_status

    processing_status["status"] = "processing"
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            csv_reader = csv.reader(file, delimiter=delimiter)

            if has_header:
                next(csv_reader)

            rows = list(csv_reader)
            processing_status.update({"total": len(rows), "processed": 0, "succeeded": 0, "failed": 0})

            for row in rows:
                processing_status["processed"] += 1

                if len(row) >= 4:
                    try:
                        customer_info = {
                            "customer_id": int(row[0]),
                            "customer_name": row[1],
                            "phone_number": row[2]
                        }
                        message = row[3]

                        # Debug: Log customer info for every row
                        logger.info(f"Processing row for customer: {customer_info}")

                        # Use date from row[4] if present, else fallback to today
                        date_str = row[4] if len(row) > 4 else datetime.now().strftime("%Y-%m-%d")

                        result = await process_single_message(date_str, message, customer_info)
                        processing_status["succeeded"] += 1
                    except Exception as e:
                        processing_status["failed"] += 1
                        logger.error(f"Error processing row: {row}")
                        logger.error(f"Exception: {str(e)}")
                else:
                    processing_status["failed"] += 1
                    logger.error(f"Row has insufficient columns (found {len(row)}): {row}")

        processing_status["status"] = "completed"
    except Exception as e:
        processing_status["status"] = "failed"
        logger.error(f"Error processing CSV file: {str(e)}")
        raise




async def process_single_message(date_str: str, message: str, customer_info: Dict[str, Any]):
    if not message.strip():
        raise ValueError("Message is empty or blank.")

    logger.info(f"[LLM] Analyzing message for customer: {customer_info}")

    try:
        llm_result = await analyze_with_llm(message)
    except Exception as e:
        logger.error(f"[❌] LLM analysis failed: {str(e)}")
        raise

    if not llm_result:
        raise ValueError("LLM returned no result or failed to extract information")

    message_type = llm_result["message_type"]
    data = llm_result["extracted_data"]
    important_points = llm_result["important_points"]

    if 'transaction_date' not in data or not data['transaction_date']:
        parsed_date = parse_date(date_str) if date_str else None
        if parsed_date:
            data['transaction_date'] = parsed_date
        else:
            logger.warning("No valid transaction date found or inferred.")

    # Store data to the database
    await store_financial_data(message_type, data, message, customer_info)

    return {
        "message_type": message_type,
        "important_points": important_points,
        "data": data
    }


def extract_json_block(text: str) -> str:
    """Extract the JSON object from a response string, ignoring any formatting like markdown."""
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            return text[start:end+1]
    except Exception as e:
        logger.error(f"Failed to extract JSON block: {e}")
    return text.strip()



async def analyze_with_llm(message: str) -> Optional[Dict[str, Any]]:
    try:
        prompt = f"""
Analyze the following financial SMS and return a clean JSON response only (no markdown or code block formatting):

Message: \"{message}\"

Identify the transaction category from one of the following:
- SALARY_CREDIT (salary deposits)
- EMI_PAYMENT (loan repayments)
- CREDIT_CARD_TRANSACTION (credit card usage)
- SIP_INVESTMENT (mutual fund investments)
- CREDIT_TRANSACTION (other deposits)
- DEBIT_TRANSACTION (other withdrawals)
- INSURANCE_PAYMENT (insurance payments)
- OTHER_FINANCIAL (other financial messages)

Extract fields based on the category such as:
- amount, account_number, transaction_date (must be present), available_balance, bank_name
- employer (for salary), merchant (for card), authorization_code, total_outstanding
- loan_reference, loan_type (for EMI), fund_name, folio_number, nav_value (for SIP)
- policy_number, insurance_company, insurance_type (for insurance)

The transaction_date field is mandatory. Ensure it is extracted in YYYY-MM-DD format or explicitly set it as null.

Format strictly as:
{{
  "message_type": "CATEGORY_NAME",
  "extracted_data": {{
    "field1": "value1",
    "field2": "value2"
  }},
  "important_points": ["point 1", "point 2", "point 3"]
}}

Return only valid JSON. Do not return markdown, explanation, or formatting like ```json.
        """

        response = model.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)

        clean_text = extract_json_block(response_text)

        try:
            result = json.loads(clean_text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            logger.error(f"Problematic response text: {clean_text}")
            return None

        if ("message_type" not in result or 
            "extracted_data" not in result or 
            "important_points" not in result):
            raise ValueError("Missing required fields in LLM response")

        result["extracted_data"] = sanitize_llm_data(result["extracted_data"])

        return result

    except Exception as e:
        logger.error(f"LLM analysis failed: {e}")
        return None






def generate_important_points(message_type: str, data: Dict[str, Any]) -> List[str]:
    """Generate important points based on the extracted data"""
    important_points = []
    
    if "amount" in data and data["amount"]:
        important_points.append(f"Transaction amount: ₹{data['amount']:,.2f}")
    
    if "transaction_date" in data and data["transaction_date"]:
        date_obj = data["transaction_date"]
        if isinstance(date_obj, str):
            important_points.append(f"Date: {date_obj}")
        else:
            important_points.append(f"Date: {date_obj.strftime('%Y-%m-%d')}")
    
    if "bank_name" in data and data["bank_name"]:
        important_points.append(f"Bank: {data['bank_name']}")
    
    if "available_balance" in data and data["available_balance"]:
        important_points.append(f"Available balance: ₹{data['available_balance']:,.2f}")
    
    # Add type-specific points
    if message_type == "SALARY_CREDIT" and "employer" in data and data["employer"]:
        important_points.append(f"Salary from: {data['employer']}")
        
    elif message_type == "EMI_PAYMENT":
        if "loan_reference" in data and data["loan_reference"]:
            important_points.append(f"Loan reference: {data['loan_reference']}")
        if "loan_type" in data and data["loan_type"]:
            important_points.append(f"Loan type: {data['loan_type']}")
            
    elif message_type == "CREDIT_CARD_TRANSACTION":
        if "merchant" in data and data["merchant"]:
            important_points.append(f"Purchase at: {data['merchant']}")
        if "total_outstanding" in data and data["total_outstanding"]:
            important_points.append(f"Total outstanding: ₹{data['total_outstanding']:,.2f}")
            
    elif message_type == "SIP_INVESTMENT":
        if "fund_name" in data and data["fund_name"]:
            important_points.append(f"Fund: {data['fund_name']}")
        if "folio_number" in data and data["folio_number"]:
            important_points.append(f"Folio: {data['folio_number']}")
        if "nav_value" in data and data["nav_value"]:
            important_points.append(f"NAV: {data['nav_value']}")
    
    return important_points

# API endpoints
@app.post("/api/upload-csv", response_model=UploadResponse)
async def upload_csv_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    delimiter: str = Form(","),
    has_header: bool = Form(True)
):
    """Upload and process a CSV file"""
    global processing_status
    
    # Check if processing is already in progress
    if processing_status["status"] == "processing":
        return UploadResponse(
            status="error",
            message="Another file is already being processed. Please wait for completion."
        )
    
    # Reset status
    processing_status = {
        "total": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "status": "idle"
    }
    
    try:
        # Save the uploaded file
        file_path = f"uploads/{file.filename}"
        os.makedirs("uploads", exist_ok=True)
        
        # Write the file
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Start processing in the background
        background_tasks.add_task(process_csv_file, file_path, delimiter, has_header)
        
        return UploadResponse(
            status="success",
            message=f"File {file.filename} uploaded successfully. Processing started in the background."
        )
        
    except Exception as e:
        logger.error(f"Error uploading CSV file: {str(e)}")
        return UploadResponse(
            status="error",
            message=f"Error uploading file: {str(e)}"
        )

@app.post("/api/process-csv", response_model=UploadResponse)
async def process_existing_csv(background_tasks: BackgroundTasks, request: CSVUploadRequest):
    """Process an existing CSV file on the server"""
    global processing_status
    
    # Check if processing is already in progress
    if processing_status["status"] == "processing":
        return UploadResponse(
            status="error",
            message="Another file is already being processed. Please wait for completion."
        )
    
    # Reset status
    processing_status = {
        "total": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "status": "idle"
    }
    
    try:
        # Verify file exists
        if not os.path.exists(request.file_path):
            return UploadResponse(
                status="error",
                message=f"File {request.file_path} not found."
            )
        
        # Start processing in the background
        background_tasks.add_task(process_csv_file, request.file_path, request.delimiter, request.has_header)
        
        return UploadResponse(
            status="success",
            message=f"Processing of file {request.file_path} started in the background."
        )
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        return UploadResponse(
            status="error",
            message=f"Error processing file: {str(e)}"
        )

@app.get("/api/process-status", response_model=ProcessingStatus)
async def get_processing_status():
    """Get the current status of CSV processing"""
    global processing_status
    return ProcessingStatus(**processing_status)

@app.post("/api/analyze-message", response_model=MessageResponse)
async def analyze_single_message(message: str, date_str: Optional[str] = None):
    """Analyze a single SMS message"""
    try:
        if not message.strip():
            raise HTTPException(status_code=400, detail="Message cannot be empty")
        
        result = await process_single_message(date_str or datetime.now().strftime("%Y-%m-%d"), message)
        return MessageResponse(
            message_type=result["message_type"],
            important_points=result["important_points"],
            data=result["data"]
        )
        
    except Exception as e:
        logger.error(f"Error analyzing message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error analyzing message: {str(e)}")

@app.post("/api/process-csv-file", response_model=UploadResponse)
async def process_csv_file_path(file_path: str = Form(...)):
    """Process a specific CSV file on the server"""
    global processing_status
    
    # Check if processing is already in progress
    if processing_status["status"] == "processing":
        return UploadResponse(
            status="error",
            message="Another file is already being processed. Please wait for completion."
        )
    
    try:
        # Verify file exists
        if not os.path.exists(file_path):
            return UploadResponse(
                status="error",
                message=f"File {file_path} not found."
            )
        
        # Start processing in the background
        background_tasks = BackgroundTasks()
        background_tasks.add_task(process_csv_file, file_path, ",", True)
        
        return UploadResponse(
            status="success",
            message=f"Processing of file {file_path} started in the background."
        )
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        return UploadResponse(
            status="error",
            message=f"Error processing file: {str(e)}"
        )

# Database initialization
async def init_db():
    """Initialize database tables if they don't exist"""
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            # Check if tables exist, if not create them
            tables_exist = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'raw_messages'
                )
            """)
            
            if not tables_exist:
                # Create the tables
                with open('db_init.sql', 'r') as f:
                    sql = f.read()
                    await conn.execute(sql)
                    logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
    finally:
        await pool.close()

# Create SQL file for database initialization
def create_db_init_sql():
    """Create SQL file for database initialization"""
    sql_content = """
-- customers table
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    phone_number VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- salary_transactions table
CREATE TABLE IF NOT EXISTS salary_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    transaction_id VARCHAR(100),
    employer VARCHAR(200),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_salary_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- emi_payments table
CREATE TABLE IF NOT EXISTS emi_payments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    payment_date DATE,
    loan_reference VARCHAR(100),
    loan_type VARCHAR(100),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_emi_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- credit_card_transactions table
CREATE TABLE IF NOT EXISTS credit_card_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    card_number VARCHAR(50),
    amount DECIMAL(12, 2),
    merchant VARCHAR(200),
    transaction_date DATE,
    authorization_code VARCHAR(50),
    available_balance DECIMAL(12, 2),
    total_outstanding DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cc_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- sip_investments table
CREATE TABLE IF NOT EXISTS sip_investments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    fund_name VARCHAR(200),
    folio_number VARCHAR(100),
    amount DECIMAL(12, 2),
    investment_date DATE,
    nav_value DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sip_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- general_transactions table
CREATE TABLE IF NOT EXISTS general_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    transaction_type VARCHAR(50),
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_general_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- insurance_payments table
CREATE TABLE IF NOT EXISTS insurance_payments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    policy_number VARCHAR(100),
    insurance_company VARCHAR(100),
    insurance_type VARCHAR(100),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_insurance_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS raw_messages (
    id SERIAL PRIMARY KEY,
    message_text TEXT,
    message_type VARCHAR(100),
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


"""
    
    # Write the SQL to a file
    with open('db_init.sql', 'w') as f:
        f.write(sql_content)
    
    logger.info("Database initialization SQL file created")

# Dashboard data endpoints
@app.get("/api/dashboard/summary")
async def get_dashboard_summary():
    """Get summary statistics for the dashboard"""
    pool = await get_db_pool()
    summary = {}
    
    try:
        async with pool.acquire() as conn:
            # Get salary summary
            salary_data = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    MAX(amount) as highest_salary,
                    MIN(transaction_date) as earliest_date,
                    MAX(transaction_date) as latest_date
                FROM salary_transactions
            """)
            
            if salary_data:
                summary["salary"] = {
                    "transaction_count": salary_data["transaction_count"],
                    "total_amount": float(salary_data["total_amount"]) if salary_data["total_amount"] else 0,
                    "highest_salary": float(salary_data["highest_salary"]) if salary_data["highest_salary"] else 0,
                    "date_range": {
                        "from": salary_data["earliest_date"].isoformat() if salary_data["earliest_date"] else None,
                        "to": salary_data["latest_date"].isoformat() if salary_data["latest_date"] else None
                    }
                }
            
            # Get EMI summary
            emi_data = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    COUNT(DISTINCT loan_reference) as unique_loans
                FROM emi_payments
            """)
            
            if emi_data:
                summary["emi"] = {
                    "transaction_count": emi_data["transaction_count"],
                    "total_amount": float(emi_data["total_amount"]) if emi_data["total_amount"] else 0,
                    "unique_loans": emi_data["unique_loans"]
                }
            
            # Get credit card summary
            cc_data = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_spent,
                    MAX(total_outstanding) as highest_outstanding
                FROM credit_card_transactions
            """)
            
            if cc_data:
                summary["credit_card"] = {
                    "transaction_count": cc_data["transaction_count"],
                    "total_spent": float(cc_data["total_spent"]) if cc_data["total_spent"] else 0,
                    "highest_outstanding": float(cc_data["highest_outstanding"]) if cc_data["highest_outstanding"] else 0
                }
            
            # Get SIP summary
            sip_data = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_invested,
                    COUNT(DISTINCT folio_number) as unique_folios
                FROM sip_investments
            """)
            
            if sip_data:
                summary["sip"] = {
                    "transaction_count": sip_data["transaction_count"],
                    "total_invested": float(sip_data["total_invested"]) if sip_data["total_invested"] else 0,
                    "unique_folios": sip_data["unique_folios"]
                }
            
            # Get general transactions summary
            general_data = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as transaction_count,
                    SUM(CASE WHEN transaction_type = 'CREDIT_TRANSACTION' THEN amount ELSE 0 END) as total_credits,
                    SUM(CASE WHEN transaction_type = 'DEBIT_TRANSACTION' THEN amount ELSE 0 END) as total_debits
                FROM general_transactions
            """)
            
            if general_data:
                summary["general"] = {
                    "transaction_count": general_data["transaction_count"],
                    "total_credits": float(general_data["total_credits"]) if general_data["total_credits"] else 0,
                    "total_debits": float(general_data["total_debits"]) if general_data["total_debits"] else 0
                }
                
            # Get processing error count
            error_count = await conn.fetchval("SELECT COUNT(*) FROM error_log")
            summary["errors"] = {
                "count": error_count
            }
            
    except Exception as e:
        logger.error(f"Error retrieving dashboard data: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dashboard data")
    finally:
        await pool.close()
    
    return summary

@app.get("/api/messages/recent")
async def get_recent_messages(limit: int = 10):
    """Get the most recent processed messages"""
    pool = await get_db_pool()
    messages = []
    
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, message_text, message_type, created_at
                FROM raw_messages
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
            
            for row in rows:
                messages.append({
                    "id": row["id"],
                    "message": row["message_text"],
                    "type": row["message_type"],
                    "processed_at": row["created_at"].isoformat()
                })
                
    except Exception as e:
        logger.error(f"Error retrieving recent messages: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving recent messages")
    finally:
        await pool.close()
    
    return messages

@app.get("/api/errors/recent")
async def get_recent_errors(limit: int = 10):
    """Get the most recent processing errors"""
    pool = await get_db_pool()
    errors = []
    
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, message_text, error_message, created_at
                FROM error_log
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
            
            for row in rows:
                errors.append({
                    "id": row["id"],
                    "message": row["message_text"],
                    "error": row["error_message"],
                    "created_at": row["created_at"].isoformat()
                })
                
    except Exception as e:
        logger.error(f"Error retrieving recent errors: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving recent errors")
    finally:
        await pool.close()
    
    return errors

# Application startup events
@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup"""
    # Create the database initialization SQL file
    create_db_init_sql()
    
    # Initialize the database
    await init_db()
    
    # Create uploads directory if it doesn't exist
    os.makedirs("uploads", exist_ok=True)
    
    logger.info("Application started successfully")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("csv_processor:app", host="0.0.0.0", port=8000, reload=True)