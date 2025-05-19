from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
from datetime import datetime
import asyncpg
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure Database
DATABASE_URL = os.getenv("DATABASE_URL")

# Configure Gemini API
try:
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-thinking-exp-01-21",
        temperature=0.7,
        max_tokens=None,
        google_api_key=os.getenv("API_KEY")
    )
except Exception as e:
    print(f"Error initializing Gemini model: {e}")

# Define Pydantic models
class MessageRequest(BaseModel):
    message: str

class MessageResponse(BaseModel):
    message_type: str
    important_points: List[str]
    data: Optional[Dict[str, Any]] = None

# Database connection pool
async def get_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# Initialize database tables
async def init_db():
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
                # Create the tables using the SQL we defined earlier
                with open('db_init.sql', 'r') as f:
                    sql = f.read()
                    await conn.execute(sql)
                    print("Database tables created successfully")
    except Exception as e:
        print(f"Database initialization error: {e}")
    finally:
        await pool.close()

# Helper function to clean numeric strings before conversion
def clean_numeric_string(value_str):
    if not value_str:
        return None
        
    # Remove commas and any trailing periods
    cleaned = value_str.replace(',', '')
    
    # Remove trailing period if present
    if cleaned.endswith('.'):
        cleaned = cleaned[:-1]
        
    # Handle cases where there might be multiple dots
    if cleaned.count('.') > 1:
        # Keep only the first decimal point
        first_dot = cleaned.find('.')
        cleaned = cleaned[:first_dot+1] + cleaned[first_dot+1:].replace('.', '')
    
    return cleaned

# Classify message type more specifically
def classify_message_type(message: str) -> str:
    message = message.lower()
    
    # Check for salary-related messages
    if (("salary" in message or "deposited" in message or "credited" in message) and 
        any(bank in message.lower() for bank in ["hdfc", "sbi", "icici", "axis"])):
        return "SALARY_CREDIT"
    
    # Check for EMI-related messages
    if (("loan" in message or "emi" in message) and 
        ("debited" in message or "deducted" in message)):
        return "EMI_PAYMENT"
    
    # Check for credit card transactions
    if "credit card" in message or "creditcard" in message or "card member" in message:
        return "CREDIT_CARD_TRANSACTION"
    
    # Check for SIP investments - Updated pattern
    if ("sip" in message and 
        ("processed" in message or 
         "deducted" in message or 
         "under folio" in message or 
         "has been processed" in message)):
        return "SIP_INVESTMENT"
    
    # Default to generic type
    if "credited" in message or "deposited" in message:
        return "CREDIT_TRANSACTION"
    elif "debited" in message or "deducted" in message:
        return "DEBIT_TRANSACTION"
    else:
        return "OTHER_FINANCIAL"

# Extract data based on message type
def extract_financial_data(message_type: str, message: str) -> Dict[str, Any]:
    data = {}
    
    # Common extraction patterns
    amount_pattern = r'(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)'
    account_pattern = r'(?:A/c|Ac\sNo\.|card ending|account)\s*([A-Z0-9]+\d{4})'
    date_pattern = r'(\d{2}[-/]\d{2}[-/]\d{2,4})'
    balance_pattern = r'(?:Avl bal|available balance|net available balance)[^0-9]*(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)'
    
    # Extract common fields
    amount_match = re.search(amount_pattern, message)
    if amount_match:
        amount_str = clean_numeric_string(amount_match.group(1))
        if amount_str:
            try:
                data['amount'] = float(amount_str)
            except ValueError:
                print(f"Failed to convert amount: {amount_str}")
    
    account_match = re.search(account_pattern, message, re.IGNORECASE)
    if account_match:
        data['account_number'] = account_match.group(1)
    
    date_match = re.search(date_pattern, message)
    if date_match:
        date_str = date_match.group(1)
        # Normalize date format
        day, month, year = date_str.replace('/', '-').split('-')
        if len(year) == 2:
            year = '20' + year
        data['transaction_date'] = f"{year}-{month}-{day}"
    
    balance_match = re.search(balance_pattern, message, re.IGNORECASE)
    if balance_match:
        balance_str = clean_numeric_string(balance_match.group(1))
        if balance_str:
            try:
                data['available_balance'] = float(balance_str)
            except ValueError:
                print(f"Failed to convert balance: {balance_str}")
    
    # Message type specific extraction
    if message_type == "SALARY_CREDIT":
        # Extract employer
        employer_match = re.search(r'- ([A-Za-z\s]+) -', message)
        if employer_match:
            data['employer'] = employer_match.group(1).strip()
        
        # Extract bank name
        for bank in ["HDFC Bank", "SBI", "ICICI Bank", "Axis Bank"]:
            if bank.lower() in message.lower():
                data['bank_name'] = bank
                break
    
    elif message_type == "EMI_PAYMENT":
        # Extract loan reference
        loan_ref_match = re.search(r'([A-Z0-9]+\d{6,})', message)
        if loan_ref_match:
            data['loan_reference'] = loan_ref_match.group(1)
        
        # Extract loan type if available
        loan_type_match = re.search(r'Loan\s+([A-Za-z]+)', message, re.IGNORECASE)
        if loan_type_match:
            data['loan_type'] = loan_type_match.group(1)
        else:
            data['loan_type'] = "Personal Loan"  # Default
        
        # Extract bank name
        for bank in ["HDFC Bank", "SBI", "ICICI Bank", "Axis Bank", "NNSB"]:
            if bank.lower() in message.lower():
                data['bank_name'] = bank
                break
    
    elif message_type == "CREDIT_CARD_TRANSACTION":
        # Extract merchant
        merchant_match = re.search(r'at\s+([A-Za-z\s]+)\s+on', message, re.IGNORECASE)
        if merchant_match:
            data['merchant'] = merchant_match.group(1).strip()
        
        # Extract authorization code
        auth_code_match = re.search(r'Authorization code:- (\w+)', message)
        if auth_code_match:
            data['authorization_code'] = auth_code_match.group(1)
        
        # Extract total outstanding
        outstanding_match = re.search(r'total outstanding is\s+(?:Rs\.?|INR|₹)\s*([\d,]+\.?\d*)', message, re.IGNORECASE)
        if outstanding_match:
            outstanding_str = clean_numeric_string(outstanding_match.group(1))
            if outstanding_str:
                try:
                    data['total_outstanding'] = float(outstanding_str)
                except ValueError:
                    print(f"Failed to convert outstanding: {outstanding_str}")
        
        # Extract bank name
        for bank in ["HDFC Bank", "SBI", "ICICI Bank", "Axis Bank"]:
            if bank.lower() in message.lower():
                data['bank_name'] = bank
                break
    
    elif message_type == "SIP_INVESTMENT":
        # Use the specialized SIP extraction function
        sip_data = extract_sip_data(message)
        data.update(sip_data)
    
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
                print(f"Failed to convert SIP amount: {amount_str}")
    
    # Extract date - handle both formats: 11/04/2025 and 11-04-2025
    date_pattern = r'(\d{2}[/-]\d{2}[/-]\d{4})'
    date_match = re.search(date_pattern, message)
    if date_match:
        date_str = date_match.group(1)
        # Normalize format
        if '/' in date_str:
            day, month, year = date_str.split('/')
        else:
            day, month, year = date_str.split('-')
        data['transaction_date'] = f"{year}-{month}-{day}"
    
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
                print(f"Failed to convert NAV: {nav_str}")
    
    return data

# Sanitize numeric values in LLM extracted data
def sanitize_llm_data(data):
    if not data or not isinstance(data, dict):
        return {}
    
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, str) and key in ['amount', 'available_balance', 'total_outstanding', 'nav_value']:
            # Clean and convert numeric strings
            try:
                sanitized[key] = float(clean_numeric_string(value))
            except (ValueError, TypeError):
                print(f"Failed to convert {key}: {value}")
                continue
        else:
            sanitized[key] = value
    
    return sanitized

# Store data in appropriate database table
async def store_financial_data(message_type: str, data: Dict[str, Any], raw_message: str):
    pool = await get_db_pool()
    raw_message_id = None
    
    try:
        async with pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # First store the raw message
                raw_message_id = await conn.fetchval("""
                    INSERT INTO raw_messages(message_text, message_type, processed)
                    VALUES($1, $2, $3) RETURNING id
                """, raw_message, message_type, True)
                
                # Now store in the appropriate table based on message type
                if message_type == "SALARY_CREDIT":
                    await conn.execute("""
                        INSERT INTO salary_transactions(
                            bank_name, account_number, amount, transaction_date, 
                            employer, available_balance
                        ) VALUES($1, $2, $3, $4, $5, $6)
                    """, 
                    data.get('bank_name'), data.get('account_number'), data.get('amount'),
                    data.get('transaction_date'), data.get('employer'), data.get('available_balance'))
                    
                elif message_type == "EMI_PAYMENT":
                    await conn.execute("""
                        INSERT INTO emi_payments(
                            bank_name, account_number, amount, payment_date,
                            loan_reference, loan_type, available_balance
                        ) VALUES($1, $2, $3, $4, $5, $6, $7)
                    """,
                    data.get('bank_name'), data.get('account_number'), data.get('amount'),
                    data.get('transaction_date'), data.get('loan_reference'),
                    data.get('loan_type'), data.get('available_balance'))
                    
                elif message_type == "CREDIT_CARD_TRANSACTION":
                    await conn.execute("""
                        INSERT INTO credit_card_transactions(
                            bank_name, card_number, amount, merchant, transaction_date,
                            authorization_code, available_balance, total_outstanding
                        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    data.get('bank_name'), data.get('account_number'), data.get('amount'),
                    data.get('merchant'), data.get('transaction_date'),
                    data.get('authorization_code'), data.get('available_balance'),
                    data.get('total_outstanding'))
                    
                elif message_type == "SIP_INVESTMENT":
                    await conn.execute("""
                        INSERT INTO sip_investments(
                            fund_name, folio_number, amount, investment_date, nav_value
                        ) VALUES($1, $2, $3, $4, $5)
                    """,
                    data.get('fund_name'), data.get('folio_number'), data.get('amount'),
                    data.get('transaction_date'), data.get('nav_value'))
    
    except Exception as e:
        print(f"Database error: {e}")
        # Log the error and the data that caused it
        print(f"Problematic data: {data}")
        # You might want to store the failed message in a separate table or log
    
    finally:
        await pool.close()
    
    return raw_message_id

# Improved LLM-based analysis function
async def analyze_with_llm(message: str) -> dict:
    try:
        # Specialized prompt for financial messages
        prompt = f"""
        Analyze this financial SMS message and extract all available data in JSON format:

        Message: {message}

        Determine the specific type from these categories:
        - SALARY_CREDIT (salary deposits)
        - EMI_PAYMENT (loan repayments)
        - CREDIT_CARD_TRANSACTION (credit card usage)
        - SIP_INVESTMENT (mutual fund investments)
        - CREDIT_TRANSACTION (other deposits)
        - DEBIT_TRANSACTION (other withdrawals)
        - OTHER_FINANCIAL (other financial messages)

        Then extract all relevant fields based on the message type, such as:
        - amount: numeric value without currency symbol and commas
        - account_number: masked account number
        - transaction_date: in YYYY-MM-DD format
        - available_balance: available balance amount
        - bank_name: name of the bank
        - employer: name of employer for salary credits
        - merchant: name of merchant for card transactions
        - authorization_code: for card transactions
        - total_outstanding: for credit cards
        - loan_reference: for EMI payments
        - loan_type: type of loan
        - fund_name: name of mutual fund for SIP
        - folio_number: investment folio number
        - nav_value: NAV value for SIP investments

        Important: For all numeric values (amount, available_balance, nav_value, etc.), 
        provide clean numeric strings without commas and trailing periods.

        Return a JSON object with:
        1. message_type: the specific type
        2. extracted_data: an object containing all extracted fields
        3. important_points: an array of 3-5 key statements about the transaction

        Response format:
        {
          "message_type": "SPECIFIC_TYPE",
          "extracted_data": {
            "field1": "value1",
            "field2": "value2"
          },
          "important_points": ["key point 1", "key point 2", "key point 3"]
        }
        """
        
        response = model.invoke(prompt)
        response_text = response.content
        
        # Handle case where response might contain markdown code blocks
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
            
        try:
            result = json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Problematic response text: {response_text}")
            return None
        
        # Validate expected fields
        if ("message_type" not in result or 
            "extracted_data" not in result or 
            "important_points" not in result):
            raise ValueError("Missing required fields in LLM response")
        
        # Sanitize the extracted data
        result["extracted_data"] = sanitize_llm_data(result["extracted_data"])
            
        return result
    
    except Exception as e:
        print(f"LLM analysis failed: {e}")
        # Return None to trigger fallback
        return None

# Main endpoint for analyzing messages
@app.post("/analyze", response_model=MessageResponse)
async def analyze_message(request: MessageRequest):
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    message = request.message
    
    # Try LLM analysis first 
    llm_result = await analyze_with_llm(message)
    
    if llm_result and "extracted_data" in llm_result and llm_result["extracted_data"]:
        # LLM analysis successful
        message_type = llm_result["message_type"]
        extracted_data = llm_result["extracted_data"]
        important_points = llm_result["important_points"]
        
        # Additional validation for SIP investments
        if message_type == "SIP_INVESTMENT" and ("nav_value" not in extracted_data or extracted_data["nav_value"] is None):
            # Try to extract the NAV value with the specialized function
            sip_data = extract_sip_data(message)
            if "nav_value" in sip_data:
                extracted_data["nav_value"] = sip_data["nav_value"]
        
        # Store data in database
        try:
            await store_financial_data(message_type, extracted_data, message)
        except Exception as e:
            print(f"Error storing data: {e}")
            # Continue to return the response even if storage fails
        
        return MessageResponse(
            message_type=message_type,
            important_points=important_points,
            data=extracted_data
        )
    else:
        # Fallback to rule-based analysis
        message_type = classify_message_type(message)
        extracted_data = extract_financial_data(message_type, message)
        
        # Generate important points based on extracted data
        important_points = []
        if "amount" in extracted_data:
            important_points.append(f"Transaction amount: ₹{extracted_data['amount']:,.2f}")
        if "transaction_date" in extracted_data:
            important_points.append(f"Date: {extracted_data['transaction_date']}")
        if "bank_name" in extracted_data:
            important_points.append(f"Bank: {extracted_data['bank_name']}")
        if "available_balance" in extracted_data:
            important_points.append(f"Available balance: ₹{extracted_data['available_balance']:,.2f}")
        
        # Add type-specific points
        if message_type == "SALARY_CREDIT" and "employer" in extracted_data:
            important_points.append(f"Salary from: {extracted_data['employer']}")
        elif message_type == "CREDIT_CARD_TRANSACTION" and "merchant" in extracted_data:
            important_points.append(f"Purchase at: {extracted_data['merchant']}")
        elif message_type == "SIP_INVESTMENT" and "fund_name" in extracted_data:
            important_points.append(f"Fund: {extracted_data['fund_name']}")
            if "nav_value" in extracted_data:
                important_points.append(f"NAV: {extracted_data['nav_value']}")
        
        # Try to store the data in the database
        try:
            await store_financial_data(message_type, extracted_data, message)
        except Exception as e:
            print(f"Error storing data: {e}")
            # Continue to return the response even if storage fails
        
        return MessageResponse(
            message_type=message_type,
            important_points=important_points,
            data=extracted_data
        )

# Test your SIP message extraction directly
@app.get("/test-sip")
def test_sip_extraction():
    # Your SIP example
    message = "Greetings, Your SIP of 11/04/2025 for Rs.2499.88 under Folio XXXXXXX0016 in Mirae Asset Midcap Fund-Regular has been processed for NAV of 30.441.subject to realisation.Mirae Asset MF"
    
    # Extract data
    data = extract_sip_data(message)
    
    return {
        "message": message,
        "extracted_data": data,
        "nav_value_test": float(clean_numeric_string("30.441."))
    }

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "ok"}

# Dashboard data endpoints
@app.get("/dashboard/summary")
async def get_dashboard_summary():
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
    except Exception as e:
        print(f"Error retrieving dashboard data: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dashboard data")
    finally:
        await pool.close()
    
    return summary

# Create the SQL file for database initialization
@app.on_event("startup")
async def create_db_init_sql():
    # SQL content for creating tables
    sql_content = """
-- Create tables if they don't exist

-- salary_transactions table for salary deposits
CREATE TABLE IF NOT EXISTS salary_transactions (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    transaction_id VARCHAR(100),
    employer VARCHAR(200),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- emi_payments table for loan EMIs
CREATE TABLE IF NOT EXISTS emi_payments (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    payment_date DATE,
    loan_reference VARCHAR(100),
    loan_type VARCHAR(100),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- credit_card_transactions table for credit card expenses
CREATE TABLE IF NOT EXISTS credit_card_transactions (
    id SERIAL PRIMARY KEY,
    bank_name VARCHAR(100),
    card_number VARCHAR(50),
    amount DECIMAL(12, 2),
    merchant VARCHAR(200),
    transaction_date TIMESTAMP,
    authorization_code VARCHAR(50),
    available_balance DECIMAL(12, 2),
    total_outstanding DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- sip_investments table for SIP deductions
CREATE TABLE IF NOT EXISTS sip_investments (
    id SERIAL PRIMARY KEY,
    fund_name VARCHAR(200),
    folio_number VARCHAR(100),
    amount DECIMAL(12, 2),
    investment_date DATE,
    nav_value DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- raw_messages table to store original messages
CREATE TABLE IF NOT EXISTS raw_messages (
    id SERIAL PRIMARY KEY,
    message_text TEXT,
    message_type VARCHAR(100),
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- error_log table to track processing errors
CREATE TABLE IF NOT EXISTS error_log (
    id SERIAL PRIMARY KEY,
    message_text TEXT,
    error_message TEXT,
    error_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
    
    # Write the SQL to a file
    with open('db_init.sql', 'w') as f:
        f.write(sql_content)
    
    # Initialize the database
    await init_db()

# Run with: uvicorn main:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)