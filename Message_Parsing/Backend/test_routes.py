from typing import List
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],  # You can specify just ["GET", "POST"] if needed
    allow_headers=["*"],
)

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

# Expanded fallback functions
def detect_message_type_fallback(message: str) -> str:
    message = message.lower()
    
    # Financial transaction patterns
    if any(term in message for term in ["credited", "debited", "transaction", "account", "balance"]):
        if "credited" in message or "received" in message:
            return "Financial Credit Alert"
        elif "debited" in message or "spent" in message or "withdrawn" in message:
            return "Financial Debit Alert"
        elif "balance" in message:
            return "Balance Update"
        else:
            return "Financial Notification"
    
    # Communication categories
    elif "meeting" in message or "schedule" in message:
        return "Meeting Invitation / Schedule"
    elif "reminder" in message:
        return "Reminder"
    elif "congratulations" in message or "well done" in message:
        return "Appreciation"
    elif "alert" in message or "issue" in message or "error" in message:
        return "Alert / Issue"
    elif "update" in message or "news" in message:
        return "Update"
    elif "deadline" in message or "due" in message:
        return "Deadline Reminder"
    elif any(term in message for term in ["otp", "verification code", "confirm"]):
        return "Authentication Message"
    elif any(term in message for term in ["offer", "discount", "deal", "sale"]):
        return "Promotional Message"
    else:
        return "General Message"

def extract_important_points_fallback(message: str) -> List[str]:
    import re
    # Simple sentence splitter based on punctuation
    sentences = re.split(r'[.!?]\s+', message.strip())
    
    # Financial transaction keywords
    financial_keywords = ['rs', 'inr', '$', '₹', 'usd', 'amount', 'account', 'balance', 'credit', 
                         'debit', 'transaction', 'transferred', 'received', 'paid', 'avl bal']
    
    # Standard keywords
    standard_keywords = ['on', 'by', 'at', 'before', 'please', 'note', 'due', 'important']
    
    # Combine all keywords
    all_keywords = financial_keywords + standard_keywords
    
    # Filter key points
    points = [
        s.strip() for s in sentences
        if len(s.split()) > 2 and (
            any(keyword in s.lower() for keyword in all_keywords) or
            any(s.lower().count(digit) >= 4 for digit in "0123456789")  # Messages with numbers (like transaction IDs)
        )
    ]
    
    # If no points found, use the most important-looking sentences
    if not points and sentences:
        # Sort by potential importance (presence of numbers, keywords, sentence length)
        sentences = [s for s in sentences if len(s) > 10]
        sentences.sort(key=lambda x: (
            sum(c.isdigit() for c in x) * 2 +  # Prioritize sentences with numbers
            sum(kw in x.lower() for kw in all_keywords) * 3 +  # Keywords importance
            (0 if len(x.split()) > 15 else 5)  # Shorter sentences get priority
        ), reverse=True)
        points = sentences[:3]
    
    return points[:5] if points else ["No important points detected"]

# LLM-based analysis function - Fixed to handle LangChain correctly
async def analyze_with_llm(message: str) -> dict:
    try:
        # prompt = f"""
        # Analyze the following message and respond in JSON format with these fields:
        # 1. message_type: Categorize the message into the most specific appropriate type.
        #    Some possible types include (but are not limited to):
        #    - Financial Credit Alert (for money received/credited notifications)
        #    - Financial Debit Alert (for money spent/debited notifications)
        #    - Balance Update (account balance information)
        #    - Transaction Confirmation
        #    - Meeting Invitation / Schedule
        #    - Reminder
        #    - Appreciation/Congratulation
        #    - Alert/Issue/Warning
        #    - Information Update
        #    - Deadline Reminder
        #    - Authentication Message (OTP, verification codes)
        #    - Promotional Message (offers, marketing)
        #    - Travel Update (flight, train status)
        #    - Delivery Status
        #    - Bill/Payment Reminder
        #    - Emergency Alert
        #    - General Message
        
        # 2. important_points: Extract 3-5 key points from the message.
        #    For financial messages, extract: amounts, account details, dates, transaction IDs.
        #    For general messages: deadlines, action items, key information.
        #    Convert to simple, direct statements.
        
        # Message: {message}
        
        # Respond only with valid JSON like:
        # {{
        #   "message_type": "specific category",
        #   "important_points": ["key point 1", "key point 2", "key point 3"]
        # }}
        # """

        prompt = f"""
    Analyze the following message and extract all available financial data.
    
    1. message_type: Categorize the message into the most specific appropriate type.
           Some possible types include (but are not limited to):
           - Financial Credit Alert (for money received/credited notifications)
           - Financial Debit Alert (for money spent/debited notifications)
           - Balance Update (account balance information)
           - Transaction Confirmation
           - Meeting Invitation / Schedule
           - Reminder
           - Appreciation/Congratulation
           - Alert/Issue/Warning
           - Information Update
           - Deadline Reminder
           - Authentication Message (OTP, verification codes)
           - Promotional Message (offers, marketing)
           - Travel Update (flight, train status)
           - Delivery Status
           - Bill/Payment Reminder
           - Emergency Alert
           - General Message
    
    Then extract important_points focusing on these key fields if present in the message otherwise important_points: Extract 3-5 key points from the message.
           For financial messages, extract: amounts, account details, dates, transaction IDs.
           For general messages: deadlines, action items, key information.
           Convert to simple, direct statements
    - user_id, device_id, year_month
    - total_inflow, total_expense
    - bank_accounts: balance, monthly_credit, monthly_debit, bank, account, last_txn_date
    - loan_dues: emi_due, emi_date, loan_type, bank, account, emi_paid, status
    - investment: type, amount, folio_no, last_txn_date
    - credit_card_accounts: bank, account, available_limit, due_amount, due_date, total_credit_limit
    - wallet: balance, wallet_name, last_txn_date, monthly_credit, monthly_debit
    - utility: utility_name, consumer_id, paid_amount, due_date, due_amount, utility_type
    - insurance: insurance_name, insurance_no, insurance_type, paid_amount, due_date, due_amount
    - top_spend_areas: category, amount, top spends details
    - monthly_transactions: relevant transaction details


     the extractacted points should have proper name and its value 
    
    Message: {message}
    
    Respond only with valid JSON like:
    {{
        "message_type": "specific category",
        "important_points": ["key point 1 with field_name: value", "key point 2 with field_name: value", "key point 3 with field_name: value"]
    }}
"""
        
        # Fixed: LangChain invoke doesn't need await
        response = model.invoke(prompt)
        
        # Extract response text - Fixed to work with LangChain response format
        response_text = response.content
        
        # Handle case where response might contain markdown code blocks
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
            
        result = json.loads(response_text)
        
        # Validate expected fields
        if "message_type" not in result or "important_points" not in result:
            raise ValueError("Missing required fields in LLM response")
            
        return result
    
    except Exception as e:
        print(f"LLM analysis failed: {e}")
        # Return None to trigger fallback
        return None

@app.post("/analyze", response_model=MessageResponse)
async def analyze_message(request: MessageRequest):
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    # Try LLM analysis first
    llm_result = await analyze_with_llm(request.message)
    
    if llm_result:
        # LLM analysis successful
        return MessageResponse(
            message_type=llm_result["message_type"],
            important_points=llm_result["important_points"]
        )
    else:
        # Fallback to rule-based analysis
        message_type = detect_message_type_fallback(request.message)
        important_points = extract_important_points_fallback(request.message)
        
        return MessageResponse(
            message_type=message_type,
            important_points=important_points
        )

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "ok"}

# Optional: Add endpoint to get all possible message types
@app.get("/message-types")
def get_message_types():
    return {
        "message_types": [
            "Financial Credit Alert",
            "Financial Debit Alert",
            "Balance Update",
            "Transaction Confirmation",
            "Meeting Invitation / Schedule",
            "Reminder",
            "Appreciation/Congratulation",
            "Alert/Issue/Warning",
            "Information Update",
            "Deadline Reminder",
            "Authentication Message",
            "Promotional Message",
            "Travel Update",
            "Delivery Status",
            "Bill/Payment Reminder",
            "Emergency Alert",
            "General Message"
        ]
    }