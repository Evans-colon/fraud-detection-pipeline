import os


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW = "raw-transactions"
KAFKA_TOPIC_ENRICHED_TRANSACTION = "enriched-transactions"
KAFKA_TOPIC_ALERTS = "fraud-alerts"
KAFKA_TOPIC_METRICS = "model-metrics"
KAFKA_CONSUMER_GROUP = "fraud-pipeline"

#feature engine
WINDOW_5M_SECONDS = 300
WINDOW_1H_SECONDS = 3600
WINDOW_24H_SECONDS = 86400


#scoring
MODEL_PATH = os.getenv("MODEL_PATH", "models/model.onnx")
SCORE_THRESHOLD_BLOCK = 0.85
SCORE_THRESHOLD_FLAG = 0.5


#CBN regulation thresholds (hard rules and not adjustable)
CBN_SINGLE_TXN_REPORT_NGN = 5_000_000
CBN_DAILY_CUMULATIVE_NGN = 10_000_000
CBN_CASH_WITHDRAW_LIMIT_NGN = 500_000


#Transaction generator
GENERATOR_TPS = float(os.getenv("GENERATOR_TPS", "10"))
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.02"))


#Dashboard
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = os.getenv("DASHBOARD_PORT", "8000")


#Storage
ALERT_DB_PATH = os.getenv("ALERT_DB_PATH", "data/alerts.db")



#Nigerian bank codes (subset for simulation)
BANK_CODES = {
    "000004": "United Bank for Africa",
    "000007": "Fidelity Bank",
    "000013": "GTBank",
    "000014": "Access Bank",
    "000015": "Zenith Bank",
    "000016": "First Bank",
    "000017": "Wema Bank",
    "000018": "Union Bank",
    "090267": "Kuda MFB",
    "100003": "Paystack",
    "100004": "OPay",
    "100033": "PalmPay",
    "100035": "Moniepoint MFB",
}



#Transaction channels
CHANNELS = ["NIP", "CARD_POS", "CARD_WEB", "USSD", "MOBILE_APP"]



#Merchant Category Codes
MCC_CODES = {
    "5411": "Grocery Stores",
    "5541": "Fuel Stations",
    "5812": "Restaurants",
    "4814": "Telecom (Airtime)",
    "6012": "Bank Transfer",
    "7011": "Hotels/Lodging",
    "8011": "Medical Services",
    "8211": "Schools/Education",
}


