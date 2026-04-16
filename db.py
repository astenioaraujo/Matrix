import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.uaafkuovkzkozmscyapw",
        password="DataMatrix@1962#",
        host="aws-1-us-east-1.pooler.supabase.com",
        port=5432,
    )