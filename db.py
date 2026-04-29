import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL yoki SUPABASE_KEY topilmadi")

print("SUPABASE_URL:", SUPABASE_URL)
print("SUPABASE_KEY prefix:", SUPABASE_KEY[:20])

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
