from pymongo import MongoClient
import os
from dotenv import load_dotenv, find_dotenv

print("=== MongoDB Connection Test ===\n")

# Try to find .env in current directory or parent directories
env_path = find_dotenv(usecwd=True)
if env_path:
    load_dotenv(env_path)
    print(f"✓ Loaded .env from: {env_path}")
else:
    print("⚠ No .env file found, checking environment variables...")

# Get URI from environment
uri = os.getenv('MONGO_URI')

if not uri:
    print("❌ MONGO_URI not found!")
    print("Set it via:")
    print("  1. Create .env file in current directory")
    print("  2. Export MONGO_URI in shell profile")
    print("  3. Activate conda env with MONGO_URI set")
    exit(1)

# Debug: Print first 50 chars of URI (hide password)
print(f"\n📡 Testing connection...")
print(f"URI format: {uri[:50]}...")

try:
    # Create client with proper parameters
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5000,
        tlsAllowInvalidCertificates=True
    )
    
    # Test connection
    client.admin.command('ping')
    print('✅ MongoDB connected successfully!')
    
    # List databases
    dbs = client.list_database_names()
    print(f'✅ Available databases: {dbs}')
    
    client.close()
    
except Exception as e:
    print(f'❌ Connection failed: {e}')
    print(f'\nConnection string being used:')
    print(f'  Username: {uri.split("://")[1].split(":")[0] if "://" in uri else "unknown"}')
    print(f'  Cluster: {uri.split("@")[1].split("/")[0] if "@" in uri else "unknown"}')
    exit(1)
