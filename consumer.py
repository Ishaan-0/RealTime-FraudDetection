import argparse
import json
import os
import numpy as np
import pandas as pd
import joblib
from datetime import datetime  
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dotenv import load_dotenv, find_dotenv


# loading env variables for the mongodb connection string
env_path = find_dotenv(usecwd=True)
if env_path:
    load_dotenv(env_path)
    print(f"✓ Loaded .env from: {env_path}")
else:
    print("⚠ No .env file found, checking environment variables...")

uri = os.getenv('MONGO_URI') # actual uri from .env

# MongoDB constants
DB_NAME = os.getenv('MONGO_DB_NAME', 'FraudDetection')
FRAUD_COLLECTION = os.getenv('FRAUD_COLLECTION_NAME', 'FraudAlerts')
NON_FRAUD_COLLECTION = os.getenv('NON_FRAUD_COLLECTION_NAME', 'nonFraudTransactions')

def build_schema(feature_names):
    fields = []
    # transaction_id
    fields.append(StructField("transaction_id", StringType(), True))
    # Raw Kaggle fields
    for i in range(1, 29):
        name = f"V{i}"
        fields.append(StructField(name, DoubleType(), True))
    fields.append(StructField("Class", IntegerType(), True))
    # Engineered fields — add generously, nullable
    for extra in ["log_amt", "outlier_score", "DayOfWeek", "IsWeekend"]:
        fields.append(StructField(extra, DoubleType(), True))
    # Any other columns in feature_names not covered (from engineered file)
    for f in feature_names:
        if f not in [f"V{i}" for i in range(1, 29)] and f not in ["Amount", "Time", "Class",
                                                                  "log_amt", "outlier_score", 
                                                                  "DayOfWeek", "IsWeekend"]:
            fields.append(StructField(f, DoubleType(), True))

    return StructType(fields)


def connect_mongo(uri):
    try:
        mongo_client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
        )
        mongo_client.admin.command('ping')
        print('✅ MongoDB connected!')
    except Exception as e:
        print(f'❌ Connection failed: {e}')
        exit(1)
    
    def check_user_role(mongo_client):
        try:
            
            # all currently authenticated users info and what permissions they have
            user_info = mongo_client.admin.command('connectionStatus') 
            
            auth_info = user_info.get('authInfo', {}) # all authentication info
            authenticated_users = auth_info.get('authenticatedUsers', []) # list of all authenticated users
            
            # if there are no authenticated users, return unknown
            if not authenticated_users: 
                return False, "unknown", []
            
            user = authenticated_users[0] # user whose connected to the cluster rn, the one in the connection string
            username = user.get('user', 'unknown') 
            
            user_roles = {role.get('role') for role in user_info['authInfo']['authenticatedUserRoles']}
            admin_roles = {'atlasAdmin', 'root', 'dbAdminAnyDatabase', 'userAdminAnyDatabase', 'readWriteAnyDatabase'}
            is_admin = bool(user_roles & admin_roles)

            return is_admin, username, user_roles

        except Exception as e:
            print(f"❌ MongoDB user role check failed: {e}")
            raise
        
    def setup_database(mongo_client):        
        # Check user role
        is_admin, username, roles = check_user_role(mongo_client)
        
        print(f"\n👤 Connected as: {username}")
        print(f"🔑 Roles: {[r.get('role') for r in roles]}")
        print(f"🔒 Admin privileges: {'✓ Yes' if is_admin else '✗ No'}\n")
        
        # Check if database exists
        existing_dbs = mongo_client.list_database_names()
        print(f"📂 Existing databases: {existing_dbs}")
        
        if DB_NAME in existing_dbs:
            print(f"✓ Database '{DB_NAME}' found")
            db = mongo_client[DB_NAME]
            
            # Check collections
            existing_collections = db.list_collection_names()
            print(f"✓ Existing collections: {existing_collections}")
            
            # Verify required collections exist
            missing_collections = []
            if FRAUD_COLLECTION not in existing_collections:
                missing_collections.append(FRAUD_COLLECTION)
            if NON_FRAUD_COLLECTION not in existing_collections:
                missing_collections.append(NON_FRAUD_COLLECTION)
            
            if missing_collections:
                if is_admin:
                    print(f"⚠ Missing collections: {missing_collections}")
                    for coll_name in missing_collections:
                        db.create_collection(coll_name)
                        print(f"✓ Created collection: {coll_name}")
                else:
                    print(f"❌ Missing collections: {missing_collections}")
                    print(f"❌ ERROR: Admin privileges required to create collections!")
                    raise PermissionError(
                        f"User '{username}' does not have admin privileges. "
                        f"Cannot create missing collections: {missing_collections}"
                    )
        else:
            # Database doesn't exist - need admin to create
            if is_admin:
                print(f"⚠ Database '{DB_NAME}' not found. Creating...")
                db = mongo_client[DB_NAME]
                
                # Create collections
                db.create_collection(FRAUD_COLLECTION)
                db.create_collection(NON_FRAUD_COLLECTION)
                
                print(f"✓ Created database: {DB_NAME}")
                print(f"✓ Created collections: {FRAUD_COLLECTION}, {NON_FRAUD_COLLECTION}")
            else:
                print(f"❌ Database '{DB_NAME}' not found!")
                print(f"❌ ERROR: Admin privileges required to create database!")
                raise PermissionError(
                    f"User '{username}' does not have admin privileges. "
                    f"Cannot create database '{DB_NAME}'. "
                    f"Please contact your database administrator."
                )
        
        # Get collection references
        db = mongo_client[DB_NAME]
        fraud_collection = db[FRAUD_COLLECTION]
        non_fraud_collection = db[NON_FRAUD_COLLECTION]
        
        print(f"\n✅ Database setup complete!")
        print(f"   Database: {DB_NAME}")
        print(f"   Collections: {FRAUD_COLLECTION}, {NON_FRAUD_COLLECTION}\n")
        
        return db, fraud_collection, non_fraud_collection, is_admin
    
    db, fraud_collection, non_fraud_collection, is_admin = setup_database(mongo_client)
    return mongo_client, db, fraud_collection, non_fraud_collection, is_admin

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--artifacts-dir", default="./artifacts")
    parser.add_argument("--output-dir", default="./output/alerts")
    parser.add_argument("--checkpoint-dir", default="./output/checkpoints")
    parser.add_argument(
        "--mongo-uri",
        default=uri,  # Use the URI from environment variables as default
        help="MongoDB Atlas connection string"
    )
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of messages to process per batch")
    parser.add_argument("--batch-interval", type=int, default=10,
                       help="Batch processing interval in seconds")
    args = parser.parse_args()

    mongo_client, db, fraud_collection, non_fraud_collection, is_admin = connect_mongo(args.mongo_uri)
    
    # Load artifacts on driver
    rf_path = os.path.join(args.artifacts_dir, "rf_pipeline.joblib")
    xgb_path = os.path.join(args.artifacts_dir, "xgboost_pipeline.joblib")
    
    if not (os.path.exists(rf_path) or os.path.exists(xgb_path)):
        raise FileNotFoundError("Model artifacts not found. Train and save them first.")

    rf_model = joblib.load(rf_path) if os.path.exists(rf_path) else None
    xgb_model = joblib.load(xgb_path) if os.path.exists(xgb_path) else None

    # For RF pipeline we can get feature names safely
    if rf_model is not None and hasattr(rf_model, "feature_names_in_"):
        feature_names = list(rf_model.feature_names_in_)
    elif xgb_model is not None and hasattr(xgb_model, "feature_names_in_"):
        feature_names = list(xgb_model.feature_names_in_)
    else:
        raise ValueError("Could not determine feature names from saved models.")

    # Broadcast artifacts (driver -> executors)
    spark = SparkSession.builder.appName("FraudDetectionStream").getOrCreate()
    sc = spark.sparkContext
    b_rf_model = sc.broadcast(rf_model)
    b_xgb_model = sc.broadcast(xgb_model)
    b_features = sc.broadcast(feature_names)
    threshold = args.threshold

    # Kafka source
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", args.batch_size)  # Process messages per batch
        .option("spark.streaming.kafka.maxRatePerPartition", args.batch_size // 10)  # Rate limit per partition
        .load()
    )

    json_df = raw.selectExpr("CAST(value AS STRING) as json_str")

    # Build a schema flexible enough for engineered or raw
    schema = build_schema(feature_names=b_features.value)
    parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
    
    # ForeachBatch scoring function
    def foreach_batch(df, epoch_id: int):
        if df.rdd.isEmpty():
            return
        
        # Ensure all expected features exist
        use_cols = b_features.value
        for c in use_cols:
            if c not in df.columns:
                df = df.withColumn(c, col("Amount") * 0.0)
        
        # Select features + transaction_id for output
        cols_for_pd = ["transaction_id"] + list(use_cols)
        cols_for_pd = [c for c in cols_for_pd if c in df.columns]
        pdf = df.select(*cols_for_pd).toPandas()
        
        if pdf.empty:
            return
        
        tx_ids = pdf["transaction_id"] if "transaction_id" in pdf.columns else None
        
        # Replace inf/-inf with NaN
        X_raw = pdf[use_cols]
        X_raw.replace([np.inf, -np.inf], np.nan, inplace=True)
        X = X_raw.values
        
        # Get model predictions
        rf_model = b_rf_model.value
        xgb_model = b_xgb_model.value
        probs = None
        
        if rf_model is not None and hasattr(rf_model, "predict_proba"):
            probs = rf_model.predict_proba(X)[:, 1]
        elif xgb_model is not None and hasattr(xgb_model, "predict_proba"):
            probs = xgb_model.predict_proba(X)[:, 1]
        else:
            preds = (rf_model or xgb_model).predict(X)
            probs = (preds - np.min(preds)) / (np.ptp(preds) + 1e-9)
        
        out = pd.DataFrame({
            "transaction_id": tx_ids if tx_ids is not None else range(len(probs)),
            "fraud_prob": probs,
            "flagged": (probs >= threshold).astype(int),
        })
        
        # Add timestamp
        out["scored_at"] = datetime.utcnow()
        
        # ========== CORRECTED MONGODB WRITES ==========
        # Separate fraud and non-fraud transactions
        fraud_records = out[out["flagged"] == 1].copy()
        non_fraud_records = out[out["flagged"] == 0].copy()
        
        # Write fraud transactions to FraudAlerts collection
        if not fraud_records.empty:
            try:
                fraud_docs = fraud_records[["transaction_id", "fraud_prob", "scored_at"]].to_dict("records")
                fraud_collection.insert_many(fraud_docs)  # Use fraud_collection from outer scope
                print(f"[epoch {epoch_id}] ✓ MongoDB: {len(fraud_docs)} fraud alerts → FraudAlerts")
            except Exception as e:
                print(f"[epoch {epoch_id}] ✗ Fraud insert failed: {e}")
        
        # Write non-fraud transactions to nonFraudTransactions collection
        if not non_fraud_records.empty:
            try:
                non_fraud_docs = non_fraud_records[["transaction_id", "fraud_prob", "scored_at"]].to_dict("records")
                non_fraud_collection.insert_many(non_fraud_docs)  # Use non_fraud_collection from outer scope
                print(f"[epoch {epoch_id}] ✓ MongoDB: {len(non_fraud_docs)} clean transactions → nonFraudTransactions")
            except Exception as e:
                print(f"[epoch {epoch_id}] ✗ Non-fraud insert failed: {e}")
        
        # Keep existing Parquet write (for fraud only)
        if not fraud_records.empty:
            os.makedirs(args.output_dir, exist_ok=True)
            out_path = os.path.join(args.output_dir, "alerts.parquet")
            fraud_records.to_parquet(out_path, index=False, mode='append')
            print(f"[epoch {epoch_id}] ✓ Parquet: {len(fraud_records)} fraud alerts")
        
        # Stats logging
        total = len(out)
        fraud_count = len(fraud_records)
        non_fraud_count = len(non_fraud_records)
        mean_prob = out["fraud_prob"].mean()
        max_prob = out["fraud_prob"].max()
        
        # Verify MongoDB collections
        try:
            fraud_count_mongo = fraud_collection.count_documents({"scored_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)}})
            non_fraud_count_mongo = non_fraud_collection.count_documents({"scored_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)}})
            print(f"[epoch {epoch_id}] 🔍 MongoDB Verification:")
            print(f"   - FraudAlerts today: {fraud_count_mongo}")
            print(f"   - NonFraudTransactions today: {non_fraud_count_mongo}")
        except Exception as e:
            print(f"[epoch {epoch_id}] ❌ MongoDB verification failed: {e}")
        
        print(f"[epoch {epoch_id}] 📊 Stats: Total={total} | Fraud={fraud_count} | "
            f"Clean={non_fraud_count} | Mean prob={mean_prob:.4f} | Max prob={max_prob:.4f}\n")

    query = (
        parsed.writeStream.foreachBatch(foreach_batch)
        .trigger(processingTime=f"{args.batch_interval} seconds")  # Process batch based on interval parameter
        .option("checkpointLocation", args.checkpoint_dir)
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
