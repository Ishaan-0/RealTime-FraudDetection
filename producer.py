
import argparse
import json
import time
import uuid
import pandas as pd
from kafka import KafkaProducer

def make_event(row: pd.Series):
    # Convert pandas Series to dict; ensure JSON-serializable types
    record = row.to_dict()
    # Attach a unique transaction_id if not present
    if "transaction_id" not in record:
        record["transaction_id"] = str(uuid.uuid4())
    return record

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:19092")  # EXTERNAL
    parser.add_argument("--topic", default="txns")
    parser.add_argument("--csv-path", required=True, help="./data/credit_card_engineered.csv")
    parser.add_argument("--rate", type=float, default=20, help="Events per second")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to send (0 = all)")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)
    # Basic cleanup
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0

    count = 0
    total = len(df) if args.limit == 0 else min(args.limit, len(df))
    print(f"Producing {total} events to {args.topic} at ~{args.rate} eps via {args.bootstrap_servers}")

    try:
        for _, row in df.iterrows():
            event = make_event(row)
            producer.send(args.topic, value=event)
            count += 1
            if count % 10000 == 0:
                producer.flush()
                print(f"Sent {count}/{total} events...")
            if interval > 0:
                time.sleep(interval)
            if args.limit and count >= args.limit:
                break
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Sent {count} events.")

if __name__ == "__main__":
    main()
