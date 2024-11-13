import lmdb
import os
import sys
from pathlib import Path

def print_lmdb_stats(lmdb_path):
    # Check if path exists and contains LMDB files
    path = Path(lmdb_path)
    data_file = path / 'data.mdb'
    
    if not path.exists() or not data_file.exists():
        print(f"Error: Path '{lmdb_path}' does not contain a valid LMDB database")
        return
    
    # Open and print stats for the single database
    env = lmdb.open(str(path), readonly=True)
    print("\nLMDB Statistics:")
    print("-" * 40)
    stats = env.stat()
    for key, value in stats.items():
        print(f"{key}: {value}")

    # Sample some data from each sub-database
    with env.begin() as txn:
        # Get list of sub-databases
        databases = [None]  # None represents the main DB
        
        for db_name in databases:
            db_label = 'main' if db_name is None else db_name.decode('utf-8')
            print(f"\nData Sample from database '{db_label}' (first 5 entries):")
            print("-" * 60)
            
            cursor = txn.cursor(db=db_name)
            for i, (key, value) in enumerate(cursor):
                if i >= 5:
                    break
                try:
                    print(f"Key: {key.decode('utf-8')}")
                    print(f"Value: {value.decode('utf-8')[:100]}...")
                except UnicodeDecodeError:
                    print(f"Key: {key!r}")
                    print(f"Value: <binary data, length: {len(value)} bytes>")
                print()

    env.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python print_stats.py <lmdb_path>")
        sys.exit(1)
    
    print_lmdb_stats(sys.argv[1])