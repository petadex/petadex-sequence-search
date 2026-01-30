import boto3
import subprocess
import os
import json
from pathlib import Path

S3_BUCKET = "petadex-sequence-db"
DB_CACHE_PATH = "/tmp/enzyme_fastaa_mmseqs"

def download_database():
    """
    Download MMseqs2 database from S3 to /tmp
    Only downloads if not already cached
    """
    
    # Check if already cached
    if os.path.exists(f"{DB_CACHE_PATH}.index"):
        print("Database already cached in /tmp")
        return DB_CACHE_PATH
    
    print("Downloading database from S3...")
    s3 = boto3.client('s3')
    
    # Get latest version
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="mmseqs2/LATEST")
        latest_version = obj['Body'].read().decode('utf-8').strip()
        print(f"Latest version: {latest_version}")
    except Exception as e:
        print(f"Error reading LATEST pointer: {e}")
        raise
    
    # List all database files
    prefix = f"mmseqs2/{latest_version}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    if 'Contents' not in response:
        raise Exception(f"No database files found at {prefix}")
    
    # Download each file
    for obj in response['Contents']:
        key = obj['Key']
        filename = key.split('/')[-1]
        
        # Skip metadata file
        if filename == 'metadata.json':
            continue
        
        # Reconstruct expected local path
        local_path = f"/tmp/{filename}"
        
        print(f"Downloading {filename} ({obj['Size']/1024/1024:.2f} MB)...")
        s3.download_file(S3_BUCKET, key, local_path)
    
    print("Database download complete")
    return DB_CACHE_PATH

def validate_sequence(sequence):
    """Validate input protein sequence"""
    
    # Remove whitespace and newlines
    sequence = ''.join(sequence.split())
    
    # Check valid amino acids
    valid_aa = set('ACDEFGHIKLMNPQRSTVWY')
    if not all(c in valid_aa for c in sequence.upper()):
        raise ValueError("Invalid amino acid characters in sequence")
    
    # Check length
    if len(sequence) < 10:
        raise ValueError("Sequence too short (minimum 10 amino acids)")
    
    if len(sequence) > 10000:
        raise ValueError("Sequence too long (maximum 10,000 amino acids)")
    
    return sequence.upper()

def run_search(query_sequence, db_path, max_results=50):
    """
    Run MMseqs2 search against database
    Returns list of matches with similarity metrics
    """
    
    # Write query to temp file
    query_file = "/tmp/query.fasta"
    with open(query_file, 'w') as f:
        f.write(f">query\n{query_sequence}\n")
    
    # Output file
    result_file = "/tmp/results.tsv"
    
    # Run MMseqs2 easy-search
    print(f"Running MMseqs2 search...")
    result = subprocess.run([
        "mmseqs", "easy-search",
        query_file,
        db_path,
        result_file,
        "/tmp",
        "--format-output", "target,qstart,qend,tstart,tend,alnlen,fident,evalue,bits",
        "--max-seqs", str(max_results)
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"MMseqs2 error: {result.stderr}")
        raise Exception(f"MMseqs2 search failed: {result.stderr}")
    
    print(f"Search complete")
    
    # Parse results
    results = []
    if os.path.exists(result_file):
        with open(result_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 9:
                    results.append({
                        'target_id': parts[0],
                        'query_start': int(parts[1]),
                        'query_end': int(parts[2]),
                        'target_start': int(parts[3]),
                        'target_end': int(parts[4]),
                        'alignment_length': int(parts[5]),
                        'percent_identity': float(parts[6]) * 100,
                        'evalue': float(parts[7]),
                        'bitscore': float(parts[8])
                    })
    
    return results

def handler(event, context):
    """
    Lambda handler function
    
    Expected input:
    {
        "sequence": "MKLLIVLLA...",
        "max_results": 50  // optional
    }
    
    Returns:
    {
        "query_length": 150,
        "num_results": 25,
        "results": [...]
    }
    """
    
    try:
        print(f"Event: {json.dumps(event)}")
        
        # Parse input
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event
        
        query_sequence = body.get('sequence', '').strip()
        max_results = body.get('max_results', 50)
        
        # Validate sequence
        query_sequence = validate_sequence(query_sequence)
        
        # Download database (cached after first invocation)
        db_path = download_database()
        
        # Run search
        results = run_search(query_sequence, db_path, max_results)
        
        # Return results
        response = {
            'query_length': len(query_sequence),
            'num_results': len(results),
            'results': results
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(response)
        }
        
    except ValueError as e:
        # Validation error
        print(f"Validation error: {e}")
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }
        
    except Exception as e:
        # Internal error
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }
