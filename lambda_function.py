import boto3
import subprocess
import os
import json
import uuid
import psycopg2
import psycopg2.extras
from pathlib import Path

S3_BUCKET = "petadex"
PREFIX = "mmseqs2/"
DB_CACHE_PATH = "/tmp/enzyme_fastaa_mmseqs"
DB_HOST = "petadex.ccz9y6yshbls.us-east-1.rds.amazonaws.com"
DB_NAME = "petadex"

_db_secret_cache = None

def get_db_credentials():
    global _db_secret_cache
    if _db_secret_cache:
        return _db_secret_cache
    sm = boto3.client("secretsmanager", region_name="us-east-1")
    secret = json.loads(sm.get_secret_value(SecretId=os.environ["DB_SECRET_ARN"])["SecretString"])
    _db_secret_cache = secret
    return secret


def fetch_metadata(accession_ids):
    """Fetch BLAST_NR_METADATA rows for a list of genbank_accession_ids."""
    if not accession_ids:
        return {}
    creds = get_db_credentials()
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=creds["username"], password=creds["password"],
        connect_timeout=5
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT genbank_accession_id, organism, protein_id, definition,
                          taxonomy, journal, collection_date, country
                   FROM blast_nr_metadata
                   WHERE genbank_accession_id = ANY(%s)""",
                (list(accession_ids),)
            )
            return {row["genbank_accession_id"]: dict(row) for row in cur.fetchall()}
    finally:
        conn.close()


def download_database():
    """
    Download MMseqs2 database from S3 to /tmp
    Only downloads if not already cached
    """

    s3 = boto3.client("s3", region_name="us-east-1")

    # Get latest version
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{PREFIX}LATEST")
        latest_version = obj["Body"].read().decode("utf-8").strip().rstrip("/")
        print(f"Targeting database version: {latest_version}")
    except Exception as e:
        print(f"Error reading LATEST pointer: {e}")
        raise

    # List all database files
    prefix = f"{PREFIX}{latest_version}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        raise Exception(f"No database files found at {prefix}")

    # Extract database base name from first file (before downloading)
    db_base_name = None
    for obj in response["Contents"]:
        key = obj["Key"]
        filename = key.split("/")[-1]

        # Skip directory markers and metadata
        if not filename or filename.endswith("/") or filename == "metadata.json":
            continue

        # Get base name by removing file extensions
        db_base_name = filename.split(".")[0]
        if db_base_name.endswith("_h"):
            db_base_name = db_base_name[:-2]
        break

    if db_base_name is None:
        raise Exception("Could not determine database base name from files")

    actual_db_path = f"/tmp/{db_base_name}"
    print(f"Detected database base: {actual_db_path}")

    # Check if already cached BEFORE downloading
    if os.path.exists(f"{actual_db_path}.index"):
        print("Database already cached in /tmp")
        return actual_db_path

    print("Downloading database from S3...")

    # Download each file
    for obj in response["Contents"]:
        key = obj["Key"]
        filename = key.split("/")[-1]

        # Skip directory markers
        if not filename or filename.endswith("/"):
            print(f"Skipping directory marker: {key}")
            continue

        # Skip metadata file
        if filename == "metadata.json":
            continue

        local_path = f"/tmp/{filename}"
        print(f"Downloading {filename} ({obj['Size'] / 1024 / 1024:.2f} MB)...")
        s3.download_file(S3_BUCKET, key, local_path)

    print("Database download complete")
    return actual_db_path


def parse_fasta(raw):
    """Parse a FASTA string into (header, sequence). Also accepts bare sequence."""
    raw = raw.strip()
    if raw.startswith(">"):
        lines = raw.splitlines()
        header = lines[0][1:].strip()  # strip leading '>'
        sequence = "".join(lines[1:])
    else:
        header = None
        sequence = raw
    return header, sequence


def validate_sequence(sequence):
    """Validate input protein sequence"""

    # Remove whitespace and newlines
    sequence = "".join(sequence.split())

    # Check valid amino acids
    valid_aa = set("ACDEFGHIKLMNPQRSTVWY")
    if not all(c in valid_aa for c in sequence.upper()):
        raise ValueError("Invalid amino acid characters in sequence")

    # Check length
    if len(sequence) < 10:
        raise ValueError("Sequence too short (minimum 10 amino acids)")

    if len(sequence) > 10000:
        raise ValueError("Sequence too long (maximum 10,000 amino acids)")

    return sequence.upper()


def run_search(query_sequence, db_path, session_id, max_results=50, query_header=None):
    """
    Run MMseqs2 search and upload results to S3
    Returns S3 key for the results file
    """

    # Write query to temp file
    query_file = "/tmp/query.fasta"
    with open(query_file, "w") as f:
        f.write(f">query\n{query_sequence}\n")

    # Output file
    result_file = "/tmp/results.tsv"

    # Run MMseqs2 easy-search
    print(f"Running MMseqs2 search...")
    result = subprocess.run(
        [
            "mmseqs",
            "easy-search",
            query_file,
            db_path,
            result_file,
            "/tmp",
            "--format-output",
            "target,qstart,qend,tstart,tend,alnlen,fident,evalue,bits",
            "--max-seqs",
            str(max_results),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"MMseqs2 error: {result.stderr}")
        raise Exception(f"MMseqs2 search failed: {result.stderr}")

    print(f"Search complete")

    # Parse results
    results = []
    if os.path.exists(result_file):
        with open(result_file, "r") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 9:
                    results.append(
                        {
                            "target_id": parts[0],
                            "query_start": int(parts[1]),
                            "query_end": int(parts[2]),
                            "target_start": int(parts[3]),
                            "target_end": int(parts[4]),
                            "alignment_length": int(parts[5]),
                            "percent_identity": float(parts[6]) * 100,
                            "evalue": float(parts[7]),
                            "bitscore": float(parts[8]),
                        }
                    )

    # Enrich results with metadata
    accession_ids = {r["target_id"] for r in results}
    metadata = fetch_metadata(accession_ids)
    for r in results:
        r["metadata"] = metadata.get(r["target_id"])

    # Upload to S3
    job_id = str(uuid.uuid4())
    s3_key = f"results/{session_id}/{job_id}.json"

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(
            {
                "query_header": query_header,
                "query_sequence": query_sequence,
                "query_length": len(query_sequence),
                "num_results": len(results),
                "results": results,
            }
        ),
        ContentType="application/json",
    )

    print(f"Results uploaded to s3://{S3_BUCKET}/{s3_key}")

    # Write index file so Express can find this result by sessionId
    # results/{sessionId}.index contains the job_id (uuid)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"results/{session_id}.index",
        Body=job_id,
        ContentType="text/plain",
    )
    print(f"Index written to s3://{S3_BUCKET}/results/{session_id}.index")

    return job_id


def get_history(session_id):
    """
    List all search results for a given session
    Returns list of job metadata
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    prefix = f"results/{session_id}/"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        return []

    history = []
    for obj in response["Contents"]:
        key = obj["Key"]
        # Extract job_id from path: results/{sessionId}/{job_id}.json
        job_id = key.split("/")[-1].replace(".json", "")
        history.append(
            {
                "job_id": job_id,
                "s3_key": key,
                "last_modified": obj["LastModified"].isoformat(),
                "size": obj["Size"],
            }
        )

    # Sort by last_modified descending (most recent first)
    history.sort(key=lambda x: x["last_modified"], reverse=True)
    return history


def handler(event, context):
    """
    Lambda handler function

    Expected input for search:
    {
        "action": "search",  // optional, default
        "sessionId": "abc123",
        "sequence": "MKLLIVLLA...",
        "max_results": 50  // optional
    }

    Expected input for history:
    {
        "action": "history",
        "sessionId": "abc123"
    }

    Returns for search:
    {
        "job_id": "uuid",
        "s3_key": "results/{sessionId}/{job_id}.json"
    }

    Returns for history:
    {
        "history": [...]
    }
    """

    try:
        print(f"Event: {json.dumps(event)}")

        # Parse input
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event

        action = body.get("action", "search")
        session_id = body.get("sessionId")

        if not session_id:
            raise ValueError("sessionId is required")

        # Route by action
        if action == "history":
            history = get_history(session_id)
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"history": history}),
            }

        # Default: search action
        raw_input = body.get("sequence", "").strip()
        max_results = body.get("max_results", 50)

        # Parse FASTA (supports both ">header\nsequence" and bare sequence)
        query_header, query_sequence = parse_fasta(raw_input)

        # Validate sequence
        query_sequence = validate_sequence(query_sequence)

        # Download database (cached after first invocation)
        db_path = download_database()

        # Run search
        job_id = run_search(query_sequence, db_path, session_id, max_results, query_header)

        # Return job ID
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(
                {"job_id": job_id, "s3_key": f"results/{session_id}/{job_id}.json"}
            ),
        }

    except ValueError as e:
        # Validation error
        print(f"Validation error: {e}")
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }

    except Exception as e:
        # Internal error
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal server error"}),
        }
