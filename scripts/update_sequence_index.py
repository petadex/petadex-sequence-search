#!/usr/bin/env python3
"""
PETadex Sequence Database Builder

Extracts sequences from PostgreSQL, builds MMseqs2 database, and uploads to S3.
"""

import os
import sys
import subprocess
import argparse
import json
import boto3
from datetime import datetime
from pathlib import Path

# Database connection (assumes environment variables are set)
# DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'petadex'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': os.getenv('DB_PORT', '5432')
}


def extract_sequences(fasta_file):
    """
    Extract sequences from PostgreSQL enzyme_fastaa table.

    Returns:
        int: Number of sequences extracted
    """
    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    print(f"Connecting to database: {DB_CONFIG['host']}/{DB_CONFIG['database']}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query to get sequences
        # Use enzyme_id and translated_sequence columns
        query = """
            SELECT
                enzyme_id,
                genbank_accession_id,
                translated_sequence
            FROM enzyme_fastaa
            WHERE translated_sequence IS NOT NULL
              AND translated_sequence != ''
            ORDER BY enzyme_id
        """

        cursor.execute(query)

        count = 0
        with open(fasta_file, 'w') as f:
            for row in cursor:
                enzyme_id, genbank_id, sequence = row
                # Use genbank_accession_id if available, otherwise enzyme_id
                seq_id = genbank_id if genbank_id else f"enzyme_{enzyme_id}"
                # FASTA format: >header\nsequence
                f.write(f">{seq_id}\n{sequence}\n")
                count += 1

                if count % 10000 == 0:
                    print(f"  Extracted {count:,} sequences...", end='\r')

        print(f"  Extracted {count:,} sequences... Done!")

        cursor.close()
        conn.close()

        return count

    except Exception as e:
        print(f"Database error: {e}")
        raise


def build_mmseqs2_database(fasta_file, db_name):
    """
    Build MMseqs2 database from FASTA file.

    Returns:
        list: List of database files created
    """
    # Check if mmseqs2 is installed
    try:
        subprocess.run(['mmseqs', 'version'],
                      capture_output=True,
                      check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: MMseqs2 not found. Install from: https://github.com/soedinglab/MMseqs2")
        sys.exit(1)

    # Create database
    cmd = ['mmseqs', 'createdb', fasta_file, db_name]
    print(f"  Running: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error creating database:")
        print(result.stderr)
        raise Exception("MMseqs2 createdb failed")

    print("  MMseqs2 database created successfully")

    # Create search index for faster queries
    print("  Creating search index...")
    tmp_dir = str(Path(db_name).parent / "mmseqs_tmp")
    cmd_index = ['mmseqs', 'createindex', db_name, tmp_dir, '--threads', '4']

    result = subprocess.run(cmd_index, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Warning: Index creation failed:")
        print(result.stderr)
        print("Database will work but searches may be slower")
    else:
        print("  Search index created successfully")

    # Clean up tmp directory
    import shutil
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # Find all database files
    db_files = []
    db_path = Path(db_name)
    parent_dir = db_path.parent
    base_name = db_path.name

    for file in parent_dir.glob(f"{base_name}*"):
        db_files.append(str(file))

    print(f"  Database files: {len(db_files)} files")
    for f in db_files:
        size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"    - {os.path.basename(f)}: {size_mb:.2f} MB")

    return db_files


def upload_to_s3(db_name, s3_bucket, s3_prefix):
    """
    Upload all MMseqs2 database files to S3.
    """
    s3_client = boto3.client('s3')

    # Find all database files
    db_path = Path(db_name)
    parent_dir = db_path.parent
    base_name = db_path.name

    db_files = list(parent_dir.glob(f"{base_name}*"))

    if not db_files:
        raise Exception(f"No database files found matching: {db_name}*")

    print(f"  Uploading {len(db_files)} files to s3://{s3_bucket}/{s3_prefix}/")

    for local_file in db_files:
        file_name = local_file.name
        s3_key = f"{s3_prefix}/{file_name}"

        file_size_mb = local_file.stat().st_size / (1024 * 1024)
        print(f"    Uploading {file_name} ({file_size_mb:.2f} MB)...", end='')

        try:
            s3_client.upload_file(
                str(local_file),
                s3_bucket,
                s3_key,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )
            print(" ✓")
        except Exception as e:
            print(f" ✗")
            raise Exception(f"Failed to upload {file_name}: {e}")

    print(f"  All files uploaded successfully")


def create_metadata_file(s3_bucket, s3_prefix, timestamp, num_sequences):
    """
    Create and upload metadata file about the database.
    """
    s3_client = boto3.client('s3')

    metadata = {
        'version': f'enzyme_fastaa_{timestamp}',
        'timestamp': timestamp,
        'num_sequences': num_sequences,
        'source_table': 'enzyme_fastaa',
        'created_at': datetime.now().isoformat(),
        's3_prefix': s3_prefix
    }

    metadata_key = f"{s3_prefix}/metadata.json"

    print(f"  Creating metadata file: {metadata_key}")

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2),
        ContentType='application/json',
        ServerSideEncryption='AES256'
    )

    print("  Metadata file created")


def update_latest_pointer(s3_bucket, timestamp):
    """
    Update the LATEST file to point to the new database version.
    """
    s3_client = boto3.client('s3')

    latest_content = f"enzyme_fastaa_{timestamp}"

    print(f"  Updating LATEST pointer to: {latest_content}")

    s3_client.put_object(
        Bucket=s3_bucket,
        Key='mmseqs2/LATEST',
        Body=latest_content,
        ContentType='text/plain',
        ServerSideEncryption='AES256'
    )

    print("  LATEST pointer updated")


def cleanup_temp_files(fasta_file, db_name):
    """
    Clean up temporary files.
    """
    print("  Cleaning up temporary files...")

    # Remove FASTA file
    if os.path.exists(fasta_file):
        os.remove(fasta_file)
        print(f"    Removed {fasta_file}")

    # Remove database files
    db_path = Path(db_name)
    parent_dir = db_path.parent
    base_name = db_path.name

    for file in parent_dir.glob(f"{base_name}*"):
        os.remove(file)
        print(f"    Removed {file.name}")


def main():
    parser = argparse.ArgumentParser(
        description='Build MMseqs2 sequence database and upload to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build and upload to S3:
  python update_sequence_index.py

  # Build locally only (for testing):
  python update_sequence_index.py --skip-s3

Environment Variables:
  DB_HOST       - PostgreSQL host (default: localhost)
  DB_NAME       - Database name (default: petadex)
  DB_USER       - Database user (default: postgres)
  DB_PASSWORD   - Database password
  DB_PORT       - Database port (default: 5432)

  AWS_ACCESS_KEY_ID       - AWS credentials (if not using IAM role)
  AWS_SECRET_ACCESS_KEY   - AWS credentials (if not using IAM role)
        """
    )

    parser.add_argument(
        '--skip-s3',
        action='store_true',
        help='Skip S3 upload (build locally only)'
    )

    parser.add_argument(
        '--bucket',
        default='petadex-sequence-db',
        help='S3 bucket name (default: petadex-sequence-db)'
    )

    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Configuration
    fasta_file = f"/tmp/enzyme_fastaa_{timestamp}.fasta"
    db_name = f"/tmp/enzyme_fastaa_mmseqs_{timestamp}"
    s3_bucket = args.bucket
    s3_prefix = f"mmseqs2/enzyme_fastaa_{timestamp}"

    print("="*60)
    print("PETadex Sequence Database Builder")
    print("="*60)
    print(f"Source: enzyme_fastaa table")
    print(f"Timestamp: {timestamp}")
    if not args.skip_s3:
        print(f"S3 Bucket: {s3_bucket}")
    print("="*60)

    try:
        # Step 1: Extract sequences from PostgreSQL
        print("\n[1/5] Extracting sequences from PostgreSQL...")
        num_sequences = extract_sequences(fasta_file)

        if num_sequences == 0:
            print("\nError: No sequences extracted!")
            sys.exit(1)

        # Step 2: Build MMseqs2 database
        print("\n[2/5] Building MMseqs2 database...")
        db_files = build_mmseqs2_database(fasta_file, db_name)

        if args.skip_s3:
            print("\n⚠️  Skipping S3 upload (--skip-s3 flag)")
            print(f"Database files located at: {db_name}*")
            print("\nTo upload later, run without --skip-s3 flag")

            print("\n" + "="*60)
            print("✓ DATABASE BUILD COMPLETE (LOCAL ONLY)")
            print("="*60)
            print(f"Sequences indexed: {num_sequences:,}")
            print(f"Local location: {db_name}*")
            print("="*60)
        else:
            # Step 3: Upload to S3
            print("\n[3/5] Uploading to S3...")
            upload_to_s3(db_name, s3_bucket, s3_prefix)

            # Step 4: Create metadata
            print("\n[4/5] Creating metadata...")
            create_metadata_file(s3_bucket, s3_prefix, timestamp, num_sequences)

            # Step 5: Update LATEST pointer
            print("\n[5/5] Updating LATEST pointer...")
            update_latest_pointer(s3_bucket, timestamp)

            # Cleanup
            print("\n[Cleanup] Removing temporary files...")
            cleanup_temp_files(fasta_file, db_name)

            # Final summary
            print("\n" + "="*60)
            print("✓ DATABASE BUILD COMPLETE!")
            print("="*60)
            print(f"Sequences indexed: {num_sequences:,}")
            print(f"S3 Location: s3://{s3_bucket}/{s3_prefix}")
            print(f"Database version: enzyme_fastaa_{timestamp}")
            print("="*60)
            print("\nThe Lambda function will use this database on next cold start.")

    except KeyboardInterrupt:
        print("\n\nBuild interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n{'='*60}")
        print("ERROR during database build")
        print("="*60)
        print(f"{e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
