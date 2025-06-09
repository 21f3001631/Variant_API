from flask import Flask, request, jsonify, send_from_directory
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)
CORS(app)  # 🔥 Allow all origins

API_KEY = os.getenv("VARIANT_API_KEY", "default-key")
DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///database.sqlite")

if DATABASE_URI.startswith("postgres://"):
    DATABASE_URI = DATABASE_URI.replace("postgres://", "postgresql://")

engine = create_engine(DATABASE_URI)

from vcfdb import Records

@app.route('/')
def serve_index():
    return send_from_directory('.', 'index.html')

@app.route('/variant_public', methods=['GET'])
def get_variant_public():
    chrom = request.args.get('chr')
    pos = request.args.get('pos')
    print(f"🔍 Incoming request: chr={chrom}, pos={pos}")

    if not chrom or not pos:
        print("❌ Missing 'chr' or 'pos'")
        return jsonify({"error": "Missing 'chr' or 'pos' parameter"}), 400

    try:
        pos = int(pos)
    except ValueError:
        print("❌ Position not an integer")
        return jsonify({"error": "Position must be an integer"}), 400

    with Session(engine) as session:
        # Query 1: stats for this variant
        variant_stats = session.execute(
            text("""
                SELECT 
                    SUM(CASE WHEN IDF = '0/1' THEN 1 ELSE 0 END) AS het_count,
                    SUM(CASE WHEN IDF = '1/1' THEN 1 ELSE 0 END) AS hom_count,
                    COUNT(*) AS total
                FROM Records
                WHERE "#CHROM" = :chrom AND POS = :pos
            """),
            {"chrom": chrom, "pos": pos}
        ).fetchone()

        # Query 2: total distinct files in DB
        distinct_file_count = session.execute(
            text("SELECT COUNT(DISTINCT file_id) FROM Records")
        ).scalar()

        het_count = variant_stats.het_count or 0
        hom_count = variant_stats.hom_count or 0
        total = variant_stats.total or 0
        other_count = total - het_count - hom_count

    print(f"✅ Result: {total} records found at {chrom}:{pos}")
    return jsonify({
        "chr": chrom,
        "pos": pos,
        "count": total,
        "distinct_files": distinct_file_count,
        "heterozygous (0/1)": het_count,
        "homozygous (1/1)": hom_count,
        "other_genotypes": other_count
    })

if __name__ == "__main__":
    print("✅ Available columns in Records table:")
    for col in Records.__table__.columns:
        print(" -", col.name)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
