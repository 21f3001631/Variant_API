from flask import Flask, request, jsonify
from sqlalchemy import create_engine, and_, text
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("VARIANT_API_KEY", "default-key")
DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///fallback.db")

# Ensure Heroku-compatible Postgres URI
if DATABASE_URI.startswith("postgres://"):
    DATABASE_URI = DATABASE_URI.replace("postgres://", "postgresql://")

# Flask app and database engine
app = Flask(__name__)
engine = create_engine(DATABASE_URI)

# Import Records model after engine setuprom vcfdb import Records

@app.route('/variant', methods=['GET'])
def get_variant():
    try:
        if request.headers.get("X-API-Key") != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401

        chrom = request.args.get('chr')
        pos = request.args.get('pos')

        if not chrom or not pos:
            return jsonify({"error": "Missing 'chr' or 'pos' parameter"}), 400

        try:
            pos = int(pos)
        except ValueError:
            return jsonify({"error": "Position must be an integer"}), 400

        with Session(engine) as session:
            result = session.execute(
                text("SELECT COUNT(*) FROM Records WHERE \"#CHROM\" = :chrom AND POS = :pos"),
                {"chrom": chrom, "pos": pos}
            )
            count = result.scalar()

        return jsonify({"chr": chrom, "pos": pos, "count": count})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route('/variant_public', methods=['GET'])
def get_variant_public():
    chrom = request.args.get('chr')
    pos = request.args.get('pos')

    if not chrom or not pos:
        return jsonify({"error": "Missing 'chr' or 'pos' parameter"}), 400

    try:
        pos = int(pos)
    except ValueError:
        return jsonify({"error": "Position must be an integer"}), 400

    with Session(engine) as session:
        result = session.execute(
            text("SELECT COUNT(*) FROM Records WHERE \"#CHROM\" = :chrom AND POS = :pos"),
            {"chrom": chrom, "pos": pos}
        )
        count = result.scalar()

    return jsonify({"chr": chrom, "pos": pos, "count": count})

if __name__ == "__main__":
    print("\u2705 Available columns in Records table:")
    for col in Records.__table__.columns:
        print(" -", col.name)

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
