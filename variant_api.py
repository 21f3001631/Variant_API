# variant_api.py — Flask API ready for Render deployment

from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from vcfdb import Records  # Ensure vcfdb.py defines the Records model
from dotenv import load_dotenv
import os
import traceback

# Load environment variables from .env or Render environment
load_dotenv()

app = Flask(__name__)

# Load API key and DB URI
API_KEY = os.getenv("VARIANT_API_KEY", "default-key")
DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///vcfdb/vcfdb.sqlite")

# Fix deprecated Postgres URI on Heroku/Render
if DATABASE_URI.startswith("postgres://"):
    DATABASE_URI = DATABASE_URI.replace("postgres://", "postgresql://")

engine = create_engine(DATABASE_URI)

@app.route('/variant', methods=['GET'])
def get_variant():
    try:
        # Auth check
        if request.headers.get("X-API-Key") != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401

        chrom = request.args.get('chr')
        pos = request.args.get('pos')

        # Parameter validation
        if not chrom or not pos:
            return jsonify({"error": "Missing 'chr' or 'pos' parameter"}), 400

        try:
            pos = int(pos)
        except ValueError:
            return jsonify({"error": "Position must be an integer"}), 400

        # Query database
        with Session(engine) as session:
            count = session.query(Records).filter_by(**{"#CHROM": chrom, "POS": pos}).count()

        return jsonify({
            "chr": chrom,
            "pos": pos,
            "count": count
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500

# Entry point for local/server run
if __name__ == "__main__":
    print("✅ Available columns in Records table:")
    for col in Records.__table__.columns:
        print(" -", col.name)

    port = int(os.environ.get("PORT", 5000))  # Render/Heroku assigns dynamic port
    app.run(host="0.0.0.0", port=port)
