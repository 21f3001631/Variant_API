
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from vcfdb import Records  # Ensure this file exists and is correctly defined
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = Flask(__name__)
API_KEY = os.getenv("VARIANT_API_KEY", "default-key")
DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///vcfdb/vcfdb.sqlite")

# Ensure compatibility with Heroku Postgres URI formatting
if DATABASE_URI.startswith("postgres://"):
    DATABASE_URI = DATABASE_URI.replace("postgres://", "postgresql://")

engine = create_engine(DATABASE_URI)

@app.route('/variant', methods=['GET'])
def get_variant():
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
        try:
            count = session.query(Records).filter_by(**{"#CHROM": chrom, "POS": pos}).count()
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({
        "chr": chrom,
        "pos": pos,
        "count": count
    })

if __name__ == "__main__":
    print("✅ Available columns in Records table:")
    for col in Records.__table__.columns:
        print(" -", col.name)

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
