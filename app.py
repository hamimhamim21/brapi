# Ensure Depends is imported
from fastapi import FastAPI, HTTPException, Query, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

app = FastAPI()

# Database Configuration
SQLALCHEMY_DATABASE_URL = 'postgresql://postgres:admin@localhost/biologicalsamples'
engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()

# Pydantic models to structure responses


class Status(BaseModel):
    messageType: str
    message: str


class Metadata(BaseModel):
    pagination: Dict[str, Any] = {}
    status: List[Status] = []
    datafiles: List[Dict[str, Any]] = []


class ApiResponse(BaseModel):
    metadata: Metadata
    result: Any  # Use Any to allow different types of payloads


class Variant(BaseModel):
    chromosome: str
    position: int
    ref: str
    alt: str
    quality: float
    filter: str
    info: str


class QualSummary(BaseModel):
    chromosome: str
    quality: float


@app.get("/brapi/v2/variants", response_model=ApiResponse)
def get_variant(
    chrom: str = Query(..., description="Chromosome of the variant"),
    pos: int = Query(...,
                     description="Position of the variant on the chromosome"),
    ref: str = Query(..., description="Reference base"),
    alt: str = Query(..., description="Alternate base"),
    db: Session = Depends(get_db)
):
    sql_query = text(
        "SELECT \"CHROM\", \"POS\", \"REF\", \"ALT\", \"QUAL\", \"FILTER\", \"INFO\" "
        "FROM genomic_data "
        "WHERE \"CHROM\" = :chrom AND \"POS\" = :pos AND \"REF\" = :ref AND \"ALT\" = :alt"
    )
    result = db.execute(
        sql_query, {'chrom': chrom, 'pos': pos, 'ref': ref, 'alt': alt}).fetchone()
    if result is None:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="Variant not found")]), result={})
    variant = Variant(chromosome=result[0], position=result[1], ref=result[2],
                      alt=result[3], quality=result[4], filter=result[5], info=result[6])
    return ApiResponse(metadata=Metadata(status=[Status(messageType="INFO", message="Variant retrieved successfully")]), result=variant)


@app.get("/brapi/v2/qualsummaries", response_model=ApiResponse)
def get_quality_summaries(db: Session = Depends(get_db)):
    sql_query = text(
        "SELECT \"CHROM\", AVG(CAST(\"QUAL\" AS FLOAT)) as AverageQual "
        "FROM genomic_data "
        "GROUP BY \"CHROM\" "
        "ORDER BY AVG(CAST(\"QUAL\" AS FLOAT)) DESC "
        "LIMIT 10"
    )
    result = db.execute(sql_query).fetchall()
    if not result:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="No quality summaries found")]), result=[])
    summaries = [QualSummary(chromosome=row[0], quality=row[1])
                 for row in result]
    return ApiResponse(metadata=Metadata(status=[Status(messageType="INFO", message="Quality summaries retrieved successfully")]), result=summaries)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
