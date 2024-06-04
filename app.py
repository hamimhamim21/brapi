from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import RedirectResponse
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
import os
from dotenv import load_dotenv
import traceback
import httpx
import json
import time
from typing import List
from models import Status, Metadata, ApiResponse, Variant, QualSummary
from utils import extract_and_upload_metadata
if os.path.isdir('static'):
    print("Exists")
else:
    print("Doesn't exists")
    os.mkdir('static')


class MongoJSONEncoder:
    def __init__(self):
        self.encoder = json.JSONEncoder()

    def encode(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return self.encoder.encode(obj)


class TimeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response


# Load environment variables from .env file
load_dotenv()
mongo_url = os.getenv('MONGO_URL')
db_name = os.getenv('MONGO_DB_NAME')
print(mongo_url)
print(db_name)

app = FastAPI()

app.add_middleware(TimeMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/brapi/v2/variants", response_model=ApiResponse)
async def get_variant(
    chrom: str = Query(..., description="Chromosome of the variant"),
    ref: str = Query(..., description="ref of the variant on the chromosome"),
    studyDbIds: str = Query(..., description="studyDbIds"),
    request: Request = None,
):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[studyDbIds]
    result = list(collection.find({"#CHROM": chrom, "REF": ref}))
    if not result:
        return ApiResponse(
            metadata=Metadata(
                status=[Status(messageType="ERROR",
                               message="Variant not found")],
                process_time=float(request.headers.get("X-Process-Time", 0)),
            ),
            result={}
        )

    variants = []
    for document in result:
        variant = Variant(
            chromosome=document["#CHROM"],
            position=int(document["POS"]),
            ref=document["REF"],
            alt=document["ALT"],
            quality=float(document["QUAL"]),
            filter=document["FILTER"],
            info=document["INFO"],
        )
        variants.append(variant)

    return ApiResponse(
        metadata=Metadata(
            status=[Status(messageType="INFO",
                           message="Variants retrieved successfully")],
            process_time=float(request.headers.get("X-Process-Time", 0)),
        ),
        result=variants
    )


@app.get("/brapi/v2/qualsummaries", response_model=ApiResponse)
async def get_quality_summaries(
    studyDbIds: str = Query(..., description="studyDbIds"),
    request: Request = None,
):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[studyDbIds]

    pipeline = [
        {"$group": {"_id": "$#CHROM", "averagequal": {"$avg": {"$toDouble": "$QUAL"}}}},
        {"$sort": {"averagequal": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    print(result)

    if not result:
        return ApiResponse(
            metadata=Metadata(
                status=[Status(messageType="ERROR",
                               message="No quality summaries found")],
                process_time=float(request.headers.get("X-Process-Time", 0)),
            ),
            result=[]
        )

    summaries = [
        QualSummary(chromosome=doc["_id"], quality=doc["averagequal"])
        for doc in result
    ]

    return ApiResponse(
        metadata=Metadata(
            status=[Status(messageType="INFO",
                           message="Quality summaries retrieved successfully")],
            process_time=float(request.headers.get("X-Process-Time", 0)),
        ),
        result=summaries
    )


@app.post("/brapi/v2/upload_data/")
async def upload_data(file: UploadFile = File(...), request: Request = None):
    start_time = time.time()
    try:
        file_location = f"static/{file.filename}"

        if os.path.exists(file_location):
            raise HTTPException(status_code=400, detail="File already exists")
        with open(file_location, "wb") as buffer:
            buffer.write(await file.read())
        extract_and_upload_metadata(file_location, mongo_url, db_name)
        process_time = time.time() - start_time
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error processing file {file.filename}: {str(e)}")

    return {
        "message": f"Metadata for {file.filename} uploaded successfully",
        "process_time": float(request.headers.get("X-Process-Time", process_time))
    }


@app.get("/brapi/v2/search/variantsets", response_model=ApiResponse)
def search_variantsets(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    request: Request = None,
):
    client = MongoClient(mongo_url)
    db = client[db_name]
    variantsets_collection = db.vcf_metadata

    total_count = variantsets_collection.count_documents({})
    total_pages = (total_count + page_size - 1) // page_size
    skip = page * page_size
    limit = page_size

    variantsets_cursor = variantsets_collection.find().skip(skip).limit(limit)
    variantsets = list(variantsets_cursor)

    available_formats = [
        {
            'data_format': variant['data_format'],
            'file_format': variant['file_format'],
            'file_url': variant['file_url']
        } for variant in variantsets if 'data_format' in variant and 'file_format' in variant and 'file_url' in variant
    ]

    if not variantsets:
        return ApiResponse(
            metadata=Metadata(
                status=[Status(messageType="ERROR",
                               message="No variant sets found")],
                process_time=float(request.headers.get("X-Process-Time", 0)),
            ),
            result={}
        )

    processed_results = [
        {
            "callSetCount": variant.get('call_set_count'),
            "referenceSetDbId": variant.get('reference_set_db_id'),
            "studyDbId": variant.get('study_db_id'),
            "variantCount": variant.get('variant_count'),
            "variantSetDbId": variant.get('variant_set_db_id'),
            "variantSetName": variant.get('variant_set_name'),
            "metadataFields": variant.get('metadata_fields'),
        } for variant in variantsets
    ]

    response = {
        "metadata": {
            "pagination": {
                "pageSize": page_size,
                "totalCount": total_count,
                "totalPages": total_pages,
                "currentPage": page,
                "process_time": float(request.headers.get("X-Process-Time", 0))
            }
        },
        "result": {
            "availableFormats": available_formats,
            "data": processed_results
        }
    }

    return response


@app.get("/brapi/v2/search/references")
def search_references(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    studyDbIds: List[str] = Query([], description="List of studyDbIds"),
    request: Request = None,
):
    client = MongoClient(mongo_url)
    db = client[db_name]

    processed_results = []
    total_count = 0

    for studyDbId in studyDbIds:
        collection = db[studyDbId]

        distinct_chroms = collection.distinct("#CHROM")
        chrom_count = len(distinct_chroms)
        total_count += chrom_count

        for chrom in distinct_chroms:
            processed_results.append({
                "referenceDbId": f"{studyDbId}-{chrom}",
                "referenceName": chrom,
                "referenceSetDbId": studyDbId
            })

    total_pages = (total_count + page_size - 1) // page_size
    start = page * page_size
    end = start + page_size

    paginated_results = processed_results[start:end]

    response = {
        "metadata": {
            "pagination": {
                "pageSize": page_size,
                "totalCount": total_count,
                "totalPages": total_pages,
                "currentPage": page,
                "process_time": float(request.headers.get("X-Process-Time", 0))
            }
        },
        "result": {
            "data": paginated_results
        }
    }

    return response


@app.get("/brapi/v2/search/samples")
def search_samples(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    programDbIds: List[str] = Query([], description="programDbIds"),
    request: Request = None,
):
    client = MongoClient(mongo_url)
    db = client[db_name]

    processed_results = []
    total_count = 0
    all_distinct_columns = set()

    for studyDbId in programDbIds:
        collection = db[studyDbId]

        exclude_fields = ["_id", "CHROM", "POS", "ID",
                          "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]
        distinct_columns = [column for column in collection.distinct(
            "_id") if column not in exclude_fields]

        all_distinct_columns.update(distinct_columns)

    for column_name in all_distinct_columns:
        for studyDbId in programDbIds:
            processed_results.append({
                "additionalInfo": {},
                "germplasmDbId": f"{studyDbId}-{column_name}",
                "sampleDbId": studyDbId,
                "sampleName": column_name,
                "studyDbId": studyDbId
            })

    total_count = len(processed_results)

    total_pages = (total_count + page_size - 1) // page_size
    start = page * page_size
    end = start + page_size

    paginated_results = processed_results[start:end]

    response = {
        "metadata": {
            "pagination": {
                "pageSize": page_size,
                "totalCount": total_count,
                "totalPages": total_pages,
                "currentPage": page,
                "process_time": float(request.headers.get("X-Process-Time", 0))
            }
        },
        "result": {
            "data": paginated_results
        }
    }

    custom_encoder = {ObjectId: MongoJSONEncoder().encode}
    return jsonable_encoder(response, custom_encoder=custom_encoder)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
