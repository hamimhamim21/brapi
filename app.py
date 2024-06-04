from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile, File
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient

from typing import List, Dict, Any, Optional
from models import Status, Metadata, ApiResponse, Variant, QualSummary
# from database import get_db, get_dburl, execute_query_to_dataframe
from utils import extract_and_upload_metadata
from fastapi.middleware.cors import CORSMiddleware
import httpx
from fastapi import APIRouter, HTTPException
import traceback
from fastapi import FastAPI, APIRouter, HTTPException, Depends
import httpx
import os
from dotenv import load_dotenv
from starlette.responses import RedirectResponse
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
import json


class MongoJSONEncoder:
    def __init__(self):
        self.encoder = json.JSONEncoder()

    def encode(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return self.encoder.encode(obj)


# Extract MongoDB connection details from environment variables
load_dotenv()
mongo_url = os.getenv('MONGO_URL')
db_name = os.getenv('MONGO_DB_NAME')
print(mongo_url)
print(db_name)
# Load environment variables from .env file
load_dotenv()
app = FastAPI()
router = APIRouter()
origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load SQL queries
# variant_query = read_query('./database_queries/variants_query.sql')
# quality_summaries_query = read_query(
#     './database_queries/qual_summaries_query.sql')
# variantsetsearch_query = read_query(
#     './database_queries/variantsets.sql')
# availableFormats_query = read_query(
#     './database_queries/availableFormats.sql'
# )
# distinctDBId = read_query(
#     './database_queries/distinctDB_Id.sql'
# )
# getRefernces_query = read_query(
#     './database_queries/getReferences.sql'
# )
# getSamples_query = read_query('./database_queries/getSamples.sql')

########################## TESTING##########################################
# Using environment variables for configuration
API_URL = os.getenv("API_URL")
API_USERNAME = os.getenv("API_USERNAME")
API_PASSWORD = os.getenv("API_PASSWORD")
token_storage = {"token": 'lol'}


@router.post("//api/Clients/login")
async def login():
    print('Logged in successfully')
    return {"message": "Logged in successfully"}


@router.get("//api/Blocks/blockFeatureLimits")
async def get_data():
    return RedirectResponse(url="/brapi/v2/search/variantsets")


@router.get("/external-data")
async def get_external_data():
    data_url = f"{API_URL}/AnotherEndpoint"
    async with httpx.AsyncClient() as client:
        # Removed the Authorization header
        response = await client.get(data_url)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code,
                                detail="Failed to retrieve data from external API")
        return response.json()


app.include_router(router)

#################################### TESTING################################


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/brapi/v2/variants", response_model=ApiResponse)
def get_variant(
    chrom: str = Query(..., description="Chromosome of the variant"),
    ref: str = Query(..., description="ref of the variant on the chromosome"),
    studyDbIds: str = Query(..., description="studyDbIds"),
):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[studyDbIds]
    result = list(collection.find({"#CHROM": chrom, "REF": ref}))
    if not result:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="Variant not found")]), result={})

    variants = []
    for document in result:
        variant = Variant(
            chromosome=document["#CHROM"],
            position=int(document["POS"]),
            ref=document["REF"],
            alt=document["ALT"],
            quality=float(document["QUAL"]),
            filter=document["FILTER"],
            info=document["INFO"]
        )
        variants.append(variant)

    return ApiResponse(
        metadata=Metadata(
            status=[Status(messageType="INFO",
                           message="Variants retrieved successfully")]
        ),
        result=variants
    )


@app.get("/brapi/v2/qualsummaries", response_model=ApiResponse)
def get_quality_summaries(studyDbIds: str = Query(..., description="studyDbIds")):
    # Connect to MongoDB
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[studyDbIds]

    # Aggregate to get the average quality for each chromosome
    pipeline = [
        {"$group": {"_id": "$#CHROM", "averagequal": {"$avg": {"$toDouble": "$QUAL"}}}},
        {"$sort": {"averagequal": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    print(result)

    if not result:
        return ApiResponse(
            metadata=Metadata(status=[Status(messageType="ERROR", message="No quality summaries found")]), result=[])

    summaries = [
        QualSummary(chromosome=doc["_id"], quality=doc["averagequal"])
        for doc in result
    ]

    return ApiResponse(
        metadata=Metadata(status=[Status(
            messageType="INFO", message="Quality summaries retrieved successfully")]),
        result=summaries
    )


@app.post("/brapi/v2/upload_data/")
async def upload_data(file: UploadFile = File(...)):
    try:
        file_location = f"static/{file.filename}"

        # Check if the file already exists
        if os.path.exists(file_location):
            raise HTTPException(status_code=400, detail="File already exists")
        # Save the uploaded file to the static directory
        with open(file_location, "wb") as buffer:
            buffer.write(await file.read())
        # Process the file content to extract and upload metadata
        extract_and_upload_metadata(file_location, mongo_url, db_name)

        # Save the uploaded file to the static directory
        with open(file_location, "wb") as buffer:
            buffer.write(await file.read())

    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error processing file {file.filename}: {str(e)}")

    return {"message": f"Metadata for {file.filename} uploaded successfully"}


@app.get("/brapi/v2/search/variantsets", response_model=ApiResponse)
def search_variantsets(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page")
):
    # Connect to MongoDB
    client = MongoClient(mongo_url)
    db = client[db_name]
    variantsets_collection = db.vcf_metadata

    # Pagination logic
    total_count = variantsets_collection.count_documents({})
    total_pages = (total_count + page_size - 1) // page_size
    skip = page * page_size
    limit = page_size

    variantsets_cursor = variantsets_collection.find().skip(skip).limit(limit)
    variantsets = list(variantsets_cursor)

    # Retrieve distinct available formats
    # available_formats_cursor = variantsets_collection.distinct('file_format')
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
                status=[Status(messageType="ERROR", message="No variant sets found")]).dict(),
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
                "currentPage": page
            }
        },
        "result": {
            "availableFormats": available_formats,
            "data": processed_results
        }
    }

    return response


@ app.get("/brapi/v2/search/references")
def search_references(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    studyDbIds: List[str] = Query([], description="List of studyDbIds")
):
    # Connect to MongoDB
    client = MongoClient(mongo_url)
    db = client[db_name]

    processed_results = []
    total_count = 0

    for studyDbId in studyDbIds:
        collection = db[studyDbId]

        # Fetch distinct CHROM values and count
        distinct_chroms = collection.distinct("#CHROM")
        chrom_count = len(distinct_chroms)
        total_count += chrom_count

        # Process results
        for chrom in distinct_chroms:
            processed_results.append({
                "referenceDbId": f"{studyDbId}-{chrom}",
                "referenceName": chrom,
                "referenceSetDbId": studyDbId
            })

    # Pagination
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
                "currentPage": page
            }
        },
        "result": {
            "data": paginated_results
        }
    }

    return response


@app.get("/brapi/v2/search/samples")
@app.get("/brapi/v2/search/samples")
def search_samples(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    programDbIds: List[str] = Query([], description="programDbIds")
):
    # Connect to MongoDB
    client = MongoClient(mongo_url)
    db = client[db_name]

    processed_results = []
    total_count = 0
    all_distinct_columns = set()

    for studyDbId in programDbIds:
        collection = db[studyDbId]

        # Fetch distinct column names excluding specific fields
        exclude_fields = ["_id", "CHROM", "POS", "ID",
                          "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]
        distinct_columns = [column for column in collection.distinct(
            "_id") if column not in exclude_fields]

        # Add distinct columns to the combined set
        all_distinct_columns.update(distinct_columns)

    # Process results
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

    # Pagination
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
                "currentPage": page
            }
        },
        "result": {
            "data": paginated_results
        }
    }

    # Create a mapping of types to the custom encoder
    custom_encoder = {ObjectId: MongoJSONEncoder().encode}
    return jsonable_encoder(response, custom_encoder=custom_encoder)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
