from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile, File
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from models import Status, Metadata, ApiResponse, Variant, QualSummary
from database import get_db, get_dburl, execute_query_to_dataframe
from utils import read_query, read_vcf, format_sql_query, extract_and_upload_metadata
from fastapi.middleware.cors import CORSMiddleware
import httpx
from fastapi import APIRouter, HTTPException
import traceback
from fastapi import FastAPI, APIRouter, HTTPException, Depends
import httpx
import os
from dotenv import load_dotenv
from starlette.responses import RedirectResponse

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
variant_query = read_query('./database_queries/variants_query.sql')
quality_summaries_query = read_query(
    './database_queries/qual_summaries_query.sql')
variantsetsearch_query = read_query(
    './database_queries/variantsets.sql')
availableFormats_query = read_query(
    './database_queries/availableFormats.sql'
)
distinctDBId = read_query(
    './database_queries/distinctDB_Id.sql'
)
getRefernces_query = read_query(
    './database_queries/getReferences.sql'
)
getSamples_query = read_query('./database_queries/getSamples.sql')

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


@ app.get("/brapi/v2/variants", response_model=ApiResponse)
def get_variant(
    chrom: str = Query(..., description="Chromosome of the variant"),
    ref: str = Query(...,
                     description="ref of the variant on the chromosome"),
    db: Session = Depends(get_db),
    studyDbIds: str = Query(..., description="studyDbIds"),
):
    custom_query = variant_query.replace("%(table_name)s", str(studyDbIds))
    custom_query = custom_query.replace('%(chrom)s', str(chrom))
    custom_query = custom_query.replace('%(ref)s', str(ref))
    print(custom_query)

    sql_query = text(custom_query)
    result = execute_query_to_dataframe(sql_query)

    if result is None or result.empty:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="Variant not found")]), result={})

    variants = []
    for _, row in result.iterrows():
        variant = Variant(
            chromosome=row['CHROM'],
            position=int(row['POS']),
            ref=row['REF'],
            alt=row['ALT'],
            quality=float(row['QUAL']),
            filter=row['FILTER'],
            info=row['INFO']
        )
        variants.append(variant)

    return ApiResponse(
        metadata=Metadata(
            status=[Status(messageType="INFO",
                           message="Variants retrieved successfully")]
        ),
        result={"variants": variants}
    )


@ app.get("/brapi/v2/qualsummaries", response_model=ApiResponse)
def get_quality_summaries(db: Session = Depends(get_db), studyDbIds: str = Query(..., description="studyDbIds")):
    custom_query = quality_summaries_query.replace(
        "%(table_name)s", str(studyDbIds))
    print(custom_query)
    result = execute_query_to_dataframe(
        custom_query)
    print(result)
    if result is None:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="No quality summaries found")]), result=[])
    summaries = [
        QualSummary(chromosome=row['CHROM'], quality=float(row['averagequal']))
        for _, row in result.iterrows()
    ]
    print(summaries)
    return ApiResponse(
        metadata=Metadata(status=[Status(
            messageType="INFO", message="Quality summaries retrieved successfully")]),
        result=summaries
    )


@ app.post("/brapi/v2/upload_data/")
async def upload_data(file: UploadFile = File(...), db: Session = Depends(get_dburl)):
    file_location = f"static/{file.filename}"
    print(file_location)

    # Save the uploaded file to the static directory
    with open(file_location, "wb") as buffer:
        buffer.write(await file.read())

    try:
        # Process the file to extract and upload metadata
        extract_and_upload_metadata(file_location, db)

    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Error processing file {file.filename}: {str(e)}")

    return {"message": f"Metadata for {file.filename} uploaded successfully"}


@ app.get("/brapi/v2/search/variantsets")
def search_variantsets(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    db: Session = Depends(get_db)
):
    # Use the function from database.py to get the DataFrame
    df_variantsearch = execute_query_to_dataframe(variantsetsearch_query)
    df_availbleformat = execute_query_to_dataframe(availableFormats_query)

    available_formats = []
    for _, row in df_availbleformat.iterrows():
        available_formats.append({
            'data_format': row['data_format'],
            'file_format': row['file_format'],
            'file_url': row['file_url']
        })
    # Check if the DataFrame is empty
    if df_variantsearch.empty:
        return ApiResponse(metadata=Metadata(status=[Status(messageType="ERROR", message="No variant sets found")]), result=[])
    # Calculate pagination
    total_count = len(df_variantsearch)
    total_pages = (total_count + page_size - 1) // page_size
    start = page * page_size
    end = start + page_size

    # Paginate the DataFrame
    paginated_df = df_variantsearch.iloc[0:end]

    # Process results to the desired format
    processed_results = []
    for _, row in paginated_df.iterrows():
        processed_results.append({
            "callSetCount": row['call_set_count'],
            "referenceSetDbId": row['reference_set_db_id'],
            "studyDbId": row['study_db_id'],
            "variantCount": row['variant_count'],
            "variantSetDbId": row['variant_set_db_id'],
            "variantSetName": row['variant_set_name'],
            # Assuming metadata_fields is stored as JSON string
            "metadataFields": row['metadata_fields'],


        })

    # Prepare the response
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
            "availableFormats":   available_formats,
            "data": processed_results
        }
    }
    return response


@ app.get("/brapi/v2/search/references")
def search_references(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    studyDbIds: list[str] = Query([], description="List of studyDbIds"),
    db: Session = Depends(get_db)
):

    subqueries = []

    # Loop through each table name and construct the subquery using the base query
    for table_name in studyDbIds:
        subquery = getRefernces_query.replace("%(table_name)s", table_name)
        subqueries.append(subquery)

    # Combine all subqueries with UNION ALL
    combined_query = " UNION ALL ".join(subqueries)

    # Read and format the SQL query from the file
    # sql_query = format_sql_query(
    #     getRefernces_query, placeholders, 'studyDbIds')

    df_references = execute_query_to_dataframe(combined_query)
    total_count = len(df_references)
    total_pages = (total_count + page_size - 1) // page_size
    start = page * page_size
    end = start + page_size

    processed_results = []
    for _, row in df_references.iloc[start:end+1].iterrows():
        processed_results.append({
            "referenceDbId": row['referencedbid'],
            "referenceName": row['referencename'],
            "referenceSetDbId": row['referencesetdbid']

        })

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
            "data": processed_results
        }
    }
    return response


@ app.get("/brapi/v2/search/samples")
def search_references(
    page: int = Query(0, description="Page number"),
    page_size: int = Query(10, description="Number of items per page"),
    programDbIds: list[str] = Query(200, description="programDbIds"),
    db: Session = Depends(get_db)
):
    print(programDbIds)
    subqueries = []

    # Loop through each table name and construct the subquery using the base query
    for table_name in programDbIds:
        subquery = getSamples_query.replace("%(table_name)s", table_name)
        subqueries.append(subquery)

    # Combine all subqueries with UNION ALL
    combined_query = " UNION ALL ".join(subqueries)

    print(combined_query)
    df_samples = execute_query_to_dataframe(combined_query)
    values_to_remove = ["CHROM", "POS", "ID", "REF",
                        "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]
    df_samples = df_samples.loc[~df_samples['column_name'].isin(
        values_to_remove)]

    total_count = len(df_samples)
    total_pages = (total_count + page_size - 1) // page_size
    start = page * page_size
    end = start + page_size
    processed_results = []
    for _, row in df_samples.iterrows():
        processed_results.append({
            "additionalInfo": {},
            "germplasmDbId": f"{row['table_name']}-{row['column_name']}",
            "sampleDbId": row['table_name'],
            "sampleName": row['column_name'],
            "studyDbId": row['table_name']

        })

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
            "data": processed_results
        }
    }
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
