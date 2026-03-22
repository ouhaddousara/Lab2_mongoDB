"""
Lab2 MongoDB with Python (PyMongo)
Dataset  : sample_mflix
"""

import os
import random
import traceback
from datetime import datetime as dt

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import DESCENDING, TEXT, MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, WriteError

# On charge les variables du fichier .env
load_dotenv()


# On utilise ces imports pour:
"""
os          → lire la variable MONGO_URI depuis .env
random      → générer les sentiments aléatoires (Q5)
pandas      → transformer les résultats en DataFrame
ObjectId    → créer des IDs MongoDB valides (Q7)
load_dotenv → charger le fichier .env automatiquement

"""





###  QUESTION 1 : Connection, Inspection & Indexing  ###

def get_db():
    """
    Establishes and returns a connection to the sample_mflix database.
 
    Why centralize here?
    - Single point of configuration → easy to change cluster later
    - serverSelectionTimeoutMS prevents 30s hang on bad credentials
    - ping() forces an actual connection attempt
    """
    client = MongoClient(
        os.getenv("MONGO_URI"),
        serverSelectionTimeoutMS=5000,
        tlsAllowInvalidCertificates=True
    )

    # Force connection validation immediately
    client.admin.command("ping")
    return client["sample_mflix"]
 
#Connection test
try:
    db = get_db()
    print("Successfully connected to sample_mflix")
except ConnectionFailure as e:
    print(f"Connection failed : {e}")
    exit(1)

"""
try → try to connect
    if it works → continue the script
except → if it fails → display the error and stop everything
"""

#List all collections
collections = db.list_collection_names()
print("\nAvailable collections:")
for col in sorted(collections):
    print(f"  - {col}")

#Total document count in movies
movies = db["movies"]
count = movies.count_documents({})
print(f"\nTotal documents in movies collection: {count:,}")

#TEXT index on title + plot
movies.create_index(
    [("title", TEXT), ("plot", TEXT)],
    name="text_title_plot"
)
print("\nTEXT index created on (title, plot)")

#Compound DESCENDING index on year + imdb.rating
movies.create_index(
    [("year", DESCENDING), ("imdb.rating", DESCENDING)],
    name="compound_year_rating_desc"
)
print("Compound index created on (year DESC, imdb.rating DESC)")

#Verify active indexes
print("\nActive indexes on movies collection:")
for idx in movies.list_indexes():
    print(f"  - {idx['name']}: {dict(idx['key'])}")





###  QUESTION 2 : Data Ingestion  ###

#Define the list of new movies to insert
new_movies = [
    {

        "title": "The Algorithm",
        "year": 2023,
        "genres": ["Thriller", "Science Fiction"],
        "plot": "A rogue AI starts making decisions that alter the course of human history.",
        "released": dt(2023, 3, 15),
        "imdb": {"rating": 8.4, "votes": 125000},
        "tomatoes": {"viewer": {"rating": 4.2}}
    },
    {
        "title": "Sahara Blues",
        "year": 2022,
        "genres": ["Drama", "Music"],
        "plot": "A Moroccan musician travels across the Sahara to preserve ancestral melodies.",
        "released": dt(2022, 7, 22),
        "imdb": {"rating": 7.9, "votes": 43000},
        "tomatoes": {"viewer": {"rating": 3.8}}
    },
    {
        "title": "Zero Gravity",
        "year": 2024,
        "genres": ["Adventure", "Science Fiction"],
        "plot": "Engineers must repair a broken satellite before it triggers a global blackout.",
        "released": dt(2024, 1, 10),
        "imdb": {"rating": 8.1, "votes": 87000},
        "tomatoes": {"viewer": {"rating": 4.0}}
    }
]

#Insert all movies in a single operation
result = movies.insert_many(new_movies, ordered=True)
#Print inserted document IDs
print(f"\n {len(result.inserted_ids)} movies inserted successfully")

#We display each ID to confirm that the documents are properly saved
print("Inserted document IDs:")
for doc_id in result.inserted_ids:
    print(f"  - {doc_id}")





###  QUESTION 3 : Aggregation Pipeline – Top-Rated Movies by Genre  ###



#Define the aggregation pipeline
pipeline_q3 = [

    #Step 1 : Filter movies with imdb.rating >= 8.0 and genres field
    {
        "$match": {
            "imdb.rating": {"$gte": 8.0},   
            "genres": {"$exists": True, "$not": {"$size": 0}}  # genres exists and is not empty
        }
    },

    #Step 2 : Unwind the genres array
    {
        "$unwind": {
            "path": "$genres",
            "preserveNullAndEmptyArrays": False  # ignore documents without genres
        }
    },

    #Step 3 : Group by genre and compute stats
    {
        "$group": {
            "_id": "$genres",
            "avg_rating": {"$avg": "$imdb.rating"},
            "movies_list": {
                "$push": {
                    "title": "$title",
                    "rating": "$imdb.rating",
                    "year": "$year"
                }
            }
        }
    },

    #Step 4 : Round avg_rating and keep only top 5 movies per genre
    {
        "$addFields": {
            "avg_rating": {"$round": ["$avg_rating", 2]},
            "top_movies": {"$slice": ["$movies_list", 5]}
        }
    },

    #Step 5 : Sort by average rating descending
    {"$sort": {"avg_rating": -1}},

    #Step 6 : Project final output fields
    {
        "$project": {
            "_id": 0,           
            "genre": "$_id",    
            "avg_rating": 1,
            "top_movies": 1
        }
    }
]

#Execute the pipeline
results_q3 = list(movies.aggregate(pipeline_q3, allowDiskUse=True))

#Convert results to a Pandas DataFrame and display
df_q3 = pd.DataFrame(results_q3)
print("\nTop-Rated Movies by Genre :\n")
print(df_q3[["genre", "avg_rating"]].to_string(index=False))





###  Question 4: Advanced Analytics– Most Active Commenters  ###


#Access the comments collection
comments = db["comments"]

#Define the aggregation pipeline with $lookup
pipeline_q4 = [

    #Step 1 : Join comments with movies collection
    {
        "$lookup": {
            "from": "movies",        
            "localField": "movie_id", 
            "foreignField": "_id",    
            "as": "movie_info"        
        }
    },

    #Step 2 : Unwind the lookup result
    {
        "$unwind": {
            "path": "$movie_info",
            "preserveNullAndEmptyArrays": True
        }
    },

    #Step 3 : Group by commenter name
    {
        "$group": {
            "_id": "$name",
            "comment_count": {"$sum": 1},
            "movie_title": {"$first": "$movie_info.title"}
        }
    },

    #Step 4 : Sort by comment count descending
    {"$sort": {"comment_count": -1}},

    #Step 5 : Limit to top 10 commenters
    {"$limit": 10},

    #Step 6 : Project final output fields
    {
        "$project": {
            "_id": 0,
            "commenter_name": "$_id",
            "comment_count": 1,
            "movie_title": 1
        }
    }
]

#Execute the pipeline
results_q4 = list(comments.aggregate(pipeline_q4, allowDiskUse=True))

#Convert results to a Pandas DataFrame and display
df_q4 = pd.DataFrame(results_q4)
print("\nTop 10 Most Active Commenters :\n")
print(df_q4.to_string(index=False))





###  Question 5: Data Enrichment  ###


#Define the possible sentiment values
sentiments = ["positive", "neutral", "negative"]

#Fetch only the _id of the first 200 comments
first_200 = list(comments.find({}, {"_id": 1}).limit(200))
print(f"\nEnriching {len(first_200)} comments with sentiment field...")

#Build bulk update operations
bulk_ops = [
    UpdateOne(
        {"_id": doc["_id"]},                        
        {"$set": {"sentiment": random.choice(sentiments)}}  
    )
    for doc in first_200 
]

#Execute all updates in a single batch
# Single network round-trip instead of 200 separate update calls
result = comments.bulk_write(bulk_ops, ordered=False)
print(f"Documents modified: {result.modified_count}")

#Verify the enrichment
enriched_count = comments.count_documents({"sentiment": {"$exists": True}})
print(f"Total comments with sentiment field: {enriched_count}")

#Show sentiment distribution
print("\nSentiment distribution:")
for s in sentiments:
    n = comments.count_documents({"sentiment": s})
    print(f"  {s:10s} : {n}")





###  Question6: Analytics Export– Monthly MovieRelease Trend  ###


#Define the aggregation pipeline
pipeline_q6 = [

    #Step 1 : Filter documents with a valid date field
    {
        "$match": {
            "released": {"$exists": True, "$type": "date"}
        }
    },

    #Step 2 : Group by year and month
    {
        "$group": {
            "_id": {
                "year": {"$year": "$released"},   
                "month": {"$month": "$released"}  
            },
            "movie_count": {"$sum": 1}
        }
    },

    #Step 3 : Sort chronologically
    {
        "$sort": {
            "_id.year": 1,   
            "_id.month": 1
        }
    },

    #Step 4 : Flatten nested _id into top-level fields
    {
        "$project": {
            "_id": 0,
            "year": "$_id.year",
            "month": "$_id.month",
            "movie_count": 1
        }
    }
]

#Execute the pipeline
results_q6 = list(movies.aggregate(pipeline_q6, allowDiskUse=True))

#Convert to Pandas DataFrame
df_q6 = pd.DataFrame(results_q6)

#Create a readable year_month column
df_q6["year_month"] = (
    pd.to_datetime(df_q6[["year", "month"]].assign(day=1))
    .dt.strftime("%Y-%m")
)

#Export to CSV
output_path = "monthly_movie_releases.csv"
df_q6.to_csv(output_path, index=False, encoding="utf-8")
print(f"\nCSV exported → {output_path}")
print(f"Period covered: {df_q6['year_month'].min()} to {df_q6['year_month'].max()}")
print(f"Total year-month combinations: {len(df_q6)}")
print(f"\nLast 10 rows:\n{df_q6.tail(10).to_string(index=False)}")





###  Question 7: Schema Validation  ###



#Drop existing collection for clean reset
if "validated_reviews" in db.list_collection_names():
    db.drop_collection("validated_reviews")
    print("\nExisting validated_reviews dropped (reset)")

#Define the JSON Schema validator
validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["movie_id", "user_id", "rating", "review_text"],
        "properties": {
            "movie_id": {
                "bsonType": "objectId",
                "description": "Must be a valid ObjectId — required"
            },
            "user_id": {
                "bsonType": "objectId",
                "description": "Must be a valid ObjectId — required"
            },
            "rating": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 10,
                "description": "Integer between 1 and 10 — required"
            },
            "review_text": {
                "bsonType": "string",
                "minLength": 1,
                "description": "Non-empty string — required"
            }
        }
    }
}

#Create the collection with the validator
db.create_collection(
    "validated_reviews",
    validator=validator,
    validationAction="error",
    validationLevel="strict"
)
print("Collection 'validated_reviews' created with JSON Schema validator\n")

#Access the new collection
validated_reviews = db["validated_reviews"]

#Test 1 : Insert a VALID document
valid_doc = {
    "movie_id": ObjectId(),
    "user_id": ObjectId(),
    "rating": 8,           
    "review_text": "Excellent film, highly recommended!"
}

try:
    validated_reviews.insert_one(valid_doc)
    print("VALID document inserted successfully")
except WriteError as e:
    print(f"Unexpected error on valid doc: {e}")

#Test 2 : Insert an INVALID document
invalid_doc = {
    "movie_id": ObjectId(),
    "user_id": ObjectId(),
    "rating": 15,          
    "review_text": "This should be rejected by the validator"
}

try:
    validated_reviews.insert_one(invalid_doc)
    print("WARNING: Invalid document was inserted (validation not working!)")
except WriteError as e:
    print(f"INVALID document correctly rejected:")
    print(f"   → {e.details.get('errmsg', str(e))}")





###  Question 8: Final Challenge– Reusable Pipeline Function  ###


def run_mflix_analytics_pipeline():
    """
    Reusable analytics pipeline for the sample_mflix dataset.
    Runs all key analyses and exports results.
    """

    #Print start message with current date
    start_time = dt.now()
    print(f"Pipeline started on {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        #Connect to the database
        db = get_db()
        movies_col = db["movies"]
        comments_col = db["comments"]
        print("\nConnected to sample_mflix")

        #Part 1 : Top-Rated Movies by Genre (Q3)
        print("\n[1/3] Computing top-rated movies by genre...")
        pipeline_genres = [
            {"$match": {"imdb.rating": {"$gte": 8.0}, "genres": {"$exists": True}}},
            {"$unwind": {"path": "$genres", "preserveNullAndEmptyArrays": False}},
            {"$group": {
                "_id": "$genres",
                "avg_rating": {"$avg": "$imdb.rating"},
                "movies_list": {"$push": {
                    "title": "$title",
                    "rating": "$imdb.rating",
                    "year": "$year"
                }}
            }},
            {"$addFields": {
                "avg_rating": {"$round": ["$avg_rating", 2]},
                "top_movies": {"$slice": ["$movies_list", 5]}
            }},
            {"$sort": {"avg_rating": -1}},
            {"$project": {"_id": 0, "genre": "$_id", "avg_rating": 1, "top_movies": 1}}
        ]

        df_genres = pd.DataFrame(
            list(movies_col.aggregate(pipeline_genres, allowDiskUse=True))
        )
        print(df_genres[["genre", "avg_rating"]].to_string(index=False))

        #Part 2 : Most Active Commenters (Q4)
        print("\n[2/3] Identifying most active commenters...")
        pipeline_commenters = [
            {"$lookup": {
                "from": "movies",
                "localField": "movie_id",
                "foreignField": "_id",
                "as": "movie_info"
            }},
            {"$unwind": {"path": "$movie_info", "preserveNullAndEmptyArrays": True}},
            {"$group": {
                "_id": "$name",
                "comment_count": {"$sum": 1},
                "movie_title": {"$first": "$movie_info.title"}
            }},
            {"$sort": {"comment_count": -1}},
            {"$limit": 10},
            {"$project": {
                "_id": 0,
                "commenter_name": "$_id",
                "comment_count": 1,
                "movie_title": 1
            }}
        ]

        df_commenters = pd.DataFrame(
            list(comments_col.aggregate(pipeline_commenters, allowDiskUse=True))
        )
        print(df_commenters.to_string(index=False))

        #Part 3 : Export Monthly Release Trend to CSV (Q6)
        print("\n[3/3] Exporting monthly release trend...")
        pipeline_monthly = [
            {"$match": {"released": {"$exists": True, "$type": "date"}}},
            {"$group": {
                "_id": {
                    "year": {"$year": "$released"},
                    "month": {"$month": "$released"}
                },
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {"_id.year": 1, "_id.month": 1}},
            {"$project": {
                "_id": 0,
                "year": "$_id.year",
                "month": "$_id.month",
                "movie_count": 1
            }}
        ]

        df_monthly = pd.DataFrame(
            list(movies_col.aggregate(pipeline_monthly, allowDiskUse=True))
        )
        df_monthly["year_month"] = (
            pd.to_datetime(df_monthly[["year", "month"]].assign(day=1))
            .dt.strftime("%Y-%m")
        )
        csv_path = "monthly_movie_releases.csv"
        df_monthly.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"  → Exported {len(df_monthly)} rows to {csv_path}")

    except Exception as e:
        print(f"\nPipeline error: {e}")
        traceback.print_exc()

    finally:
        #Print completion message with current date
        end_time = dt.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Pipeline completed on {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration:.2f} seconds")




#  MAIN : Execute all questions sequentially #

if __name__ == "__main__":

    print("Lab 2 — MongoDB sample_mflix Analytics Pipeline")

    # Question 8 → fonction pipeline finale
    run_mflix_analytics_pipeline()

    print("\nAll questions completed successfully!")