# Lab 2 — MongoDB with Python (PyMongo)

![Python](https://img.shields.io/badge/Python-3.13-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green)
![PyMongo](https://img.shields.io/badge/PyMongo-4.x-yellow)
![Pandas](https://img.shields.io/badge/Pandas-2.x-orange)

## Overview
Analytics pipeline built on the MongoDB `sample_mflix` dataset using PyMongo and Pandas.

## Dataset
MongoDB Atlas sample dataset — `sample_mflix`
Collections: `movies`, `comments`, `users`, `theaters`, `sessions`

## What this lab covers
- MongoDB connection & indexing (TEXT + compound indexes)
- Data ingestion with `insert_many()`
- Aggregation pipelines (`$match`, `$unwind`, `$group`, `$lookup`)
- Data enrichment with `bulk_write()`
- CSV export with Pandas
- JSON Schema validation
- Reusable analytics pipeline function

## Project Structure
```
lab2_mongodb/
├── mflix_pipeline_answers.py
├── requirements.txt
├── .gitignore
├── README.md
└── .env  ← NOT pushed (credentials)
```

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/ouhaddousara/Lab2_mongoDB.git
cd Lab2_mongoDB
```

### 2. Create virtual environment
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
Create a `.env` file in the root directory:
```
MONGO_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
```

### 5. Run the script
```bash
python mflix_pipeline_answers.py
```

## Expected Output
- Collections listed in terminal
- Indexes created on `movies`
- 3 new movies inserted
- Top-rated movies by genre displayed
- Top 10 commenters displayed
- `monthly_movie_releases.csv` exported
- Schema validation tested
- Full pipeline executed

## Requirements
- Python 3.13+
- MongoDB Atlas account with `sample_mflix` loaded
- See `requirements.txt` for Python dependencies

## Author
Sara Ouhaddou — Data Engineering Lab, 2026
```

---

Commit message :
```
docs: fix README formatting and badges
