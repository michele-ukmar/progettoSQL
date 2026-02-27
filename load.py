import sqlite3
import pandas as pd
import json
from pathlib import Path

DB_PATH = "movies.db"
MOVIES_CSV = "dataset/tmdb_5000_movies.csv"
CREDITS_CSV = "dataset/tmdb_5000_credits.csv"

def populate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Caricamento file CSV...")
    df_movies = pd.read_csv(MOVIES_CSV)
    df_credits = pd.read_csv(CREDITS_CSV)

    for _, row in df_movies.iterrows():
        f_id = row['id']

        # 1. FILMS (usando filmId)
        cursor.execute("""
            INSERT OR IGNORE INTO films (
                filmId, title, originalTitle, originalLanguage, releaseDate, 
                overview, runtime, status, tagline, budget, revenue, 
                popularity, voteAverage, voteCount, homepage
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (f_id, row['title'], row['original_title'], row['original_language'], 
              row['release_date'], row['overview'], row['runtime'], row['status'], 
              row['tagline'], row['budget'], row['revenue'], row['popularity'], 
              row['vote_average'], row['vote_count'], row['homepage']))

        # 2. GENRES & filmsGenres (usando genreId e filmId)
        for g in json.loads(row['genres']):
            cursor.execute("INSERT OR IGNORE INTO genres (genreId, genreName) VALUES (?, ?)", (g['id'], g['name']))
            cursor.execute("INSERT OR IGNORE INTO filmsGenres (filmId, genreId) VALUES (?, ?)", (f_id, g['id']))

        # 3. KEYWORDS & filmsKeywords (usando keywordId)
        for k in json.loads(row['keywords']):
            cursor.execute("INSERT OR IGNORE INTO keywords (keywordId, keywordName) VALUES (?, ?)", (k['id'], k['name']))
            cursor.execute("INSERT OR IGNORE INTO filmsKeywords (filmId, keywordId) VALUES (?, ?)", (f_id, k['id']))

        # 4. PRODUCTION COMPANIES & filmsProduction (usando companyId)
        for pc in json.loads(row['production_companies']):
            cursor.execute("INSERT OR IGNORE INTO productionCompanies (companyId, companyName) VALUES (?, ?)", (pc['id'], pc['name']))
            cursor.execute("INSERT OR IGNORE INTO filmsProduction (filmId, companyId) VALUES (?, ?)", (f_id, pc['id']))

        # 5. COUNTRIES & filmsCountries (usando countryCode)
        for c in json.loads(row['production_countries']):
            cursor.execute("INSERT OR IGNORE INTO countries (countryCode, countryName) VALUES (?, ?)", (c['iso_3166_1'], c['name']))
            cursor.execute("INSERT OR IGNORE INTO filmsCountries (filmId, countryCode) VALUES (?, ?)", (f_id, c['iso_3166_1']))

        # 6. LANGUAGES & filmsLanguages (usando languageCode)
        for l in json.loads(row['spoken_languages']):
            cursor.execute("INSERT OR IGNORE INTO languages (languageCode, languageName) VALUES (?, ?)", (l['iso_639_1'], l['name']))
            cursor.execute("INSERT OR IGNORE INTO filmsLanguages (filmId, languageCode) VALUES (?, ?)", (f_id, l['iso_639_1']))

    print("🎭 Elaborazione Cast e Crew...")
    for _, row in df_credits.iterrows():
        f_id = row['movie_id']
        
        # 7. PEOPLE & CAST (usando personId e filmId)
        for member in json.loads(row['cast']):
            cursor.execute("INSERT OR IGNORE INTO people (personId, personName, gender) VALUES (?, ?, ?)", 
                           (member['id'], member['name'], member['gender']))
            cursor.execute("INSERT INTO cast (filmId, personId, characterName, castOrder) VALUES (?, ?, ?, ?)", 
                           (f_id, member['id'], member['character'], member['order']))

        # 8. DEPARTMENTS, JOBS & CREW (usando departmentId, jobId)
        for member in json.loads(row['crew']):
            cursor.execute("INSERT OR IGNORE INTO people (personId, personName, gender) VALUES (?, ?, ?)", 
                           (member['id'], member['name'], member['gender']))
            
            cursor.execute("INSERT OR IGNORE INTO departments (departmentName) VALUES (?)", (member['department'],))
            cursor.execute("SELECT departmentId FROM departments WHERE departmentName = ?", (member['department'],))
            result = cursor.fetchone()
            if result is None:
                print(f"Warning: Department '{member['department']}' not found after insert")
                continue
            dept_id = result[0]

            cursor.execute("INSERT OR IGNORE INTO jobs (departmentId, jobName) VALUES (?, ?)", (dept_id, member['job']))
            cursor.execute("SELECT jobId FROM jobs WHERE jobName = ? AND departmentId = ?", (member['job'], dept_id))
            result = cursor.fetchone()
            if result is None:
                print(f"Warning: Job '{member['job']}' not found after insert in department {dept_id}")
                continue
            job_id = result[0]

            cursor.execute("INSERT INTO crew (filmId, personId, jobId) VALUES (?, ?, ?)", (f_id, member['id'], job_id))

    conn.commit()
    conn.close()
    print("Database popolato con successo seguendo lo schema xId!")

if __name__ == "__main__":
    populate()