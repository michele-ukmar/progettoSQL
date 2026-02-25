"""File con tutto il SQL necessario a creare le tabelle nel database."""
import sqlite3
import pandas as pd
import json
from pathlib import Path

# Configurazione database
DB_PATH = "movies.db"
DATASET_PATH = Path("dataset")

def creaDB():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS films (
        filmId INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        originalTitle TEXT,
        originalLanguage TEXT,
        releaseDate DATE,
        overview TEXT,
        runtime INTEGER,
        status TEXT,
        tagline TEXT,
        budget BIGINT,
        revenue BIGINT,
        popularity REAL,
        voteAverage REAL,
        voteCount INTEGER,
        homepage TEXT
    )
    """)
    print("Tabella FILMS creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres (
        genreId INTEGER PRIMARY KEY,
        genreName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella GENRES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filmsGenres (
        filmId INTEGER REFERENCES films(filmId) ON DELETE CASCADE,
        genreId INTEGER REFERENCES genres(genreId) ON DELETE CASCADE,
        CONSTRAINT pkFilmidGenreid PRIMARY KEY (filmId, genreId)
    )
    """)
    print("Tabella FILMS_GENRES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS keywords (
        keywordId INTEGER PRIMARY KEY,
        keywordName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella KEYWORDS creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filmsKeywords (
        filmId INTEGER REFERENCES films(filmId) ON DELETE CASCADE,
        keywordId INTEGER REFERENCES keywords(keywordId) ON DELETE CASCADE,
        CONSTRAINT pkFilmidKeywordid PRIMARY KEY (filmId, keywordId)
    )
    """)
    print("Tabella FILMS_KEYWORDS creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS productionCompanies (
        companyId INTEGER PRIMARY KEY,
        companyName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella PRODUCTION_COMPANIES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filmsProduction (
        filmId INTEGER REFERENCES films(filmId) ON DELETE CASCADE,
        companyId INTEGER REFERENCES productionCompanies(companyId) ON DELETE CASCADE,
        CONSTRAINT pkFilmidCompanyid PRIMARY KEY (filmId, companyId)
    )
    """)
    print("Tabella FILMSRODUCTION creata")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        countryCode TEXT PRIMARY KEY,
        countryName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella COUNTRIES creata")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filmsCountries (
        filmId INTEGER REFERENCES films(filmId) ON DELETE CASCADE,
        countryCode TEXT REFERENCES countries(countryCode) ON DELETE CASCADE,
        CONSTRAINT pkFilmidCountrycode PRIMARY KEY (filmId, countryCode)
    )
    """)
    print("Tabella FILMSCOUNTRIES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS languages (
        languageCode TEXT PRIMARY KEY,
        languageName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella LANGUAGES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filmsLanguages (
        filmId INTEGER REFERENCES films(filmId) ON DELETE CASCADE,
        languageCode TEXT REFERENCES languages(languageCode) ON DELETE CASCADE,
        CONSTRAINT pkFilmidLanguagecode PRIMARY KEY (filmId, languageCode)
    )
    """)
    print("Tabella FILMS_LANGUAGES creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS people (
        personId INTEGER PRIMARY KEY,
        personName TEXT NOT NULL,
        gender INTEGER
    )
    """)
    print("Tabella PEOPLE creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cast (
        castId INTEGER PRIMARY KEY,
        filmId INTEGER NOT NULL REFERENCES films(filmId) ON DELETE CASCADE,
        personId INTEGER NOT NULL REFERENCES people(personId) ON DELETE CASCADE,
        characterName TEXT,
        castOrder INTEGER
    )
    """)
    print("Tabella CAST creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS departments (
        departmentId INTEGER PRIMARY KEY,
        departmentName TEXT NOT NULL UNIQUE
    )
    """)
    print("Tabella DEPARTMENTS creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        jobId INTEGER PRIMARY KEY,
        departmentId INTEGER NOT NULL UNIQUE REFERENCES departments(departmentId) ON DELETE CASCADE,
        jobName TEXT NOT NULL UNIQUE
    )
    """)
    print("✓ Tabella JOBS creata")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crew (
        crewId INTEGER PRIMARY KEY,
        filmId INTEGER NOT NULL REFERENCES films(filmId) ON DELETE CASCADE,
        personId INTEGER NOT NULL REFERENCES people(personId) ON DELETE CASCADE,
        jobId INTEGER NOT NULL REFERENCES jobs(jobId) ON DELETE CASCADE
    )
    """)
    print("✓ Tabella CREW creata")
    
    conn.commit()
    conn.close()
    print("Tutte le tabelle sono state create con successo!")
if __name__ == "__main__":
    creaDB()