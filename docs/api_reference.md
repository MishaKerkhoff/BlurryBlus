# Apple Music Newsletter API Reference

This reference documents the public-facing APIs, functions, classes, and web components that power the Apple Music newsletter pipeline. It is organized by module and includes usage notes and runnable examples where practical.

## Airflow Orchestration

### DAG `apple_music_album_extraction`
Source: `dags/apple_music_album_extraction.py`

**Purpose**: Pull recent trending albums from the Apple Music API, store enriched metadata in PostgreSQL, and refresh a reporting view.

#### Task Topology
- `create_table`: Creates the `apple_music_album_releases` table if needed.
- `fetch_playlist_data`: Downloads curated playlists and distills song metadata.
- `fetch_album_details`: Looks up each album to gather release information.
- `store_album_data`: Upserts albums into PostgreSQL.
- `create_view`: Refreshes the `v_weekly_new_releases` view for downstream consumers.

Trigger with: `airflow dags trigger apple_music_album_extraction`

#### Public Functions

##### `generate_album_id(album_data: dict) -> str`
Deterministically hashes the album release date, artist, and URL to create a unique primary key.

Example:
```python
from dags.apple_music_album_extraction import generate_album_id

album_id = generate_album_id({
    "release_date": "2024-11-01",
    "artist": "Doe & Co",
    "url": "https://music.apple.com/us/album/example"
})
print(album_id)
```

##### `get_album_artwork(artwork_data: dict | None) -> str | None`
Normalizes Apple artwork URLs to 600x600 thumbnails, returning `None` when the API omits artwork.

##### `get_jwt_token() -> str`
Reads the current Apple Music JWT from `/opt/airflow/secrets/apple_jwt.txt`. Requires the token file to exist; ensure `apple_music_token_generation` runs first.

##### `get_headers() -> dict[str, str]`
Composes an HTTP `Authorization` header using the JWT from `get_jwt_token()`. Useful for one-off API calls:
```python
import requests
from dags.apple_music_album_extraction import get_headers

response = requests.get(
    "https://api.music.apple.com/v1/catalog/us/playlists/pl.xxx",
    headers=get_headers(),
    timeout=10,
)
response.raise_for_status()
```

##### `fetch_playlist_data(**kwargs) -> list[dict]`
Airflow task callable that gathers songs from the curated playlists in `PLAYLISTS`, pushes the reduced payload to XCom (`key='reduced_songs'`), and returns it.

Manual invocation outside Airflow requires a `ti` shim that implements `xcom_push`:
```python
from dags.apple_music_album_extraction import fetch_playlist_data

class DummyTI:
    def xcom_push(self, key, value):
        print(f"[XCom] {key}: {len(value)} records")

fetch_playlist_data(ti=DummyTI())
```

##### `fetch_album_details(**kwargs) -> list[dict]`
Enriches songs with album metadata by calling the Apple Music search endpoint, filters the results to recent albums, pushes them to XCom (`key='recent_albums'`), and returns the filtered list.

##### `store_album_data(**kwargs) -> None`
Persists albums produced by `fetch_album_details` into PostgreSQL via `PostgresHook`. Automatically handles duplicate URLs by skipping conflicts.

### DAG `apple_music_token_generation`
Source: `dags/apple_music_token_generation.py`

Generates and refreshes Apple Music JWT tokens every Friday at 08:30 ET and publishes them as a dataset for dependent DAGs.

#### Public Function
##### `check_and_generate_jwt(**kwargs) -> str`
Wraps `AppleAuthManager` to return a valid JWT, creating one when the on-disk token is missing or expired. The callable pushes the token to XCom (`key='apple_jwt'`).

Trigger with: `airflow dags trigger apple_music_token_generation`

## Authentication Helper

### Class `AppleAuthManager`
Source: `dags/helpers/apple_auth.py`

Manages the Apple Music JWT lifecycle. Initialize with Apple developer credentials and file paths:
```python
from dags.helpers.apple_auth import AppleAuthManager

manager = AppleAuthManager(
    team_id="TEAMID123",
    key_id="KEYID456",
    private_key_path="/secrets/AuthKey.p8",
    jwt_store_path="/secrets/apple_jwt.txt",
)
token = manager.get_valid_token()
```

#### Public Method
- `get_valid_token() -> str`: Returns a valid JWT, reusing the stored token when the cached expiration timestamp is still in the future. Generates and persists a new ES256 token otherwise.

Environment prerequisites:
- `APPLE_TEAM_ID` and `APPLE_KEY_ID` must be set for the token-generation DAG.
- `private_key_path` must point to an Apple Music private key file.
- The directory containing `jwt_store_path` must be writable by Airflow.

## Email Personalization DAG

### DAG `Personalized_email_generation`
Source: `dags/personalized_email_generation.py`

Listens to the `v_weekly_new_releases` dataset, ranks albums per subscriber, and sends personalized HTML newsletters through Brevo SMTP.

Trigger with: `airflow dags trigger Personalized_email_generation`

#### Public Functions

- `get_active_subscribers() -> list[dict]`: Retrieves active subscribers from PostgreSQL. When `TEST_MODE` is `True`, limits results to `user_id = 2`.

- `generate_album_blurb(artist: str, album: str) -> str`: Produces a random short blurb for featured albums.

- `fetch_this_weeks_albums() -> list[dict]`: Pulls the latest albums from `v_weekly_new_releases`, including genre, track count, and Apple Music URL.

- `generate_email_content(**kwargs) -> dict[str, str]`: Ranks albums for each subscriber using `calculate_album_score`, renders personalized HTML with Jinja2, pushes the resulting mapping (`email -> html`) to XCom, and returns it.

- `create_personalized_email_html(subscriber, featured, others, run_date) -> str`: Renders the newsletter HTML given a subscriber profile and curated album lists.

- `send_email_python(**kwargs) -> None`: Sends the HTML emails via Brevo SMTP, raising if any delivery fails.

**SMTP configuration**: Set `BREVO_LOGIN`, `BREVO_PASSWORD`, and ensure network access to `smtp-relay.brevo.com:587`. The DAG defaults to `from_email = 'nathanialc17@gmail.com'`; override as needed.

## Recommendation Engine Utilities

Source: `dags/utils/recommendation_weights.py`

These helpers power the personalization scores. Functions expect a live Airflow/PostgreSQL environment when retrieving data.

- `get_known_artists() -> list[str]`: Returns distinct artist names from `apple_music_album_releases`.
- `clean_user_artist_input(user_input, known_artists=None, threshold=70) -> str | None`: Fuzzily aligns user-supplied artist names to known data.
- `is_artist_match(user_artist, album_artist, threshold=70) -> bool`: Scores similarity between two artist strings.
- `genre_score(album_genres, user_genres, max_points=30) -> int`: Jaccard-based overlap scoring.
- `calculate_album_score(album: dict, user_prefs: dict, known_artists=None) -> int`: Composite scoring that blends genres, favorite artists, related artists, and album length preferences.

Example:
```python
from dags.utils.recommendation_weights import calculate_album_score

album = {"artist": "Doe & Co", "genre": "Alternative, Indie Rock", "track_count": 12}
user = {"genres": ["Alternative", "Rock"], "favorite_artist": "Doe & Company", "album_length": "medium"}

score = calculate_album_score(album, user)
print(score)
```

## Subscriber Intake Web App

### Flask Application
Source: `user_form_app/app.py`

Routes:
- `GET /`: Renders `templates/index.html`, a signup form populated with `COMMON_GENRES`.
- `GET /error`: Displays `templates/error.html`, showing a friendly message passed via the `message` query string.
- `POST /submit`: Validates form data, processes favorite artists via `process_user_artists`, writes the record to `user_preferences`, and redirects to `/success`. On validation or database errors, the route redirects to `/error`.
- `GET /success`: Shows `templates/success.html`, indicating when the next Friday newsletter will arrive.

Database configuration uses environment variables: `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS`. Default placeholders should be overwritten in production.

Quick start:
```bash
export FLASK_APP=user_form_app.app
export DB_HOST=localhost
export DB_NAME=music
export DB_USER=postgres
export DB_PASS=secret
flask run --host=0.0.0.0 --port=5000
```

## Related Artist Discovery Utilities

Source: `user_form_app/utils/related_artist.py`

These functions expand user-supplied favorite artists into related recommendations using the Apple Music API with a database fallback.

- `get_db_connection() -> psycopg2.extensions.connection`: Opens a PostgreSQL connection using hard-coded defaults (replace with environment variables before production use).
- `get_apple_music_token() -> str`: Reads a JWT from `/opt/airflow-docker/secrets/apple_jwt.txt`.
- `get_related_artists_from_apple_music(artist_name: str) -> list[str]`: Finds similar artists via Apple Music search and the `view/similar-artists` endpoint.
- `extract_similar_artists_from_response(similar_data: dict, original_artist_name: str) -> list[str]`: Parses the Apple API response into artist names, excluding the original input.
- `get_related_artists_by_genre(artist_id: str, artist_name: str, headers: dict) -> list[str]`: Fallback that queries artists in the same genre when the similar-artists endpoint fails.
- `get_related_artists_from_database(artist_name: str) -> list[str]`: Uses historical release data to infer peers when external APIs are unavailable.
- `fuzzy_deduplicate_artists(artists: list[str], threshold: int = 80) -> list[str]`: Removes near-duplicate artist names using RapidFuzz.
- `get_related_artists(artist_name: str, limit: int = 10) -> list[str]`: High-level fetcher that attempts the Apple Music API first, then falls back to the database.
- `calculate_fair_distribution(num_artists: int, total_slots: int) -> list[int]`: Allocates how many related artists to request per favorite artist.
- `process_user_artists(artist_input: str, max_related: int = 20) -> tuple[list[str], list[str]]`: Splits the user input list, fetches related artists for each, deduplicates them, and returns both the cleaned favorites and derived related artists.

Example:
```python
from user_form_app.utils.related_artist import process_user_artists

favorites, related = process_user_artists("SZA, Phoebe Bridgers", max_related=8)
print(f"Favorites: {favorites}")
print(f"Related: {related}")
```

**Rate limiting**: `process_user_artists` sleeps briefly between API calls; consider longer delays for production workloads.

## Operational Notes

- Ensure Airflow connections `postgres_default` and environment secrets (`BREVO_*`, Apple credentials) are configured before triggering DAGs.
- The dataset dependency between `apple_music_token_generation` and `apple_music_album_extraction` requires Airflow 2.4+.
- The web app and DAGs share database tables (`user_preferences`, `apple_music_album_releases`, `v_weekly_new_releases`); keep schemas synchronized when evolving the system.
