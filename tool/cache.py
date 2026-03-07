import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests_cache


class CacheManager:
    """Orchestrates all cache instances and provides a unified interface."""

    def __init__(self, cache_dir="cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Initialize all cache instances
        self.github_cache = GitHubCache(cache_dir)
        self.package_cache = PackageAnalysisCache(cache_dir)
        self.commit_comparison_cache = CommitComparisonCache(cache_dir)
        self.user_commit_cache = UserCommitCache(cache_dir)
        self.extracted_deps_cache = DependencyExtractionCache(cache_dir)

    def _setup_requests_cache(self, cache_name="http_cache"):
        requests_cache.install_cache(
            cache_name=str(self.cache_dir / f"{cache_name}_cache"),
            backend="sqlite",
            expire_after=7776000,  # 90 days
            allowable_codes=(200, 301, 302, 404),
        )

    def clear_all_caches(self, older_than_days=None):
        """Clear all caches"""
        self.github_cache.clear_cache(older_than_days)
        self.package_cache.clear_cache(older_than_days)
        self.commit_comparison_cache.clear_cache(older_than_days)
        self.user_commit_cache.clear_cache(older_than_days)
        self.extracted_deps_cache.clear_cache(older_than_days)


class Cache:
    """Abstract base class for SQLite-backed caches with automatic schema versioning."""

    def __init__(self, cache_dir="cache", db_name="cache.db"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.cache_dir / db_name
        self._execute_query(
            """
            CREATE TABLE IF NOT EXISTS schema_signatures (
                table_name TEXT PRIMARY KEY,
                signature TEXT,
                last_updated TIMESTAMP
            )
        """
        )
        self.setup_db()

    def setup_db(self):
        """Initialize SQLite database - should be implemented by subclasses"""
        raise NotImplementedError

    def _execute_query(self, query, params=None):
        """Execute SQLite query with proper connection handling"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()
        try:
            if params:
                c.execute(query, params)
            else:
                c.execute(query)
            conn.commit()
            return c.fetchall()
        finally:
            conn.close()

    def _generate_schema_signature(self, schema):
        """Generate a unique signature for a schema definition"""
        # Removing whitespace and convert to lowercase to ignore formatting differences
        normalized_schema = " ".join(schema.lower().split())
        return hashlib.md5(normalized_schema.encode()).hexdigest()

    def _check_and_update_table(self, table_name, schema):
        """Check if table exists with correct schema, otherwise recreate it"""
        new_signature = self._generate_schema_signature(schema)
        current_signature = self._get_table_signature(table_name)

        if current_signature is None:
            # Table doesn't exist or isn't versioned yet
            print(f"Creating new table: {table_name}")
            self._create_new_table(table_name, schema, new_signature)
        elif current_signature[0] != new_signature:
            # Table exists but schema has changed
            print(f"Updating table: {table_name}")
            self._update_table(table_name, schema, new_signature)
        else:
            print(f"Table {table_name} is up to date")

    def _get_table_signature(self, table_name):
        """Get the current signature of a table from schema_signatures"""
        try:
            result = self._execute_query("SELECT signature FROM schema_signatures WHERE table_name = ?", (table_name,))
            return result[0] if result else None
        except Exception:
            # Table might not exist yet
            return None

    def _create_new_table(self, table_name, schema, signature):
        """Create a new table and record its signature"""
        # Check if table exists already (but isn't tracked)
        table_exists = self._check_table_exists(table_name)

        if table_exists:
            self._execute_query(f"DROP TABLE {table_name}")
        self._execute_query(schema)
        self._execute_query(
            """
            INSERT INTO schema_signatures (table_name, signature, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (table_name, signature),
        )

    def _update_table(self, table_name, schema, new_signature):
        """Update an existing table to a new schema"""
        self._execute_query(f"DROP TABLE {table_name}")
        self._execute_query(schema)
        self._execute_query(
            """
            UPDATE schema_signatures 
            SET signature = ?, last_updated = CURRENT_TIMESTAMP
            WHERE table_name = ?
            """,
            (new_signature, table_name),
        )

    def _check_table_exists(self, table_name):
        """Check if a table exists in the database"""
        try:
            result = self._execute_query(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
                """,
                (table_name,),
            )
            return result not in [None, []]
        except Exception:
            return False

    def clear_cache(self, older_than_days=None):
        """Clear cached data older than specified days"""
        if older_than_days:
            cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
            self._execute_query("DELETE FROM cache_entries WHERE cached_at < ?", (cutoff,))
        else:
            self._execute_query("DELETE FROM cache_entries")


class GitHubCache(Cache):
    def __init__(self, cache_dir="cache/github"):
        super().__init__(cache_dir, "github_cache.db")
        self.repo_cache = {}  # In-memory LRU cache

    def setup_db(self):
        """Initialize GitHub-specific cache tables with automatic schema versioning"""
        table_schemas = {
            "github_urls": """
                CREATE TABLE github_urls (
                    package TEXT PRIMARY KEY,
                    repo_url TEXT,
                    cached_at TIMESTAMP
                )
            """,
            "pr_info": """
                CREATE TABLE pr_info (
                    package TEXT,
                    commit_sha TEXT,
                    commit_node_id TEXT PRIMARY KEY,
                    pr_info TEXT,
                    cached_at TIMESTAMP
                )
            """,
            "pr_reviews": """
                CREATE TABLE pr_reviews (
                    package TEXT,
                    repo_name TEXT,
                    author TEXT,
                    first_review_data TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (repo_name, author)
                )
            """,
            "tag_to_sha": """
                CREATE TABLE tag_to_sha (
                    repo_name TEXT,
                    tag TEXT,
                    sha TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (repo_name, tag)
                )
            """,
        }

        for table_name, schema in table_schemas.items():
            self._check_and_update_table(table_name, schema)

    def cache_pr_review(self, package, repo_name, author, first_review_data):
        """Cache PR review information"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            c.execute(
                """
                INSERT OR REPLACE INTO pr_reviews
                (package, repo_name, author, first_review_data, cached_at)
                VALUES (?, ?, ?, ?, ?)
            """,
                (package, repo_name, author, json.dumps(first_review_data), datetime.now().isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    def get_pr_review(self, repo_name=None, author=None):
        """Get PR review information from cache"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            c.execute(
                "SELECT first_review_data, cached_at FROM pr_reviews WHERE repo_name = ? AND author = ?",
                (repo_name, author),
            )
            result = c.fetchone()
            if result:
                review_data, cached_at = result
                cached_at = datetime.fromisoformat(cached_at)

                # Return cached data if it's less than 30 days old
                if datetime.now() - cached_at < timedelta(days=30):
                    return json.loads(review_data)
            return None
        finally:
            conn.close()

    def cache_github_url(self, package, repo_info):
        """Cache GitHub URL for a package"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            c.execute(
                """
                INSERT OR REPLACE INTO github_urls 
                (package, repo_url, cached_at)
                VALUES (?, ?, ?)
            """,
                (package, json.dumps(repo_info), datetime.now().isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    def get_github_url(self, package):
        """Get cached GitHub URL for a package"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            c.execute("SELECT repo_url, cached_at FROM github_urls WHERE package = ?", (package,))
            result = c.fetchone()

            if result:
                repo_info, cached_at = result
                cached_at = datetime.fromisoformat(cached_at)

                # URLs don't change often, so we can cache them for longer (180 days)
                if datetime.now() - cached_at < timedelta(days=180):
                    return json.loads(repo_info)

            return None
        finally:
            conn.close()

    def cache_pr_info(self, pr_data: Dict):
        """Cache PR info with current timestamp"""
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pr_info
                (package, commit_sha, commit_node_id, pr_info, cached_at)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    pr_data["package"],
                    pr_data["commit_sha"],
                    pr_data["commit_node_id"],
                    json.dumps(pr_data["pr_info"]),
                    datetime.now().isoformat(),
                ),
            )
            conn.commit()

    def get_pr_info(self, commit_node_id: str) -> Optional[Dict]:
        """Get PR info from cache if available and not expired"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()
        try:
            c.execute(
                "SELECT package, commit_sha, commit_node_id, pr_info, cached_at FROM pr_info WHERE commit_node_id = ?",
                (commit_node_id,),
            )
            result = c.fetchone()

            if result:
                package, commit_sha, commit_node_id, pr_info, cached_at = result
                cached_at = datetime.fromisoformat(cached_at)
                if datetime.now() - cached_at < timedelta(hours=24):
                    return {
                        "package": package,
                        "commit_sha": commit_sha,
                        "commit_node_id": commit_node_id,
                        "pr_info": json.loads(pr_info),
                    }
            return None
        finally:
            conn.close()

    def cache_tag_to_sha(self, repo_name, tag, sha):
        """Cache tag to SHA mapping"""
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tag_to_sha
                (repo_name, tag, sha, cached_at)
                VALUES (?, ?, ?, ?)
            """,
                (repo_name, tag, sha, datetime.now().isoformat()),
            )
            conn.commit()

    def get_tag_to_sha(self, repo_name, tag):
        """Get SHA for a tag from cache"""
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            c = conn.cursor()
            c.execute("SELECT sha, cached_at FROM tag_to_sha WHERE repo_name = ? AND tag = ?", (repo_name, tag))
            result = c.fetchone()

            if result:
                sha, cached_at = result
                cached_at = datetime.fromisoformat(cached_at)
                if datetime.now() - cached_at < timedelta(days=180):
                    return sha
        return None

    def clear_cache(self, older_than_days=None):
        """Clear cached data"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            if older_than_days:
                cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                c.execute("DELETE FROM github_urls WHERE cached_at < ?", (cutoff,))
                c.execute("DELETE FROM pr_info WHERE cached_at < ?", (cutoff,))
                c.execute("DELETE FROM pr_reviews WHERE cached_at < ?", (cutoff,))
                c.execute("DELETE FROM tag_to_sha WHERE cached_at < ?", (cutoff,))
            else:
                c.execute("DELETE FROM github_urls")
                c.execute("DELETE FROM pr_info")
                c.execute("DELETE FROM pr_reviews")
                c.execute("DELETE FROM tag_to_sha")
            conn.commit()

        finally:
            conn.close()

    def clear_github_urls_from_package(self, package):
        """Clear cached GitHub URLs for a package"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            # first get count of rows with the package name
            c.execute("SELECT COUNT(*) FROM github_urls WHERE package = ?", (package,))
            count = c.fetchone()[0]
            if count == 0:
                print(f"No cached data found for {package}")
                logging.info(f"No cached data found for {package}")
                return

            # delete rows with the package name
            print(f"Deleting cached data for {package}")
            logging.info(f"Deleting cached data for {package}")
            c.execute("DELETE FROM github_urls WHERE package = ?", (package,))
            conn.commit()
        finally:
            conn.close()


class PackageAnalysisCache(Cache):
    def __init__(self, cache_dir="cache/packages"):
        super().__init__(cache_dir, "package_analysis.db")

    def setup_db(self):
        """Initialize package analysis cache tables with automatic schema versioning"""
        table_schemas = {
            "package_analysis": """
                CREATE TABLE package_analysis (
                    package_name TEXT,
                    version TEXT,
                    package_manager TEXT,
                    analysis_data TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (package_name, version, package_manager)
                )
            """
        }

        for table_name, schema in table_schemas.items():
            self._check_and_update_table(table_name, schema)

    def cache_package_analysis(self, package_name, version, package_manager, analysis_data):
        """Cache package analysis results"""
        self._execute_query(
            """
            INSERT OR REPLACE INTO package_analysis 
            (package_name, version, package_manager, analysis_data, cached_at)
            VALUES (?, ?, ?, ?, ?)
        """,
            (package_name, version, package_manager, json.dumps(analysis_data), datetime.now().isoformat()),
        )

    def get_package_analysis(self, package_name, version, package_manager, max_age_days=180):
        """Get cached package analysis results"""
        results = self._execute_query(
            """SELECT analysis_data, cached_at 
               FROM package_analysis 
               WHERE package_name = ? AND version = ? AND package_manager = ?""",
            (package_name, version, package_manager),
        )

        if results:
            analysis_data, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)

            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return json.loads(analysis_data)

        return None

    def clear_cache(self, older_than_days=None):
        """Clear cached data"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            if older_than_days:
                cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                c.execute("DELETE FROM package_analysis WHERE cached_at < ?", (cutoff,))
            else:
                c.execute("DELETE FROM package_analysis")

            conn.commit()

        finally:
            conn.close()

    def clear_package_by_version(self, package_name, version):
        """Clear cached data for a specific package version"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            c.execute(
                "SELECT COUNT(*) FROM package_analysis WHERE package_name = ? AND version = ?",
                (package_name, version),
            )
            count = c.fetchone()[0]
            if count == 0:
                print(f"No cached data found for {package_name} {version}")
                logging.info(f"No cached data found for {package_name} {version}")
                return

            c.execute("DELETE FROM package_analysis WHERE package_name = ? AND version = ?", (package_name, version))
            conn.commit()
            print(f"Cleared cached data for {package_name} {version}")
            logging.info(f"Cleared cached data for {package_name} {version}")

        finally:
            conn.close()


class CommitComparisonCache(Cache):
    def __init__(self, cache_dir="cache/commits"):
        super().__init__(cache_dir, "commit_comparison_cache.db")

    def setup_db(self):
        """Initialize commit comparison cache tables with automatic schema versioning"""
        table_schemas = {
            "commit_authors_from_tags": """
                CREATE TABLE commit_authors_from_tags (
                    package TEXT,
                    tag1 TEXT,
                    tag2 TEXT,
                    data TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (package, tag1, tag2)
                )
            """,
            "commit_authors_from_url": """
                CREATE TABLE commit_authors_from_url (
                    commit_url TEXT PRIMARY KEY,
                    data TEXT,
                    cached_at TIMESTAMP
                )
            """,
            "patch_authors_from_sha": """
                CREATE TABLE patch_authors_from_sha (
                    repo_name TEXT,
                    patch_path TEXT,
                    sha TEXT,
                    data TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (repo_name, patch_path, sha)
                )
            """,
        }

        for table_name, schema in table_schemas.items():
            self._check_and_update_table(table_name, schema)

    def cache_authors_from_tags(self, package, tag1, tag2, data):
        self._execute_query(
            """
            INSERT OR REPLACE INTO commit_authors_from_tags 
            (package, tag1, tag2, data, cached_at)
            VALUES (?, ?, ?, ?, ?)
        """,
            (package, tag1, tag2, json.dumps(data), datetime.now().isoformat()),
        )

    def get_authors_from_tags(self, package, tag1, tag2, max_age_days=180):
        results = self._execute_query(
            "SELECT data, cached_at FROM commit_authors_from_tags WHERE package = ? AND tag1 = ? AND tag2 = ?",
            (package, tag1, tag2),
        )
        if results:
            data, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)
            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return json.loads(data)
        return None

    def cache_authors_from_url(self, commit_url, data):
        self._execute_query(
            """
            INSERT OR REPLACE INTO commit_authors_from_url 
            (commit_url, data, cached_at)
            VALUES (?, ?, ?)
        """,
            (commit_url, json.dumps(data), datetime.now().isoformat()),
        )

    def get_authors_from_url(self, commit_url, max_age_days=180):
        results = self._execute_query(
            "SELECT data, cached_at FROM commit_authors_from_url WHERE commit_url = ?", (commit_url,)
        )
        if results:
            data, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)
            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return json.loads(data)
        return None

    def cache_patch_authors(self, repo_name, patch_path, sha, data):
        self._execute_query(
            """
            INSERT OR REPLACE INTO patch_authors_from_sha 
            (repo_name, patch_path, sha, data, cached_at)
            VALUES (?, ?, ?, ?, ?)
        """,
            (repo_name, patch_path, sha, json.dumps(data), datetime.now().isoformat()),
        )

    def get_patch_authors(self, repo_name, patch_path, sha, max_age_days=180):
        results = self._execute_query(
            "SELECT data, cached_at FROM patch_authors_from_sha WHERE repo_name = ? AND patch_path = ? AND sha = ?",
            (repo_name, patch_path, sha),
        )
        if results:
            data, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)
            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return json.loads(data)
        return None

    def clear_cache(self, older_than_days=None):
        """Clear cached data"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            if older_than_days:
                cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                c.execute("DELETE FROM commit_authors_from_tags WHERE cached_at < ?", (cutoff,))
                c.execute("DELETE FROM commit_authors_from_url WHERE cached_at < ?", (cutoff,))
                c.execute("DELETE FROM patch_authors_from_sha WHERE cached_at < ?", (cutoff,))
            else:
                c.execute("DELETE FROM commit_authors_from_tags")
                c.execute("DELETE FROM commit_authors_from_url")
                c.execute("DELETE FROM patch_authors_from_sha")

            conn.commit()

        finally:
            conn.close()


class UserCommitCache(Cache):
    def __init__(self, cache_dir="cache/user_commits"):
        super().__init__(cache_dir, "user_commits.db")

    def setup_db(self):
        """Initialize user commit cache tables with automatic schema versioning"""
        table_schemas = {
            "user_commit": """
                CREATE TABLE user_commit (
                    api_url TEXT PRIMARY KEY,
                    earliest_commit_sha TEXT,
                    repo_name TEXT,
                    package TEXT,
                    author_login TEXT,
                    author_commit_sha TEXT,
                    author_login_in_1st_commit TEXT,
                    author_id_in_1st_commit TEXT,
                    cached_at TIMESTAMP
                )
            """
        }

        for table_name, schema in table_schemas.items():
            self._check_and_update_table(table_name, schema)

    def cache_user_commit(
        self,
        api_url,
        earliest_commit_sha,
        repo_name,
        package,
        author_login,
        author_commit_sha,
        author_login_in_1st_commit,
        author_id_in_1st_commit,
    ):
        self._execute_query(
            """
            INSERT OR REPLACE INTO user_commit 
            (api_url, earliest_commit_sha, repo_name, package, author_login, author_commit_sha, author_login_in_1st_commit, author_id_in_1st_commit, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                api_url,
                earliest_commit_sha,
                repo_name,
                package,
                author_login,
                author_commit_sha,
                author_login_in_1st_commit,
                author_id_in_1st_commit,
                datetime.now().isoformat(),
            ),
        )

    def get_user_commit(self, api_url, max_age_days=180):
        results = self._execute_query(
            "SELECT earliest_commit_sha, author_login_in_1st_commit, author_id_in_1st_commit, cached_at FROM user_commit WHERE api_url = ?",
            (api_url,),
        )
        if results:
            earliest_commit_sha, author_login_in_1st_commit, author_id_in_1st_commit, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)
            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return earliest_commit_sha, author_login_in_1st_commit, author_id_in_1st_commit
        return None

    def clear_cache(self, older_than_days=None):
        """Clear cached data"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            if older_than_days:
                cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                c.execute("DELETE FROM user_commit WHERE cached_at < ?", (cutoff,))
            else:
                c.execute("DELETE FROM user_commit")

            conn.commit()

        finally:
            conn.close()


class DependencyExtractionCache(Cache):
    def __init__(self, cache_dir="cache/extracted_deps"):
        super().__init__(cache_dir, "maven_deps.db")

    def setup_db(self):
        """Initialize dependency extraction cache tables with automatic schema versioning"""
        table_schemas = {
            "extracted_dependencies": """
                CREATE TABLE extracted_dependencies (
                    repo_path TEXT,
                    file_hash TEXT,
                    dependencies TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (repo_path, file_hash)
                )
            """
        }

        for table_name, schema in table_schemas.items():
            self._check_and_update_table(table_name, schema)

    def cache_dependencies(self, repo_path, file_hash, dependencies):
        self._execute_query(
            """
            INSERT OR REPLACE INTO extracted_dependencies 
            (repo_path, file_hash, dependencies, cached_at)
            VALUES (?, ?, ?, ?)
        """,
            (repo_path, file_hash, json.dumps(dependencies), datetime.now().isoformat()),
        )

    def get_dependencies(self, repo_path, file_hash, max_age_days=180):
        results = self._execute_query(
            "SELECT dependencies, cached_at FROM extracted_dependencies WHERE repo_path = ? AND file_hash = ?",
            (repo_path, file_hash),
        )
        if results:
            deps_json, cached_at = results[0]
            cached_at = datetime.fromisoformat(cached_at)
            if datetime.now() - cached_at < timedelta(days=max_age_days):
                return json.loads(deps_json)
        return None

    def clear_cache(self, older_than_days=None):
        """Clear cached data"""
        conn = sqlite3.connect(self.db_path, timeout=10)
        c = conn.cursor()

        try:
            if older_than_days:
                cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()
                c.execute("DELETE FROM extracted_dependencies WHERE cached_at < ?", (cutoff,))
            else:
                c.execute("DELETE FROM extracted_dependencies")

            conn.commit()

        finally:
            conn.close()


# Module-level singleton
cache_manager = CacheManager()


def get_cache_manager():
    """Return the module-level CacheManager singleton."""
    return cache_manager
