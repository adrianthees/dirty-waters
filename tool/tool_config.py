"""
Re-export facade for backward compatibility.

This module re-exports all symbols that were previously defined here,
now organized into modules:
  - tool.cache: Cache classes and CacheManager
  - tool.github_api: GitHub API authentication and request helpers
  - tool.parsers: Lockfile parsers (YarnLockParser, PNPM_LIST_COMMAND)
  - tool.config: Configuration constants and loading
  - tool.utils: PathManager, clone_repo, setup_logger, URL helpers
"""

# Cache
from tool.cache import (  # noqa: F401
    Cache,
    CacheManager,
    CommitComparisonCache,
    DependencyExtractionCache,
    GitHubCache,
    PackageAnalysisCache,
    UserCommitCache,
    cache_manager,
    get_cache_manager,
)

# Config
from tool.config import (  # noqa: F401
    CLONE_OPTIONS,
    DEFAULT_CONFIG,
    DEFAULT_CONFIG_PATH,
    DEFAULT_ENABLED_CHECKS,
    load_config,
)

# GitHub API
from tool.github_api import (  # noqa: F401
    GITHUB_GRAPHQL_URL,
    get_last_page_info,
    github_token,
    headers,
    headers_v4,
    make_github_request,
)

# Parsers
from tool.parsers import PNPM_LIST_COMMAND, YarnLockParser  # noqa: F401

# Utils
from tool.utils import PathManager, clone_repo, get_package_url, get_registry_url, setup_logger  # noqa: F401
