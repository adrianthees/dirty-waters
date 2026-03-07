import re
from typing import Dict, List, Optional, Union

PNPM_LIST_COMMAND = lambda scope: [
    "pnpm",
    "list",
    "--filter",
    scope,
    "--depth",
    "Infinity",
]


class YarnLockParser:
    def __init__(self, content: str):
        """
        Initialize the Yarn.lock v1 parser with file content.

        :param content: Full content of the yarn.lock file
        """
        self.raw_content = content
        self.dependencies: Dict[str, Dict[str, Union[str, Dict[str, str]]]] = {}

    def parse(self) -> Dict[str, Dict[str, Union[str, Dict[str, str]]]]:
        """
        Parse the Yarn.lock v1 file content and extract dependency information.

        :return: Dictionary of parsed dependencies
        """
        # Reset dependencies for each parse
        self.dependencies = {}

        # Split the file into individual dependency blocks
        dependency_blocks = self._split_dependency_blocks(self.raw_content)

        # Parse each dependency block
        for block in dependency_blocks:
            parsed_block = self._parse_dependency_block(block)
            if parsed_block:
                name, details = parsed_block
                self.dependencies[name] = details

        return self.dependencies

    def _split_dependency_blocks(self, content: str) -> List[str]:
        """
        Split the file content into individual dependency blocks.

        :param content: Full content of the yarn.lock file
        :return: List of dependency block strings
        """
        # Yarn.lock v1 uses double newline as block separator
        blocks = re.split(r"\n\n", content.strip())
        return [block for block in blocks if block.strip()]

    def _parse_dependency_block(self, block: str) -> Optional[tuple]:
        """
        Parse an individual dependency block.

        :param block: A single dependency block
        :return: Tuple of (dependency name, dependency details) or None
        """
        lines = block.split("\n")

        # First line typically contains the name and version
        first_line = lines[0].strip()

        # Skip comments or empty lines
        if first_line.startswith("#") or not first_line:
            return None

        # Extract name and version
        name_version_match = re.match(r'^"?([^@"]+)@(?:npm:([^@]+)@)?(.+)"?:', first_line)
        if not name_version_match:
            return None

        alias = name_version_match.group(1)
        original_name = name_version_match.group(2) if name_version_match.group(2) else None
        version_constraint = name_version_match.group(3)

        # Initialize details dictionary
        details: Dict[str, Union[str, Dict[str, str]]] = {
            "original_name": original_name,
            "version_constraint": version_constraint,
            "resolved": None,
            "integrity": None,
            "dependencies": {},
        }

        # Track parsing state
        current_section = "metadata"
        current_dependency = None

        # Parse subsequent lines for additional metadata and nested dependencies
        for line in lines[1:]:
            line = line.strip()

            # Check for version constraint
            version_match = re.match(r'version "(.*)"', line)
            if version_match and current_section == "metadata":
                details["version_constraint"] = version_match.group(1)
                continue

            # Check for resolved URL
            resolved_match = re.match(r'resolved "(.*)"', line)
            if resolved_match and current_section == "metadata":
                details["resolved"] = resolved_match.group(1)
                continue

            # Check for integrity hash
            integrity_match = re.match(r'integrity "(.*)"', line)
            if integrity_match and current_section == "metadata":
                details["integrity"] = integrity_match.group(1)
                continue

            # Handle dependencies section
            if line.startswith("dependencies:"):
                current_section = "dependencies"
                continue

            # Parse nested dependencies
            if current_section == "dependencies":
                # Check if this is a new nested dependency
                nested_dep_match = re.match(r'^([^\s]+) "(.*)"', line)
                if nested_dep_match:
                    current_dependency = nested_dep_match.group(1)
                    dep_version = nested_dep_match.group(2)
                    details["dependencies"][current_dependency] = dep_version

        return (alias, details)

    def get_dependency(self, name: str) -> Optional[Dict[str, Union[str, Dict[str, str]]]]:
        """
        Retrieve details for a specific dependency.

        :param name: Name of the dependency
        :return: Dependency details or None
        """
        return self.dependencies.get(name)

    def list_dependencies(self) -> List[str]:
        """
        List all parsed dependencies.

        :return: List of dependency names
        """
        return list(self.dependencies.keys())
