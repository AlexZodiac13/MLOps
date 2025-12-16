"""Package for feature repository.

Ensure feature definitions are imported when the repo package is loaded by Feast.
"""

# Import all feature definitions so Feast registers them when the package is loaded
from .example_repo import *  # noqa: F401,F403
