from importlib.metadata import version

from . import gcp

__version__ = version("mooncompute")

__all__ = ["gcp", "__version__"]
