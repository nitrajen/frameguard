"""Re-export Optional from the shared nullable module."""

from dfguard._nullable import Optional, _NullableAnnotation

__all__ = ["Optional", "_NullableAnnotation"]
