from collections import OrderedDict
from typing import Optional


class InfluxDBError(Exception):
    """Base exception for InfluxDB-related errors"""

    pass


class InvalidMeasurementError(InfluxDBError):
    """Raised when measurement name is invalid"""

    pass


class InvalidKeyError(InfluxDBError):
    """Raised when a tag or field key is invalid"""

    pass


class InvalidLineError(InfluxDBError):
    """Raised when a line protocol string is invalid"""

    pass


class LineBuilder:
    def __init__(self, measurement: str):
        if " " in measurement:
            raise InvalidMeasurementError("Measurement name cannot contain spaces")
        self.measurement = measurement
        self.tags: OrderedDict[str, str] = OrderedDict()
        self.fields: OrderedDict[str, str] = OrderedDict()
        self._timestamp_ns: Optional[int] = None

    def _validate_key(self, key: str, key_type: str) -> None:
        """Validate that a key does not contain spaces, commas, or equals signs."""
        if not key:
            raise InvalidKeyError(f"{key_type} key cannot be empty")
        if " " in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain spaces")
        if "," in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain commas")
        if "=" in key:
            raise InvalidKeyError(f"{key_type} key '{key}' cannot contain equals signs")

    def _escape_measurement(self, value: str) -> str:
        """Escape characters in measurement names according to line protocol."""
        return value.replace(",", "\\,").replace(" ", "\\ ")

    def _escape_tag_value(self, value: str) -> str:
        """Escape characters in tag values according to line protocol."""
        return (
            value.replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace("=", "\\=")
            .replace(" ", "\\ ")
        )

    def _escape_field_key(self, value: str) -> str:
        """Escape characters in field keys according to line protocol."""
        return (
            value.replace("\\", "\\\\")
            .replace(",", "\\,")
            .replace("=", "\\=")
            .replace(" ", "\\ ")
        )

    def tag(self, key: str, value: str) -> "LineBuilder":
        """Add a tag to the line protocol."""
        self._validate_key(key, "tag")
        self.tags[key] = str(value)
        return self

    def uint64_field(self, key: str, value: int) -> "LineBuilder":
        """Add an unsigned integer field to the line protocol."""
        self._validate_key(key, "field")
        if value < 0:
            raise ValueError(f"uint64 field '{key}' cannot be negative")
        self.fields[key] = f"{value}u"
        return self

    def int64_field(self, key: str, value: int) -> "LineBuilder":
        """Add an integer field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = f"{value}i"
        return self

    def float64_field(self, key: str, value: float) -> "LineBuilder":
        """Add a float field to the line protocol."""
        self._validate_key(key, "field")
        # Check if value has no decimal component
        self.fields[key] = f"{int(value)}.0" if value % 1 == 0 else str(value)
        return self

    def string_field(self, key: str, value: str) -> "LineBuilder":
        """Add a string field to the line protocol."""
        self._validate_key(key, "field")
        # Escape quotes and backslashes in string values
        escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
        self.fields[key] = f'"{escaped_value}"'
        return self

    def bool_field(self, key: str, value: bool) -> "LineBuilder":
        """Add a boolean field to the line protocol."""
        self._validate_key(key, "field")
        self.fields[key] = "t" if value else "f"
        return self

    def time_ns(self, timestamp_ns: int) -> "LineBuilder":
        """Set the timestamp in nanoseconds."""
        self._timestamp_ns = timestamp_ns
        return self

    def build(self) -> str:
        """Build the line protocol string."""
        # Start with measurement name (escape commas and spaces)
        line = self._escape_measurement(self.measurement)

        # Add tags if present
        if self.tags:
            tags_str = ",".join(
                f"{key}={self._escape_tag_value(value)}"
                for key, value in self.tags.items()
            )
            line += f",{tags_str}"

        # Add fields (required)
        if not self.fields:
            raise InvalidLineError(f"At least one field is required: {line}")

        fields_str = ",".join(
            f"{self._escape_field_key(key)}={value}"
            for key, value in self.fields.items()
        )
        line += f" {fields_str}"

        # Add timestamp if present
        if self._timestamp_ns is not None:
            line += f" {self._timestamp_ns}"

        return line
