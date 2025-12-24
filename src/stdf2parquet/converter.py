"""Convert STDF records to Parquet format."""

from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .records import get_schema, RECORD_TYPES


def convert_value(value: Any, field_type: pa.DataType) -> Any:
    """Convert a value to the appropriate type for PyArrow."""
    if value is None:
        return None

    # Handle list types
    if isinstance(field_type, pa.ListType):
        if isinstance(value, (list, tuple)):
            return list(value)
        return None

    # Handle binary types
    if pa.types.is_binary(field_type):
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return None

    # Handle string types
    if pa.types.is_string(field_type):
        if value is None:
            return None
        return str(value)

    # Handle numeric types
    if pa.types.is_integer(field_type) or pa.types.is_floating(field_type):
        try:
            return value
        except (ValueError, TypeError):
            return None

    return value


class ParquetConverter:
    """Converts STDF records to Parquet files."""

    def __init__(self, output_dir: Path):
        """
        Initialize the converter.

        Args:
            output_dir: Directory where Parquet files will be written
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Store records by type
        self._records: dict[str, list[dict]] = {}

    def add_record(self, record_type: str, record_data: dict) -> None:
        """
        Add a record to be converted.

        Args:
            record_type: Type of the record (e.g., 'PTR', 'PIR')
            record_data: Dictionary of field names to values
        """
        if record_type not in self._records:
            self._records[record_type] = []

        self._records[record_type].append(record_data)

    def _build_table(
        self, record_type: str, records: list[dict]
    ) -> pa.Table | None:
        """Build a PyArrow table from records."""
        schema = get_schema(record_type)

        if schema is None:
            # Create dynamic schema for unknown record types
            if not records:
                return None

            # Infer schema from first record
            fields = []
            for key in records[0].keys():
                fields.append(pa.field(key, pa.string()))
            schema = pa.schema(fields)

        # Build columns
        columns: dict[str, list] = {field.name: [] for field in schema}

        for record in records:
            for field in schema:
                value = record.get(field.name)
                converted = convert_value(value, field.type)
                columns[field.name].append(converted)

        # Create arrays
        arrays = []
        for field in schema:
            try:
                arr = pa.array(columns[field.name], type=field.type)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Fallback to string type if conversion fails
                arr = pa.array(
                    [str(v) if v is not None else None for v in columns[field.name]],
                    type=pa.string()
                )
            arrays.append(arr)

        return pa.Table.from_arrays(arrays, schema=schema)

    def write_all(self) -> dict[str, int]:
        """
        Write all collected records to Parquet files.

        Returns:
            Dictionary mapping record type to number of records written
        """
        results = {}

        for record_type, records in self._records.items():
            if not records:
                continue

            table = self._build_table(record_type, records)
            if table is None:
                continue

            output_path = self.output_dir / f"{record_type}.parquet"
            pq.write_table(table, output_path, compression="snappy")
            results[record_type] = len(records)

        return results

    def clear(self) -> None:
        """Clear all collected records."""
        self._records.clear()


def convert_stdf_to_parquet(
    stdf_path: Path,
    output_dir: Path,
    record_filter: set[str] | None = None,
    progress_callback: Callable[[int], None] | None = None,
) -> dict[str, int]:
    """
    Convert an STDF file to Parquet files.

    Args:
        stdf_path: Path to the STDF file
        output_dir: Directory for output Parquet files
        record_filter: Optional set of record types to include
        progress_callback: Optional callback for progress updates

    Returns:
        Dictionary mapping record type to number of records written
    """
    from .parser import parse_stdf

    converter = ParquetConverter(output_dir)

    for i, (record_type, record_data) in enumerate(parse_stdf(stdf_path)):
        # Apply filter if specified
        if record_filter and record_type not in record_filter:
            continue

        converter.add_record(record_type, record_data)

        if progress_callback and i % 1000 == 0:
            progress_callback(i)

    return converter.write_all()
