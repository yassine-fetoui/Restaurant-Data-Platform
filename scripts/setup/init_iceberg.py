"""
scripts/setup/init_iceberg.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bootstrap all Iceberg tables from YAML schema definitions.

Usage:
    python scripts/setup/init_iceberg.py [--env dev|staging|prod]
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, HourTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
    TimestampType,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

SCHEMAS_DIR = Path(__file__).parents[2] / "iceberg" / "schemas"

TYPE_MAP = {
    "string":      StringType(),
    "long":        LongType(),
    "integer":     IntegerType(),
    "double":      DoubleType(),
    "float":       FloatType(),
    "boolean":     BooleanType(),
    "date":        DateType(),
    "timestamptz": TimestamptzType(),
    "timestamp":   TimestampType(),
}

TRANSFORM_MAP = {
    "identity": IdentityTransform(),
    "day":      DayTransform(),
    "hour":     HourTransform(),
    "month":    MonthTransform(),
}


def parse_type(type_str: str):
    if type_str.startswith("decimal"):
        # e.g. decimal(10, 2)
        inner = type_str[8:-1]
        precision, scale = (int(x.strip()) for x in inner.split(","))
        return DecimalType(precision, scale)
    return TYPE_MAP[type_str]


def build_schema(fields: list[dict]) -> Schema:
    nested_fields = []
    for f in fields:
        nested_fields.append(
            NestedField(
                field_id=f["id"],
                name=f["name"],
                field_type=parse_type(f["type"]),
                required=f.get("required", False),
                doc=f.get("doc"),
            )
        )
    return Schema(*nested_fields)


def build_partition_spec(spec_def: list[dict], schema: Schema) -> PartitionSpec:
    fields = []
    for i, p in enumerate(spec_def):
        source_field = schema.find_field(p["source"])
        fields.append(
            PartitionField(
                source_id=source_field.field_id,
                field_id=1000 + i,
                transform=TRANSFORM_MAP[p["transform"]],
                name=p["name"],
            )
        )
    return PartitionSpec(*fields)


def create_tables_from_yaml(yaml_path: Path, catalog, env: str) -> None:
    data = yaml.safe_load(yaml_path.read_text())

    for table_def in data.get("tables", []):
        identifier = table_def["identifier"]
        schema = build_schema(table_def["schema"]["fields"])
        partition_spec = build_partition_spec(
            table_def.get("partition_spec", []), schema
        )
        properties = table_def.get("properties", {})

        try:
            catalog.create_table(
                identifier=identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties,
            )
            log.info("✅  Created %s", identifier)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                log.info("⏭️   %s already exists — skipping", identifier)
            else:
                log.error("❌  Failed to create %s: %s", identifier, exc)
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Iceberg tables from YAML")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    warehouse = f"s3://restaurant-iceberg-{args.env}/"
    catalog = load_catalog("glue", **{"type": "glue", "warehouse": warehouse})

    log.info("Initialising Iceberg tables in %s (%s)", warehouse, args.env)

    for schema_file in sorted(SCHEMAS_DIR.glob("*.yaml")):
        log.info("Processing %s", schema_file.name)
        create_tables_from_yaml(schema_file, catalog, args.env)

    log.info("Iceberg initialisation complete.")


if __name__ == "__main__":
    main()
