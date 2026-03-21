# =============================================================================
# infrastructure/terraform/modules/iceberg/s3_buckets.tf
# Data lake buckets, lifecycle rules, and Glue Catalog
# =============================================================================

resource "aws_s3_bucket" "data_lake" {
  bucket        = var.data_lake_bucket
  force_destroy = var.environment != "prod"

  tags = {
    Environment = var.environment
    Purpose     = "Apache Iceberg data lake"
    Compliance  = "PCI-DSS,SOC2"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Object Lock: food safety & tax audit immutability ────────────────────────

resource "aws_s3_bucket_object_lock_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    default_retention {
      mode  = "COMPLIANCE"
      years = 7   # Food safety + tax record retention
    }
  }
}

# ── Lifecycle policies ────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze: frequent access → transition to IA after 30 days
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  # Silver: keep hot for 60 days
  rule {
    id     = "silver-tiering"
    status = "Enabled"
    filter { prefix = "silver/" }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
  }

  # Gold: always hot (queried by Snowflake and BI tools)
  rule {
    id     = "gold-abort-incomplete"
    status = "Enabled"
    filter { prefix = "gold/" }

    abort_incomplete_multipart_upload { days_after_initiation = 3 }
  }

  # Iceberg metadata: keep forever but expire old manifests
  rule {
    id     = "iceberg-metadata-cleanup"
    status = "Enabled"
    filter { prefix = "*/metadata/" }

    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

# ── KMS key ───────────────────────────────────────────────────────────────────

resource "aws_kms_key" "data_lake" {
  description             = "CMK for restaurant data lake (${var.environment})"
  deletion_window_in_days = var.environment == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/restaurant-data-lake-${var.environment}"
  target_key_id = aws_kms_key.data_lake.key_id
}

# =============================================================================
# infrastructure/terraform/modules/iceberg/glue_catalog.tf
# =============================================================================

resource "aws_glue_catalog_database" "bronze" {
  name        = "restaurant_bronze_${var.environment}"
  description = "Raw ingestion layer — POS, IoT, inventory"

  create_table_default_permission {
    permissions = ["SELECT"]
    principal { data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS" }
  }
}

resource "aws_glue_catalog_database" "silver" {
  name        = "restaurant_silver_${var.environment}"
  description = "Cleaned, PII-masked, business-normalised data"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "restaurant_gold_${var.environment}"
  description = "BI-ready aggregations and ML feature tables"
}

# Lake Formation data lake settings
resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/DataLakeAdmin",
  ]
}

# Grant Data Engineer role access to all layers
resource "aws_lakeformation_permissions" "data_engineer_bronze" {
  principal   = var.data_engineer_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE", "SELECT", "INSERT", "DELETE"]

  database {
    name = aws_glue_catalog_database.bronze.name
  }
}

resource "aws_lakeformation_permissions" "data_engineer_silver" {
  principal   = var.data_engineer_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE", "SELECT", "INSERT", "DELETE"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "data_engineer_gold" {
  principal   = var.data_engineer_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE", "SELECT", "INSERT", "DELETE"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}

data "aws_caller_identity" "current" {}
