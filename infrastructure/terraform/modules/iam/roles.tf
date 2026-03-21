# =============================================================================
# infrastructure/terraform/modules/iam/roles.tf
# Restaurant-specific IAM roles with ABAC for franchisee isolation
# =============================================================================

# ── Data sources ──────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# =============================================================================
# 1. Franchisee Isolation (ABAC via session tags)
# =============================================================================

data "aws_iam_policy_document" "franchisee_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:RequestedRegion"
      values   = [local.region]
    }
  }
}

resource "aws_iam_role" "franchisee" {
  name                 = "restaurant-franchisee-data-access"
  assume_role_policy   = data.aws_iam_policy_document.franchisee_trust.json
  max_session_duration = 3600

  tags = {
    Purpose     = "Franchisee data isolation via ABAC"
    Compliance  = "PCI-DSS"
    ManagedBy   = "Terraform"
  }
}

data "aws_iam_policy_document" "franchisee_access" {
  # Allow read access only to objects tagged with the user's franchisee_id
  statement {
    sid     = "FranchiseeS3ReadOwn"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]

    resources = [
      "arn:aws:s3:::${var.data_lake_bucket}",
      "arn:aws:s3:::${var.data_lake_bucket}/*",
    ]

    condition {
      test     = "StringEquals"
      variable = "s3:ExistingObjectTag/franchisee_id"
      values   = ["&{aws:PrincipalTag/franchisee_id}"]
    }
  }

  # Explicit deny for other franchisees' tagged objects
  statement {
    sid     = "DenyOtherFranchisees"
    effect  = "Deny"
    actions = ["s3:*"]

    resources = ["arn:aws:s3:::${var.data_lake_bucket}/*"]

    condition {
      test     = "StringNotEquals"
      variable = "s3:ExistingObjectTag/franchisee_id"
      values   = ["&{aws:PrincipalTag/franchisee_id}"]
    }

    condition {
      test     = "StringEquals"
      variable = "s3:ExistingObjectTag/data_classification"
      values   = ["franchisee_accessible"]
    }
  }

  # Allow Athena queries (scoped to franchisee workgroup)
  statement {
    sid    = "AthenaFranchiseeWorkgroup"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryExecution",
      "athena:StopQueryExecution",
    ]
    resources = [
      "arn:aws:athena:${local.region}:${local.account_id}:workgroup/franchisee-*"
    ]
  }
}

resource "aws_iam_role_policy" "franchisee_access" {
  name   = "franchisee-data-access"
  role   = aws_iam_role.franchisee.id
  policy = data.aws_iam_policy_document.franchisee_access.json
}

# =============================================================================
# 2. Kitchen IoT Devices (publish-only, least privilege)
# =============================================================================

data "aws_iam_policy_document" "kitchen_iot_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["iot.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "kitchen_iot" {
  name               = "restaurant-kitchen-iot-devices"
  assume_role_policy = data.aws_iam_policy_document.kitchen_iot_trust.json

  tags = {
    Purpose   = "Kitchen IoT sensor telemetry"
    ManagedBy = "Terraform"
  }
}

data "aws_iam_policy_document" "kitchen_iot_publish" {
  # Devices may ONLY publish to their own topic
  statement {
    sid    = "IoTPublishOwnTopic"
    effect = "Allow"
    actions = ["iot:Publish"]

    resources = [
      "arn:aws:iot:${local.region}:${local.account_id}:topic/restaurant/kitchen/$${iot:Connection.Thing.ThingName}"
    ]

    condition {
      test     = "Bool"
      variable = "iot:Connection.Thing.IsAttached"
      values   = ["true"]
    }
  }

  # Allow devices to receive their own config updates
  statement {
    sid    = "IoTSubscribeConfig"
    effect = "Allow"
    actions = ["iot:Subscribe", "iot:Receive"]

    resources = [
      "arn:aws:iot:${local.region}:${local.account_id}:topicfilter/restaurant/config/$${iot:Connection.Thing.ThingName}"
    ]
  }

  # Kinesis: write sensor stream
  statement {
    sid    = "KinesisWriteSensorStream"
    effect = "Allow"
    actions = ["kinesis:PutRecord", "kinesis:PutRecords"]

    resources = [
      "arn:aws:kinesis:${local.region}:${local.account_id}:stream/${var.kitchen_sensor_stream}"
    ]
  }
}

resource "aws_iam_role_policy" "kitchen_iot_publish" {
  name   = "iot-publish-only"
  role   = aws_iam_role.kitchen_iot.id
  policy = data.aws_iam_policy_document.kitchen_iot_publish.json
}

# =============================================================================
# 3. Payment Tokenisation Service (EKS IRSA, write-only to vault)
# =============================================================================

data "aws_iam_policy_document" "payment_processor_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider}:sub"
      values   = ["system:serviceaccount:payments:tokenizer"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "payment_processor" {
  name               = "restaurant-payment-tokenisation"
  assume_role_policy = data.aws_iam_policy_document.payment_processor_trust.json

  tags = {
    Purpose    = "PCI-DSS payment tokenisation"
    Compliance = "PCI-DSS"
    ManagedBy  = "Terraform"
  }
}

data "aws_iam_policy_document" "payment_token_write_only" {
  # Write tokens only — encrypted with dedicated KMS key
  statement {
    sid     = "TokenVaultWrite"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    resources = ["arn:aws:s3:::${var.payment_vault_bucket}/*"]

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-server-side-encryption"
      values   = ["aws:kms"]
    }

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [var.payment_kms_key_arn]
    }
  }

  # Explicit deny on read — no one reads raw tokens from S3
  statement {
    sid     = "DenyTokenRead"
    effect  = "Deny"
    actions = ["s3:GetObject", "s3:ListBucket", "s3:HeadObject"]

    resources = [
      "arn:aws:s3:::${var.payment_vault_bucket}",
      "arn:aws:s3:::${var.payment_vault_bucket}/*",
    ]
  }
}

resource "aws_iam_role_policy" "payment_token_write" {
  name   = "payment-token-write-only"
  role   = aws_iam_role.payment_processor.id
  policy = data.aws_iam_policy_document.payment_token_write_only.json
}

# =============================================================================
# 4. Snowflake Cross-Account Role
# =============================================================================

data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.snowflake_aws_principal]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_iceberg" {
  name               = "restaurant-snowflake-iceberg-access"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json

  tags = {
    Purpose   = "Snowflake external volume for Iceberg tables"
    ManagedBy = "Terraform"
  }
}

data "aws_iam_policy_document" "snowflake_iceberg_access" {
  statement {
    sid    = "IcebergReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      "arn:aws:s3:::${var.data_lake_bucket}",
      "arn:aws:s3:::${var.data_lake_bucket}/*",
    ]
  }

  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartition",
      "glue:GetTableVersions",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "KMSDecrypt"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]
    resources = [var.data_lake_kms_key_arn]
  }
}

resource "aws_iam_role_policy" "snowflake_iceberg" {
  name   = "snowflake-iceberg-access"
  role   = aws_iam_role.snowflake_iceberg.id
  policy = data.aws_iam_policy_document.snowflake_iceberg_access.json
}

# =============================================================================
# 5. Supplier Read-Only Portal (time-bound cross-account)
# =============================================================================

data "aws_iam_policy_document" "supplier_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.supplier_aws_account_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.supplier_external_id]
    }

    condition {
      test     = "DateGreaterThan"
      variable = "aws:CurrentTime"
      values   = [var.supplier_access_start]
    }

    condition {
      test     = "DateLessThan"
      variable = "aws:CurrentTime"
      values   = [var.supplier_access_end]
    }
  }
}

resource "aws_iam_role" "supplier_readonly" {
  name               = "restaurant-supplier-readonly"
  assume_role_policy = data.aws_iam_policy_document.supplier_trust.json

  tags = {
    Purpose   = "Supplier analytics portal (time-bound)"
    ManagedBy = "Terraform"
  }
}

data "aws_iam_policy_document" "supplier_product_view" {
  statement {
    sid    = "SupplierOwnProductsOnly"
    effect = "Allow"
    actions = ["s3:GetObject"]

    resources = ["arn:aws:s3:::${var.data_lake_bucket}/gold/supplier_scorecards/*"]

    condition {
      test     = "StringEquals"
      variable = "s3:ExistingObjectTag/supplier_id"
      values   = ["&{aws:PrincipalTag/supplier_id}"]
    }
  }

  statement {
    sid    = "AthenaSupplierWorkgroup"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryExecution",
    ]
    resources = [
      "arn:aws:athena:${local.region}:${local.account_id}:workgroup/supplier-portal"
    ]
  }
}

resource "aws_iam_role_policy" "supplier_readonly" {
  name   = "supplier-product-view"
  role   = aws_iam_role.supplier_readonly.id
  policy = data.aws_iam_policy_document.supplier_product_view.json
}
