# =============================================================================
# infrastructure/terraform/environments/dev/main.tf
# =============================================================================

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }

  backend "s3" {
    bucket         = "restaurant-terraform-state-dev"
    key            = "restaurant-data-platform/dev/terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "restaurant-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = "dev"
      Project     = "restaurant-data-platform"
      ManagedBy   = "Terraform"
      Owner       = "data-engineering"
    }
  }
}

# ── Iceberg data lake ─────────────────────────────────────────────────────────

module "iceberg" {
  source = "../../modules/iceberg"

  environment          = "dev"
  data_lake_bucket     = "restaurant-iceberg-dev"
  payment_vault_bucket = "restaurant-payment-tokens-dev"
  data_engineer_role_arn = module.iam.data_engineer_role_arn
}

# ── IAM roles ─────────────────────────────────────────────────────────────────

module "iam" {
  source = "../../modules/iam"

  data_lake_bucket      = module.iceberg.data_lake_bucket_name
  payment_vault_bucket  = module.iceberg.payment_vault_bucket_name
  payment_kms_key_arn   = module.iceberg.payment_kms_key_arn
  data_lake_kms_key_arn = module.iceberg.data_lake_kms_key_arn

  eks_oidc_provider_arn = module.airflow.eks_oidc_provider_arn
  eks_oidc_provider     = module.airflow.eks_oidc_provider

  snowflake_aws_principal = var.snowflake_aws_principal
  snowflake_external_id   = var.snowflake_external_id

  supplier_aws_account_arn = var.supplier_aws_account_arn
  supplier_external_id     = var.supplier_external_id
}

# ── Airflow (MWAA) ────────────────────────────────────────────────────────────

module "airflow" {
  source = "../../modules/airflow"

  environment          = "dev"
  airflow_bucket       = "restaurant-airflow-dev"
  vpc_id               = var.vpc_id
  private_subnet_ids   = var.private_subnet_ids
  airflow_version      = "2.8.1"
  environment_class    = "mw1.small"   # Cost-optimised for dev
  min_workers          = 1
  max_workers          = 5
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "aws_region"               { default = "eu-west-1" }
variable "vpc_id"                   {}
variable "private_subnet_ids"       { type = list(string) }
variable "snowflake_aws_principal"  {}
variable "snowflake_external_id"    { sensitive = true }
variable "supplier_aws_account_arn" {}
variable "supplier_external_id"     { sensitive = true }

# ── Outputs ───────────────────────────────────────────────────────────────────

output "data_lake_bucket"      { value = module.iceberg.data_lake_bucket_name }
output "airflow_webserver_url" { value = module.airflow.webserver_url }
output "franchisee_role_arn"   { value = module.iam.franchisee_role_arn }
