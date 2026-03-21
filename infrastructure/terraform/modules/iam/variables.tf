variable "data_lake_bucket" {
  description = "Name of the primary Iceberg data lake S3 bucket"
  type        = string
}

variable "payment_vault_bucket" {
  description = "S3 bucket for PCI-DSS payment tokens (write-only)"
  type        = string
}

variable "payment_kms_key_arn" {
  description = "KMS key ARN for payment vault encryption"
  type        = string
}

variable "data_lake_kms_key_arn" {
  description = "KMS key ARN for data lake encryption"
  type        = string
}

variable "kitchen_sensor_stream" {
  description = "Kinesis Data Stream name for kitchen IoT sensor data"
  type        = string
  default     = "restaurant-kitchen-sensors"
}

variable "eks_oidc_provider_arn" {
  description = "ARN of the EKS cluster OIDC provider (for IRSA)"
  type        = string
}

variable "eks_oidc_provider" {
  description = "OIDC provider URL (without https://) for EKS IRSA conditions"
  type        = string
}

variable "snowflake_aws_principal" {
  description = "Snowflake's AWS IAM user ARN (from SHOW INTEGRATIONS)"
  type        = string
}

variable "snowflake_external_id" {
  description = "Snowflake external ID for the storage integration"
  type        = string
  sensitive   = true
}

variable "supplier_aws_account_arn" {
  description = "ARN of the supplier's AWS account root for cross-account trust"
  type        = string
}

variable "supplier_external_id" {
  description = "External ID for supplier cross-account assume role"
  type        = string
  sensitive   = true
}

variable "supplier_access_start" {
  description = "ISO 8601 start date for supplier time-bound access"
  type        = string
  default     = "2026-01-01T00:00:00Z"
}

variable "supplier_access_end" {
  description = "ISO 8601 end date for supplier time-bound access"
  type        = string
  default     = "2026-12-31T23:59:59Z"
}
