variable "project" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "accident-severity"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-central-1"
}

variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "azure_location" {
  description = "Azure region for Event Hubs"
  type        = string
  default     = "eastus"
}

variable "azure_resource_group" {
  description = "Azure resource group name"
  type        = string
  default     = "accident-severity-rg"
}

variable "eventhub_sku" {
  description = "Event Hubs namespace SKU (Basic or Standard)"
  type        = string
  default     = "Standard"
}

variable "db_username" {
  description = "RDS PostgreSQL master username"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "RDS PostgreSQL master password"
  type        = string
  sensitive   = true
}

variable "rds_extra_ingress_cidrs" {
  description = "Additional CIDRs allowed to access RDS (e.g. your IP)"
  type        = list(string)
  default     = []
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers per streaming job"
  type        = number
  default     = 2
}
