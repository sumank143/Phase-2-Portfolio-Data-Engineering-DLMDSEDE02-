##############################################################################
# main.tf — Root module: wires all sub-modules together
##############################################################################

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

# ── VPC ──────────────────────────────────────────────────
module "vpc" {
  source     = "./modules/vpc"
  project    = var.project
  aws_region = var.aws_region
}

# ── S3 ───────────────────────────────────────────────────
module "s3" {
  source  = "./modules/s3"
  project = var.project
}

# ── Azure Event Hubs (Kafka) ─────────────────────────────
module "eventhubs" {
  source              = "./modules/eventhubs"
  project             = var.project
  azure_location      = var.azure_location
  resource_group_name = var.azure_resource_group
  eventhub_sku        = var.eventhub_sku
}

# ── RDS PostgreSQL ───────────────────────────────────────
module "rds" {
  source                = "./modules/rds"
  project               = var.project
  vpc_id                = module.vpc.vpc_id
  subnet_ids            = module.vpc.public_subnet_ids
  db_username           = var.db_username
  db_password           = var.db_password
  glue_security_group_id = module.glue.glue_security_group_id
  rds_extra_ingress_cidrs = var.rds_extra_ingress_cidrs
}

# ── EC2 (Producer) ───────────────────────────────────────
module "ec2" {
  source       = "./modules/ec2"
  project      = var.project
  vpc_id       = module.vpc.vpc_id
  subnet_id    = module.vpc.public_subnet_ids[0]
  s3_bucket    = module.s3.bucket_name
  kafka_brokers = module.eventhubs.kafka_brokers
  kafka_password = module.eventhubs.connection_string
}

# ── Glue (Streaming + ML) ───────────────────────────────
module "glue" {
  source              = "./modules/glue"
  project             = var.project
  aws_region          = var.aws_region
  s3_bucket           = module.s3.bucket_name
  s3_bucket_arn       = module.s3.bucket_arn
  vpc_id              = module.vpc.vpc_id
  subnet_id           = module.vpc.public_subnet_ids[0]
  rds_secret_arn      = module.rds.secret_arn
  rds_secret_name     = module.rds.secret_name
  eventhub_secret_arn = module.eventhubs.secret_arn
  eventhub_secret_name = module.eventhubs.secret_name
  glue_number_of_workers = var.glue_number_of_workers
}
