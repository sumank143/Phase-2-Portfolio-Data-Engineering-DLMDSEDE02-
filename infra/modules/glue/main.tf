variable "project"                { type = string }
variable "aws_region"             { type = string }
variable "s3_bucket"              { type = string }
variable "s3_bucket_arn"          { type = string }
variable "vpc_id"                 { type = string }
variable "subnet_id"              { type = string }
variable "rds_secret_arn"         { type = string }
variable "rds_secret_name"        { type = string }
variable "eventhub_secret_arn"    { type = string }
variable "eventhub_secret_name"   { type = string }
variable "glue_number_of_workers" { type = number; default = 2 }

# ── Security Group ───────────────────────────────────────
resource "aws_security_group" "glue" {
  name_prefix = "${var.project}-glue-"
  vpc_id      = var.vpc_id

  # Self-referencing rule — required for Glue VPC ENIs
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-glue-sg" }
}

resource "aws_glue_connection" "vpc" {
  name = "${var.project}-vpc-connection"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://placeholder:5432/accidentdb"
    USERNAME            = "placeholder"
    PASSWORD            = "placeholder"
  }
  physical_connection_requirements {
    availability_zone      = data.aws_subnet.selected.availability_zone
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = var.subnet_id
  }
}

data "aws_subnet" "selected" {
  id = var.subnet_id
}

# ── IAM Role ─────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "${var.project}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_custom" {
  name = "custom-permissions"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [var.s3_bucket_arn, "${var.s3_bucket_arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [var.rds_secret_arn, var.eventhub_secret_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:*"]
        Resource = ["*"]
      }
    ]
  })
}

# ── Upload Glue Scripts to S3 ────────────────────────────
locals {
  glue_scripts = fileset("${path.module}/../../glue_scripts", "*.py")
}

resource "aws_s3_object" "glue_scripts" {
  for_each = local.glue_scripts
  bucket   = var.s3_bucket
  key      = "glue_scripts/${each.value}"
  source   = "${path.module}/../../glue_scripts/${each.value}"
  etag     = filemd5("${path.module}/../../glue_scripts/${each.value}")
}

# ── PostgreSQL JDBC JAR ──────────────────────────────────
resource "aws_s3_object" "pg_jdbc" {
  bucket = var.s3_bucket
  key    = "glue_jars/postgresql-42.7.1.jar"
  source = "${path.module}/../../glue_scripts/postgresql-42.7.1.jar"
  etag   = filemd5("${path.module}/../../glue_scripts/postgresql-42.7.1.jar") 
}

# ── Streaming Jobs ───────────────────────────────────────
locals {
  streaming_jobs = {
    "job1-accident-kpi-geo"    = "job1_accident_kpi_geo.py"
    "job2-accident-conditions" = "job2_accident_conditions.py"
    "job3-accident-hotspots"   = "job3_accident_hotspots.py"
    "job4-vehicle-profile"     = "job4_vehicle_profile.py"
    "job5-extract"             = "job5_accident_vehicle_extract.py"
  }
}

resource "aws_glue_job" "streaming" {
  for_each = local.streaming_jobs

  name         = "${var.project}-${each.key}"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  timeout      = 2880
  connections  = [aws_glue_connection.vpc.name]

  command {
    name            = "gluestreaming"
    script_location = "s3://${var.s3_bucket}/glue_scripts/${each.value}"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"               = "s3://${var.s3_bucket}/glue_scripts/common.py,s3://${var.s3_bucket}/glue_scripts/schemas.py"
    "--extra-jars"                   = "s3://${var.s3_bucket}/glue_jars/postgresql-42.7.1.jar"
    "--additional-python-modules"    = "psycopg2-binary,boto3"
    "--kafka_secret_name"            = var.eventhub_secret_name
    "--pg_secret_name"               = var.rds_secret_name
    "--aws_region"                   = var.aws_region
    "--checkpoint_path"              = "s3://${var.s3_bucket}/checkpoints/${each.key}/"
    "--checkpoint_path_accidents"    = "s3://${var.s3_bucket}/checkpoints/${each.key}/accidents/"
    "--checkpoint_path_vehicles"     = "s3://${var.s3_bucket}/checkpoints/${each.key}/vehicles/"
    "--output_path_accidents"        = "s3://${var.s3_bucket}/processed/accidents/"
    "--output_path_vehicles"         = "s3://${var.s3_bucket}/processed/vehicles/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                      = "s3://${var.s3_bucket}/glue_temp/"
  }

  number_of_workers = var.glue_number_of_workers
  worker_type       = "G.1X"

  tags = { project = var.project }
}

# ── ML Training Job ──────────────────────────────────────
resource "aws_s3_object" "train_script" {
  bucket = var.s3_bucket
  key    = "glue_scripts/train.py"
  source = "${path.module}/../../glue_scripts/../ml_accidental_severity_placeholder_train.py"
  # Note: actual train.py is in ml_accidental_severity/ — uploaded separately
}

resource "aws_glue_job" "ml_train" {
  name     = "${var.project}-ml-train"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.s3_bucket}/glue_scripts/train.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--additional-python-modules" = "scikit-learn,pandas,joblib,boto3,pyarrow"
    "--S3_BUCKET"                 = var.s3_bucket
    "--AWS_DEFAULT_REGION"        = var.aws_region
    "--TempDir"                   = "s3://${var.s3_bucket}/glue_temp/"
  }

  max_capacity = 0.0625
  timeout      = 30

  tags = { project = var.project }
}

# ── Glue Trigger (every 15 minutes) ─────────────────────
resource "aws_glue_trigger" "ml_train_schedule" {
  name     = "${var.project}-ml-train-trigger"
  type     = "SCHEDULED"
  schedule = "cron(0/15 * * * ? *)"

  actions {
    job_name = aws_glue_job.ml_train.name
  }

  tags = { project = var.project }
}

output "glue_security_group_id" { value = aws_security_group.glue.id }
