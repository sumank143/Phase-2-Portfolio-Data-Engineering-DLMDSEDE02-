variable "project"                 { type = string }
variable "vpc_id"                  { type = string }
variable "subnet_ids"              { type = list(string) }
variable "db_username"             { type = string }
variable "db_password"             { type = string; sensitive = true }
variable "glue_security_group_id"  { type = string }
variable "rds_extra_ingress_cidrs" { type = list(string); default = [] }

resource "aws_db_subnet_group" "main" {
  name       = "${var.project}-db-subnet"
  subnet_ids = var.subnet_ids
  tags       = { Name = "${var.project}-db-subnet" }
}

resource "aws_security_group" "rds" {
  name_prefix = "${var.project}-rds-"
  vpc_id      = var.vpc_id

  # Allow Glue
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.glue_security_group_id]
  }

  # Allow extra CIDRs (developer IP)
  dynamic "ingress" {
    for_each = var.rds_extra_ingress_cidrs
    content {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-rds-sg" }
}

resource "aws_db_instance" "postgres" {
  identifier             = "${var.project}-db"
  engine                 = "postgres"
  engine_version         = "15.4"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  storage_encrypted      = true
  db_name                = "accidentdb"
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = true
  skip_final_snapshot    = true

  tags = { Name = "${var.project}-db" }
}

# Store credentials in Secrets Manager
resource "aws_secretsmanager_secret" "rds" {
  name        = "${var.project}/rds-credentials"
  description = "RDS PostgreSQL credentials"
}

resource "aws_secretsmanager_secret_version" "rds" {
  secret_id = aws_secretsmanager_secret.rds.id
  secret_string = jsonencode({
    host     = aws_db_instance.postgres.address
    port     = 5432
    dbname   = "accidentdb"
    username = var.db_username
    password = var.db_password
  })
}

output "endpoint"    { value = aws_db_instance.postgres.endpoint }
output "secret_arn"  { value = aws_secretsmanager_secret.rds.arn }
output "secret_name" { value = aws_secretsmanager_secret.rds.name }
