variable "project"        { type = string }
variable "vpc_id"         { type = string }
variable "subnet_id"      { type = string }
variable "s3_bucket"      { type = string }
variable "kafka_brokers"  { type = string }
variable "kafka_password" { type = string; sensitive = true }

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "producer" {
  name_prefix = "${var.project}-producer-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-producer-sg" }
}

resource "aws_iam_role" "producer" {
  name = "${var.project}-producer-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "producer_s3" {
  name = "s3-read"
  role = aws_iam_role.producer.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = ["arn:aws:s3:::${var.s3_bucket}", "arn:aws:s3:::${var.s3_bucket}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "producer" {
  name = "${var.project}-producer-profile"
  role = aws_iam_role.producer.name
}

resource "aws_instance" "producer" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = "t3.micro"
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.producer.id]
  iam_instance_profile   = aws_iam_instance_profile.producer.name

  user_data = templatefile("${path.module}/../../templates/bootstrap.sh.tftpl", {
    s3_bucket      = var.s3_bucket
    kafka_brokers  = var.kafka_brokers
    kafka_password = var.kafka_password
  })

  tags = { Name = "${var.project}-producer" }
}

output "public_ip" { value = aws_instance.producer.public_ip }
