variable "project" { type = string }

resource "aws_s3_bucket" "data" {
  bucket        = "${var.project}-data-${random_id.suffix.hex}"
  force_destroy = true
  tags          = { Name = "${var.project}-data" }
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "https_only" {
  bucket = aws_s3_bucket.data.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EnforceHTTPS"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = ["${aws_s3_bucket.data.arn}", "${aws_s3_bucket.data.arn}/*"]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
}

# Create folder prefixes
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "processed/accidents/", "processed/vehicles/", "models/", "checkpoints/", "glue_scripts/"])
  bucket   = aws_s3_bucket.data.id
  key      = each.value
  content  = ""
}

output "bucket_name" { value = aws_s3_bucket.data.bucket }
output "bucket_arn"  { value = aws_s3_bucket.data.arn }
