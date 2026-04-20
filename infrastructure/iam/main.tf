terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 bucket (referenced in policy — create separately or import) ─────────────

data "aws_s3_bucket" "carbon_markets" {
  bucket = var.s3_bucket_name
}

# ── IAM policy — least privilege ──────────────────────────────────────────────
# Grants only what the ingestors and stream producer need:
#   - S3 PutObject / GetObject on the Bronze prefix only
#   - No IAM, no admin, no other services

resource "aws_iam_policy" "carbon_markets_ingestor" {
  name        = "carbon-markets-ingestor"
  description = "Least-privilege policy for EEX/EUTL batch ingestors and Finnhub stream producer"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
        ]
        Resource = [
          "${data.aws_s3_bucket.carbon_markets.arn}/bronze/*",
          "${data.aws_s3_bucket.carbon_markets.arn}/silver/*",
        ]
      },
      {
        Sid    = "S3ListBucket"
        Effect = "Allow"
        Action = "s3:ListBucket"
        Resource = data.aws_s3_bucket.carbon_markets.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["bronze/*", "silver/*"]
          }
        }
      },
      {
        Sid    = "SecretsManagerReadFinnhub"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:carbon-markets/finnhub-api-key*"
      },
      {
        Sid    = "SecretsManagerReadCloudflare"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:carbon-markets/cloudflare-tunnel-token*"
      }
    ]
  })
}

# ── IAM user ──────────────────────────────────────────────────────────────────

resource "aws_iam_user" "carbon_markets_ingestor" {
  name = "carbon-markets-ingestor"
  path = "/carbon-markets/"

  tags = {
    Project     = "carbon-markets"
    ManagedBy   = "terraform"
  }
}

resource "aws_iam_user_policy_attachment" "carbon_markets_ingestor" {
  user       = aws_iam_user.carbon_markets_ingestor.name
  policy_arn = aws_iam_policy.carbon_markets_ingestor.arn
}

# ── Access key ────────────────────────────────────────────────────────────────
# The secret is written to Terraform state — store state in S3 with encryption,
# or omit this block and create the key manually in the AWS console.

resource "aws_iam_access_key" "carbon_markets_ingestor" {
  user = aws_iam_user.carbon_markets_ingestor.name
}
