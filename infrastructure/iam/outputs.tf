output "iam_user_name" {
  description = "IAM username"
  value       = aws_iam_user.carbon_markets_ingestor.name
}

output "iam_user_arn" {
  description = "IAM user ARN"
  value       = aws_iam_user.carbon_markets_ingestor.arn
}

output "access_key_id" {
  description = "AWS Access Key ID — add to GitHub Secrets and Pi credentials"
  value       = aws_iam_access_key.carbon_markets_ingestor.id
}

output "secret_access_key" {
  description = "AWS Secret Access Key — store securely, shown once"
  value       = aws_iam_access_key.carbon_markets_ingestor.secret
  sensitive   = true
}
