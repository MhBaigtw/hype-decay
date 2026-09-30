variable "project" {
  description = "Name prefix for every resource in this project."
  type        = string
  default     = "hype-decay"
}

variable "region" {
  description = "The only region this project uses."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.region == "us-east-1"
    error_message = "CLAUDE.md pins this project to us-east-1."
  }
}

variable "deploy_role_trusted_principals" {
  description = <<-EOT
    Who may assume the deployment role. Deliberately the human SSO admin role
    and nothing else: the account root ARN is NOT listed, because trusting it
    would let any principal in the account assume this role. If the
    AdministratorAccess permission set is recreated, this ARN changes and must
    be updated here.
  EOT
  type        = list(string)
  default = [
    "arn:aws:iam::820697996849:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_c0149b0ede28fc0a",
  ]
}

variable "backfill_instance_types" {
  description = <<-EOT
    The only EC2 instance types the deploy role may launch, per the CLAUDE.md
    backfill exception. Enforced as an IAM condition on ec2:RunInstances, which
    is what actually stops a larger instance being launched -- a Terraform
    variable alone would only stop Terraform.
  EOT
  type        = list(string)
  default     = ["c7g.medium", "c7g.large", "c7g.xlarge", "t4g.small", "t3.small"]
}
