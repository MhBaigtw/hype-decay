output "deploy_role_arn" {
  description = "Least-privilege role every other module is applied as. Not for interactive use."
  value       = aws_iam_role.deploy.arn
}

output "how_to_use" {
  description = "Which profile applies which module."
  value = join("\n", [
    "bootstrap (this module): AWS_PROFILE=hype-decay        (human admin)",
    "everything else:         AWS_PROFILE=hype-decay-deploy (assumes the role below)",
    "the deploy role denies modifying itself, so it cannot apply this module",
  ])
}
