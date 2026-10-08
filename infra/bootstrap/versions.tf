terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Separate state from the main module, in the same bucket.
  #
  # WHY THIS MODULE EXISTS SEPARATELY. It holds the deploy role and the deploy
  # role only. The main module is applied BY that role, and a role that can
  # rewrite its own permissions can escape every limit placed on it, so the main
  # module must not contain it. The deploy role also carries an explicit Deny on
  # modifying itself, which means these resources genuinely cannot be applied by
  # the pipeline identity even by accident.
  #
  # Apply this module as the human admin (AWS_PROFILE=hype-decay), rarely.
  # Apply everything else as the deploy role (AWS_PROFILE=hype-decay-deploy).
  backend "s3" {
    bucket       = "hype-decay-tfstate-820697996849"
    key          = "bootstrap/terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Module    = "bootstrap"
    }
  }
}

data "aws_caller_identity" "current" {}
