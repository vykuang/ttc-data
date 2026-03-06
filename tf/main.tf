terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {}

resource "aws_s3_bucket" "ttc" {
  bucket = "vb2k-ttc-api"

  tags = {
    Name        = "TTC API data lake"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "translink" {
  bucket = "vb2k-translink-api"

  tags = {
    Name        = "Translink API data lake"
    Environment = "Dev"
  }
}