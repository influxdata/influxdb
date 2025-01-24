# providers.tf

terraform {
  backend "gcs" {
    bucket = "v3-pro-licensing-tfstate"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "random" {
  # No additional configuration needed
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = "${var.region}-a"
}