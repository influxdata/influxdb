# Create service account for Cloud Run
resource "google_service_account" "cloud_run_sa" {
  account_id   = "license-service-sa"
  display_name = "License Service Cloud Run SA"
  project      = var.project_id
}

# Grant KMS permissions to the service account
resource "google_kms_crypto_key_iam_member" "crypto_key_signer" {
  crypto_key_id = "projects/${var.project_id}/locations/global/keyRings/pro-licensing/cryptoKeys/signing-key-1"
  role          = "roles/cloudkms.signer"
  member        = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# Grant access to the service account
resource "google_secret_manager_secret_iam_member" "secret_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.mailgun_api_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# Create Secret Manager secret for Mailgun API key
resource "google_secret_manager_secret" "mailgun_api_key" {
  secret_id = "mailgun-api-key"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.required_apis]
}

module "cloud_run" {
  source  = "GoogleCloudPlatform/cloud-run/google"
  version = "~> 0.10.0"

  depends_on = [google_secret_manager_secret_iam_member.secret_access]

  # Required variables
  service_name = "influxdb3-pro-licensing"
  project_id   = var.project_id
  location     = var.region
  image        = var.container_image

  service_account_email = google_service_account.cloud_run_sa.email

  env_vars = [{
    name  = "IFLX_PRO_LIC_DB_CONN_STRING"
    value = "postgres://postgres:postgres@10.83.0.3:5432"
    },
    {
      name  = "IFLX_PRO_LIC_HTTP_ADDR"
      value = ":8687"
    },
    {
      name  = "IFLX_PRO_LIC_EMAIL_VERIFICATION_URL"
      value = "https://licenses.enterprise.influxdata.com"
    }
  ]

  env_secret_vars = [{
    name = "IFLX_PRO_LIC_EMAIL_API_KEY"
    value_from = [
      {
        secret_key_ref = {
          key  = "latest"
          name = google_secret_manager_secret.mailgun_api_key.secret_id
        }
      }
    ]
  }]

  ports = {
    name = "http1"
    port = 8687
  }

  template_annotations = {
    "run.googleapis.com/network-interfaces" = jsonencode([
      {
        network    = "private-network"
        subnetwork = "private-network"
        tags       = ["db"]
      }
    ])
  }

  service_annotations = {
    "run.googleapis.com/ingress" : "all"
  }
}

# Enable required Google Cloud APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "secretmanager.googleapis.com", # Secret Manager API
    "run.googleapis.com",           # Cloud Run API
    # "sql-component.googleapis.com",     # Cloud SQL
    # "sqladmin.googleapis.com",         # Cloud SQL Admin
    # "vpcaccess.googleapis.com",        # VPC Access API
    # "servicenetworking.googleapis.com" # Service Networking API
  ])

  project = var.project_id
  service = each.key

  # Disable dependent services when disabling an API
  disable_dependent_services = true
  # Disable the service on destroy
  disable_on_destroy = false
}

resource "google_compute_network" "private_network" {
  provider = google-beta

  name = "private-network"
}

resource "google_compute_global_address" "private_ip_address" {
  provider = google-beta

  name          = "private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.private_network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  provider = google-beta

  network                 = google_compute_network.private_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "instance" {
  provider = google-beta

  name             = "private-instance-${random_id.db_name_suffix.hex}"
  region           = var.region
  database_version = "POSTGRES_16"

  depends_on = [google_service_networking_connection.private_vpc_connection]

  settings {
    tier = "db-g1-small" # HA, shared vCPUs, 1.7 GB RAM, 3 GB storage
    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
    ip_configuration {
      ipv4_enabled                                  = true
      private_network                               = google_compute_network.private_network.self_link
      enable_private_path_for_google_cloud_services = true
    }
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "02:00" # 2 AM UTC
      backup_retention_settings {
        retained_backups = 7
      }
    }
  }
}

# Create a read-only service account for the telemetry client.
# This is used to collect data for business metrics that are fed into
# Snowflake for analysis and Domo for visualization.
resource "google_service_account" "service_account" {
  account_id   = "telemetry-client-sa"
  display_name = "Telemetry Client Service Account"
}

resource "google_sql_user" "iam_service_account_user" {
  # Note: for Postgres only, GCP requires omitting the ".gserviceaccount.com" suffix
  # from the service account email due to length limits on database usernames.
  name     = trimsuffix(google_service_account.service_account.email, ".gserviceaccount.com")
  instance = google_sql_database_instance.instance.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}

# Add the Cloud SQL Instance User role
resource "google_project_iam_member" "cloudsql_sa_user" {
  for_each = toset(["roles/cloudsql.instanceUser", "roles/cloudsql.client"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.service_account.email}"
}