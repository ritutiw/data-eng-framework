# Azure ADLS Gen2 Authentication

kafka-delta-sink uses [delta-rs](https://delta-io.github.io/delta-rs/) which leverages the Rust `object_store` crate for Azure authentication. No `azure-storage-file-datalake` SDK is needed.

## Methods

### 1. Service Principal (OAuth2 Client Credentials)

Best for production deployments with Azure AD app registrations.

```yaml
azure:
  account_name: "mystorageaccount"
  auth_method: "service_principal"
  client_id: "<app-registration-client-id>"
  client_secret: "<app-registration-client-secret>"
  tenant_id: "<azure-ad-tenant-id>"
```

Or via environment variables:
```bash
export AZURE_ACCOUNT_NAME=mystorageaccount
export AZURE_AUTH_METHOD=service_principal
export AZURE_CLIENT_ID=<client-id>
export AZURE_CLIENT_SECRET=<client-secret>
export AZURE_TENANT_ID=<tenant-id>
```

### 2. Storage Account Key

Simplest method, suitable for development and testing.

```yaml
azure:
  account_name: "mystorageaccount"
  auth_method: "account_key"
  account_key: "<storage-account-access-key>"
```

### 3. SAS Token

Scoped access with time-limited tokens.

```yaml
azure:
  account_name: "mystorageaccount"
  auth_method: "sas_token"
  sas_token: "sv=2021-06-08&ss=b&srt=sco&sp=rwdlac&se=2026-12-31..."
```

### 4. Azure CLI

Uses credentials from `az login`. Good for local development.

```yaml
azure:
  account_name: "mystorageaccount"
  auth_method: "cli"
```

## Required ADLS Permissions

The identity (service principal, SAS token, etc.) needs:

- **Storage Blob Data Contributor** role on the container
- Or at minimum: Read, Write, Create, Delete on blobs within the target path

## URI Formats

All of the following URI schemes are supported:

```
abfss://container@account.dfs.core.windows.net/path/to/table
abfs://container@account.dfs.core.windows.net/path/to/table
az://container/path/to/table
```
