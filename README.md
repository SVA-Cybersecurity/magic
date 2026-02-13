# MAGIC – Microsoft Azure Graph Information Crawler

MAGIC is a wrapper around the Microsoft Graph Python SDK designed to extract incident‑response‑relevant data from Microsoft 365 environments.
It assists analysts in exporting log data efficiently and produces a consolidated `.jsonl` file for ingestion into OpenSearch or Timesketch.

| :zap:        The main advantage is that the tool is written entirely in Python, so it works on almost any operating system. |
|-----------------------------------------------------------------------------------------------------------------------------|

## Getting Started in 60 Seconds

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install MAGIC
pip install git+https://github.com/magic-tool/magic.git

# 3. Initialize workspace (creates config + folders)
magic-init

# 4. Edit config.yaml:
#   - Add your M365 App credentials
#   - Add crawl modules (e.g. m365_signin) from available_crawls.yaml

# 5. Run MAGIC
magic
```

That’s it — you now have standardized M365 incident logs ready for analysis.

## 1. Requirements

### 1.1. Microsoft 365 Application (Required)

MAGIC requires an Entra ID **Application Registration** with:

- Application (client) ID
- Client secret
- Tenant ID
- Appropriate **Microsoft Graph permissions** depending on the crawl modules you want to run

MAGIC can automatically generate the application manifest with the required permissions for your configuration using:

```bash
magic --manifest
```

A full permissions manifest is available at the end of this document.

### 1.2. Python

- Python 3.11+
- A virtual environment is recommended

## 2. Installation

### 2.1. Create Virtual Environment (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2.2. Install MAGIC

```bash
pip install git+https://github.com/magic-tool/magic/.git
```

### 2.3. Initialize Workspace

```bash
magic-init
```

This creates the following structure:

```
.
├── logs                    # Log folder
├── output                  # Output folder
├── available_crawls.yaml   # All crawl modules
├── config.yaml             # Active configuration
```

## 3. Configuration

### 3.1. Application Credentials

This is required to download any information using the Microsoft Graph API.

```yaml
settings:
  auth:
    client_secret: "<your-secret>"
    client_id: "<application-id>"
    tenant_id: "<tenant-id>"
```

### 3.2. (Optional) Default Parameters

Default parameters can be used to configure settings for all crawl blocks.
Defaults can be overwritten within individual crawl blocks.

Parameter priority:

1. Crawl‑level value
2. Default value
3. Internal fallback

```yaml
settings:
  defaults:
    user_principal_names:
      - jdoe@tenant.com
    date_start: 2026-01-01 12:00:00
    date_end: 2026-01-12
```

> [!WARNING]
> Following date formats are accepted: `YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]`.
> When no time is specified the start time is set to `00:00:00` and the end time is set to `23:59:59`

### 3.3. (Optional) Permission Preflight Check

The permissions of the configured application will be validated before downloading logs.
Enable or disable via configuration:

```yaml
settings:
  permission_preflight_check: True
```

### 3.4. (Optional) IpAPI Enrichment

To enrich events using the IpAPI enricher module, configure the credentials:

```yaml
settings:
  ipapi:
    key: ExampleKey
    endpoint: "https://your-ipapi-endpoint"
    cert: False
```

## 4. Crawl Configuration

To download logs, multiple crawl modules can be configured.
A module may appear multiple times with different parameters.

Example:

```yaml
crawl:
  - type: m365_signin
    sign_in_type: user

  - type: m365_message_traces
    recipient_addresses:
      - john@tenant.com
```

All modules and parameters are documented in `available_crawls.yaml`.

## 5. CLI Usage

```bash
magic [-h] [-c CONFIG] [-o OUTPUT_DIR] [--reports-dir REPORTS_DIR] [--debug] [--manifest]

MAGIC - Microsoft Azure Graph Informations Crawler

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to configuration file (default: config.yaml in working directory or module directory)
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Directory for results (default: output in working directory or module directory)
  --reports-dir REPORTS_DIR
                        Directory for logs (default: logs in working directory or module directory)
  --debug               Enable debug logging
  --manifest            Generate permissions manifest
```

## 6. Output & Enrichment

Each crawl module creates its own output directory under the base output folder. Each crawl configuration is identified by a computed file identifier based on configured filters and parameters.

The base output is consolidated into a single `base.jsonl` file.

To ensure consistency:
- Output JSON contains **only one layer**
- Nested JSON objects are stored as JSON strings

Enricher modules can transform or enrich the output. Currently available:

- Timesketch transformer
- IPAPI enrichment
- Hash generator for integrity validation

Example:

```yaml
enrich:
  timesketch:
    enabled: True
    output_filename: timesketch.jsonl

  ipapi:
    enabled: True
    input_filename: timesketch.jsonl
    output_filename: ipapi.jsonl

  hash:
    enabled: True
    output_filename: hash.jsonl
    output_filename_csv: hash.csv
```

## 7. Full Example Workflow

To illustrate the tool’s workflow, here is a common M365 forensics scenario.

> [!NOTE]
> You can work without Timesketch and directly import the `base.jsonl` file into any analysis tool.

### 7.1. Goal
Collect all sign‑ins of `user1@contoso.com` between **2025‑12‑01** and **2025‑12‑10**, and convert the results into a Timesketch-compatible format.

### 7.2. Example config.yaml

> [!WARNING]
> Following date formats are accepted: `YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]`.
> When no time is specified the start time is set to `00:00:00` and the end time is set to `23:59:59`

```yaml
settings:
  auth:
    client_id: "<app-id>"
    client_secret: "<app-secret>"
    tenant_id: "<tenant-id>"

  defaults:
    date_start: 2025-12-01
    date_end: 2025-12-10
    user_principal_names:
      - user1@contoso.com

crawl:
  - type: m365_signin
    sign_in_type: user

enrich:
  timesketch:
    enabled: True
    output_filename: timesketch.jsonl
```

### 7.3. Run MAGIC

```bash
magic
```

### 7.4. What Happens Internally?

1. MAGIC loads and merges configuration values
2. OAuth2 client credential flow obtains a Microsoft Graph API token
3. Each crawl module calls its Graph API endpoint
4. All data is merged into `base.jsonl`
5. Enrichment modules run (Timesketch)
6. Final `timesketch.jsonl` file is ready for ingestion


## 8. Limitations

Some modules are limited by Microsoft Graph API restrictions.
If no date range is specified, default retention periods apply.
Available data may depend on the tenant’s license.


## 9. Permissions

All crawl modules declare the Microsoft Graph permissions they require.
You can validate your app permissions using:

```yaml
permission_preflight_check: True
```

To generate a permission manifest containing required permissions for your configuration:

```bash
magic --manifest
```

## 10. Contributing

Contributions are very welcome!

### How to contribute

1. **Report bugs** via GitHub Issues
2. **Propose new features** via Issues
3. **Submit Pull Requests**:
   - Use feature branches
   - Write clear commit messages
   - Update documentation when needed
