# CNPJ Data Extractor

### Project Overview

The **CNPJ Data Extractor** is an open-source project that automates the process of downloading, extracting, and transforming CNPJ (Brazilian tax ID) datasets from publicly available sources. The project is divided into two parts:
1. **Data Extraction**: Automatically download and extract the partitioned CNPJ datasets.
2. **Data Merging**: Combine the partitioned tables into consolidated datasets for further processing or analysis.

### Features

- **Automated Data Download**: Multithreaded downloading of datasets with support for remote file size checking to avoid redundant downloads.
- **Efficient Data Processing**: Handles large partitioned datasets and consolidates them into a single output.
- **Flexible Export Formats**: Data can be exported in CSV, Parquet, JSON, or Feather formats for easy integration with databases and analytics platforms.
- **Modular Configuration**: Paths, logging, and export options are easily adjustable through a configuration file (`config.yaml`).

### Project Structure

```bash
.
├── config
  ├── config.yaml         # Configuration file for paths, formats, and data types
├── data_incoming         # Folder for incoming ZIP data files
├── data_outgoing         # Folder for processed output data
├── logs                  # Folder for log files
├── scrips                # Folder for python scripts
  ├── cnpj_extractor.py   # Script for data extraction (part 1)
  ├── cnpj_merger.py      # Script for merging partitioned tables (part 2)
├── README.md             # Project documentation
```

### Getting Started

#### Prerequisites

- **Python** 3.8+
- The following Python packages:
  - `pandas`
  - `pyyaml`
  - `requests`
  - `bs4` (BeautifulSoup)
  - `tqdm`

You can install the dependencies using the following command:

```bash
pip install -r requirements.txt
```

#### Configuration

Before running the scripts, ensure that the `config.yaml` file is set up according to your environment. This file contains paths for data storage, logging, and format settings for exporting data.

**Example `config.yaml`**:

```yaml
# Base URL for CNPJ dataset
base_url: 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/'

# CSV Settings
csv_sep: ';'
csv_dec: ','
csv_quote: '"'
csv_enc: 'cp1252'

# Export format: Choose between 'csv', 'parquet', 'json', or 'feather'
export_format: 'parquet'

# Data type definitions for each table
dtypes:
  empresas:
    cnpj_basico: "str"
    razao_social: "str"
    natureza_juridica: "str"
    qualificacao_do_responsavel: "str"
    capital_social: "str"
    porte_da_empresa: "str"
    ente_federativo_resposavel: "str"

  estabelecimentos:
    cnpj_basico: "str"
    cnpj_ordem: "str"
    cnpj_dv: "str"
    identificador_matriz_filial: "str"
    nome_fantasia: "str"
    situacao_cadastral: "str"
    data_situacao_cadastral: "str"
    motivo_situacao_cadastral: "str"
    nome_da_cidade_no_exterior: "str"
    pais: "str"
    data_de_inicio_da_atividade: "str"
    cnae_fiscal_principal: "str"
    cnae_fiscal_secundaria: "str"
    tipo_de_logradouro: "str"
    logradouro: "str"
    numero: "str"
    complemento: "str"
    bairro: "str"
    cep: "str"
    uf: "str"
    municipio: "str"
    situacao_cadastral: "str"
    ddd1: "str"
    telefone1: "str"
    ddd2: "str"
    telefone2: "str"
    ddd_do_fax: "str"
    fax: "str"
    correio_eletronico: "str"
    situacao_especial: "str"
    data_da_situacao_especial: "str"
  ...
```

### Part 1: Data Extraction

To start the data extraction process, run the `cnpj_extractor.py` script. This script will download the CNPJ dataset for the latest available month, check file sizes, and avoid re-downloading files that already exist.

**Run the extraction:**

```bash
python cnpj_extractor.py
```

This will:
- Download the partitioned datasets from the latest available month on the server.
- Save them to the folder defined in `config.yaml` as `data_incoming`.

### Part 2: Data Merging

After extracting the data, the second part involves merging the partitioned tables into consolidated datasets.

To perform the merging, run the `cnpj_merger.py` script. This script reads the partitioned data files, processes them, and saves the merged data into the `data_outgoing` folder in the format you specified in `config.yaml`.

**Run the merging process:**

```bash
python cnpj_merger.py
```

This will:
- Merge partitioned data files (e.g., Empresas, Estabelecimentos, Socios) into a single dataset.
- Save the merged dataset to the `data_outgoing` folder in your preferred format.

### Customizing the Export Format

You can easily change the format of the exported files (e.g., CSV, Parquet, JSON) by modifying the `export_format` value in `config.yaml`. The supported formats are:
- `csv`
- `parquet`
- `json`
- `feather`

### Logging

Logs are generated in the `logs` folder, with details on the download, extraction, and merging processes. This helps in tracking the progress of the data extraction and merging activities.

### Contributing

Contributions are welcome! If you encounter any bugs or have suggestions for new features or improvements, feel free to submit an issue or open a pull request.

### License

This project is licensed under the MIT License.

---

### Notes for Improvement

- **Error Handling**: Further improvements could be added, such as retry mechanisms during downloads or handling of partially downloaded files.
- **Parallel Processing**: The merging process can also be optimized using parallel processing techniques for handling large datasets.

Let me know if you need additional details or further modifications to the README!

