#!/bin/bash
# Databricks cluster-scoped init script to install unixODBC development files and R odbc package

set -euxo pipefail  # Exit on error, print commands, handle errors in pipes

echo "==> Updating package manager and installing unixODBC development libraries"
if command -v apt-get &> /dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y unixodbc-dev
elif command -v yum &> /dev/null; then
    sudo yum install -y unixODBC-devel
else
    echo "Unsupported package manager. Please install unixODBC manually."
    exit 1
fi

echo "==> Installing R packages: odbc and DBI from CRAN"
Rscript -e "install.packages(c('odbc','DBI'), repos='https://cloud.r-project.org', dependencies=TRUE)"

# Optional: Install a specific version
# Rscript -e "install.packages('remotes', repos='https://cloud.r-project.org')"
# Rscript -e "remotes::install_version('odbc', version = '1.3.2')"

echo "==> R ODBC setup completed successfully."
