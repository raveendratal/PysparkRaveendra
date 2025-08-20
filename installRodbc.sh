#!/bin/bash
# Databricks cluster-scoped init script to install unixODBC development files and R odbc package

set -euxo pipefail

echo "==> Checking for R installation..."
if ! command -v Rscript &> /dev/null; then
    echo "Rscript not found. Please use a Databricks R runtime or install R manually."
    exit 1
fi

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
Rscript -e "options(repos = c(CRAN = 'https://cloud.r-project.org')); \
            if (!requireNamespace('odbc', quietly = TRUE)) install.packages('odbc', dependencies=TRUE); \
            if (!requireNamespace('DBI', quietly = TRUE)) install.packages('DBI', dependencies=TRUE)"

echo "==> R ODBC setup completed successfully."
