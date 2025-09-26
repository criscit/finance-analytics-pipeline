#!/bin/bash
# Copy sample CSVs to IMPORT_CSV_PATH

set -e

# Check if IMPORT_CSV_PATH is set
if [ -z "$IMPORT_CSV_PATH" ]; then
    echo "Error: IMPORT_CSV_PATH environment variable is not set"
    echo "Please set it to your import directory, e.g.:"
    echo "export IMPORT_CSV_PATH=/path/to/your/import/directory"
    exit 1
fi

# Check if directory exists
if [ ! -d "$IMPORT_CSV_PATH" ]; then
    echo "Error: IMPORT_CSV_PATH directory does not exist: $IMPORT_CSV_PATH"
    echo "Please create the directory first"
    exit 1
fi

# Copy sample data
echo "Copying sample data to $IMPORT_CSV_PATH..."
cp sample_data/*.csv "$IMPORT_CSV_PATH/"

echo "Sample data copied successfully!"
echo "Files copied:"
ls -la "$IMPORT_CSV_PATH"/*.csv

echo ""
echo "You can now start the pipeline with:"
echo "docker compose up --build -d"



