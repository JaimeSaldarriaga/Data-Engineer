Please review this link:

- [Looker Studio Report](https://lookerstudio.google.com/reporting/13dfca22-b0a6-4f92-8db3-7247459d4c5e)

### Folder and File Description

- **`data/`**: Contains input data, such as the file `btc-news-recent-f.csv`, which appears to be a dataset related to recent Bitcoin news.
  
- **`env/`**: Includes a Python virtual environment with binaries, configurations, and libraries needed to run the project.

- **`extract/`**: Contains scripts for data extraction:
  - `batch_ingestion.py`: Likely handles batch data ingestion.
  - `realtime_ingestion.py`: Possibly handles real-time data ingestion.
  - `utils.py`: Might include helper functions for the extraction processes.
  - `credentials.json` and `rav.yaml`: Configuration and credentials files.

- **`transform/`**: Contains scripts for data transformation:
  - `transform.py`: Performs transformations on the extracted data.
  - `credentials.json` and `rav.yaml`: Configuration and credentials files.

- **Root files**:
  - `.gitignore`: Specifies which files or folders should be ignored by Git.
  - `README.md`: Main file for documenting the project.
  - `TEST.md`: Possibly contains information or tests related to the project.

### Requirements

The project uses a Python virtual environment, so it must be activated before running any scripts. To activate it, use the following command depending on your operating system:

- **Linux/MacOS**:
  ```bash
  source env/bin/activate
