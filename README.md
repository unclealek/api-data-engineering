# Cricket Data Extraction Tool

This tool extracts cricket batsmen rankings from the Cricbuzz API and saves the data to a CSV file.

## Setup

1. Create and activate the virtual environment:
   ```bash
   # Create virtual environment (already done)
   python3 -m venv extractor
   
   # Activate virtual environment
   source extractor/bin/activate  # On macOS/Linux
   # or
   extractor\Scripts\activate     # On Windows
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure your API credentials:
   - Edit `config.py` and replace the placeholder API key with your actual Cricbuzz API key
   - Get your API key from [RapidAPI](https://rapidapi.com/cricbuzz-cricket-p-default/api/cricbuzz-cricket/)

## Usage

Run the script:
```bash
python extract_data.py
```

The script will:
- Fetch ODI batsmen rankings from Cricbuzz API
- Extract rank, name, and country data
- Save results to `batsmen_rankings.csv`

## Security

- **Never commit your actual API keys to version control**
- The `config.py` file is included in `.gitignore` to prevent accidental commits
- Consider using environment variables for production deployments
- Rotate your API keys regularly

## Output

The script generates a CSV file with the following columns:
- `rank`: Player's ranking position
- `name`: Player's name
- `country`: Player's country

## Error Handling

The script includes basic error handling for:
- API request failures
- Empty data responses
- File writing errors
