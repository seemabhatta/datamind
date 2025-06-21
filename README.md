# NL2SQL Chat Application

A Streamlit-based application that allows users to interact with SQL databases using natural language queries. The application uses OpenAI's GPT model to convert natural language questions into SQL queries, execute them, and display the results in a user-friendly format.

## Features

- Natural language to SQL query conversion
- Interactive chat interface
- Data dictionary integration for better query understanding
- Sample data preview and analysis
- Support for SQLite databases
- Caching for improved performance

## Prerequisites

- Python 3.8+
- pip (Python package manager)
- OpenAI API key
- SQLite database

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd nl2sqlchat
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root and add your OpenAI API key:
```
OPENAI_API_KEY=your_openai_api_key_here
```

## Configuration

Edit the `config.py` file to set up your database and file paths:

```python
DB_PATH = "path/to/your/database.db"
TABLE_NAME = "your_table_name"
SAMPLE_DATA_FILENAME = "user_sample_data.csv"
DATA_DICT_FILENAME = "data-dict2.yaml"
SYSTEM_PROMPTS_DIR = "system-prompts"
LLM_MODEL = "gpt-4.1-nano"  # or another supported model
```

## Data Dictionary

The application uses a YAML-based data dictionary (`data-dict2.yaml`) to understand the database schema. Each field in the dictionary includes:

- Type (integer, string, float, etc.)
- Description
- Validation rules
- Category (categorical, continuous, identifier, etc.)
- Business rules
- Source
- Relationships (if any)

## Running the Application

```bash
streamlit run nl2sqlchatv4.py
```

## Usage

1. Start the application using the command above
2. The application will load the data dictionary and sample data
3. Type your natural language question in the chat interface
4. The application will convert your question to SQL, execute it, and display the results

## Project Structure

- `nl2sqlchatv4.py`: Main application file
- `config.py`: Configuration settings
- `data-dict2.yaml`: Data dictionary for the database schema
- `system-prompts/`: Directory containing system prompts for the LLM
- `sample-data/`: Directory containing sample data
- `cache_utils.py`: Caching utilities
- `file_utils.py`: File handling utilities
- `llm_util.py`: LLM interaction utilities
- `ui_components.py`: UI components for the Streamlit app

## Customization

You can customize the application by:

1. Modifying the system prompts in the `system-prompts/` directory
2. Updating the data dictionary to match your database schema
3. Adjusting the configuration in `config.py`

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines if applicable]
