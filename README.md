# pandas-monday

The goal is to create a Python package that enables seamless integration between pandas DataFrames and Monday.com boards. This library allows you to easily read data from Monday.com boards into pandas DataFrames and write DataFrames back to Monday.com boards.

## Installation

```bash
pip install pandas-monday
```

## Quick Start

```python
import pandas_monday as pm

# Initialize client using environment variable MONDAY_API_TOKEN
client = pm.monday_pandas()

# Or explicitly pass your API token
client = pm.monday_pandas(api_token="your-api-token")

# Read from a board
df = client.read_board(board_id="your-board-id")
```

## Features

- Read Monday.com boards into pandas DataFrames
- Write pandas DataFrames to Monday.com boards
- Support for subitems
- Multiple write modes and overwrite options
- Automatic type conversion between pandas and Monday.com
- Error handling and validation

## Authentication

To use this package, you'll need a Monday.com API token. You can either:

1. Set it as an environment variable named `MONDAY_API_TOKEN`
2. Pass it directly when initializing the client

To get an API token:
1. Go to your Monday.com account
2. Click on your avatar in the bottom left
3. Go to Developer > API
4. Generate a new token

## Usage Examples

### Reading from Monday.com

```python
import pandas_monday as pm

client = pm.monday_pandas()

# Basic read
df = client.read_board(board_id="your-board-id")

# Read with subitems
df = client.read_board(
    board_id="your-board-id",
    include_subitems=True
)

# Read specific columns
df = client.read_board(
    board_id="your-board-id",
    columns=['name', 'status', 'numbers']
)
```

### Writing to Monday.com

```python
import pandas as pd
import pandas_monday as pm

client = pm.monday_pandas()

# Read your data
df = pd.read_csv("data.csv")

# Write to board
client.write_board(
    board_id="your-board-id",
    df=df,
    mode="replace",      # Options: "append", "replace"
    overwrite_type="archive"  # Options: "archive", "delete"
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
