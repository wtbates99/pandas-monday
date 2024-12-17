# pandas-monday

A Python package that enables seamless integration between pandas DataFrames and Monday.com boards. This library allows you to easily read data from Monday.com boards into pandas DataFrames and write DataFrames back to Monday.com boards.

## Installation

```bash
pip install pandas-monday
```

## Quick Start

```python
import pandas as pd
from pandas_monday import MondayClient

# Initialize the client
monday_client = MondayClient(api_token="your-api-token")

# Read a board into a DataFrame
df = monday_client.read_board(board_id="your-board-id")

# Modify your DataFrame
# ... your modifications here ...

# Write back to Monday.com
monday_client.write_board(df, board_id="target-board-id")
```

## Features

- Read Monday.com boards into pandas DataFrames
- Write pandas DataFrames to new or existing Monday.com boards
- Support for all Monday.com column types
- Automatic type conversion between pandas and Monday.com
- Batch operations for efficient data transfer
- Error handling and validation

## Authentication

To use this package, you'll need a Monday.com API token. You can get one by:

1. Going to your Monday.com account
2. Clicking on your avatar in the bottom left
3. Going to Admin > API
4. Generating a new token

## Usage Examples

### Reading from Monday.com

```python
# Read specific columns
df = monday_client.read_board(
    board_id="your-board-id",
    columns=['name', 'status', 'numbers']
)

# Read with filtering
df = monday_client.read_board(
    board_id="your-board-id",
    filter_criteria={'status': 'Done'}
)
```

### Writing to Monday.com

```python
# Create a new board
monday_client.write_board(
    df,
    board_name="New Board",
    workspace_id="your-workspace-id"
)

# Update existing board
monday_client.write_board(
    df,
    board_id="existing-board-id",
    update_method="append"  # or "replace"
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
