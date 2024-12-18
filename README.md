# pandas-monday

The goal is to create a Python package that enables seamless integration between pandas DataFrames and Monday.com boards. This library allows you to easily read data from Monday.com boards into pandas DataFrames and write DataFrames back to Monday.com boards.

## Installation

```bash
pip install pandas-monday
```

## Quick Start

```python
from pandas_monday import monday_pandas

monday_client = monday_pandas(api_token="your-api-token")

df = monday_client.read_board(board_id="your-board-id")

```

## Features

- Read Monday.com boards into pandas DataFrames
- Automatic type conversion between pandas and Monday.com
- Error handling and validation

## Authentication

To use this package, you'll need a Monday.com API token. You can get one by:

1. Going to your Monday.com account
2. Clicking on your avatar in the bottom left
3. Going to Developer > API
4. Generating a new token

## Usage Examples

### Reading from Monday.com

```python
# Read specific columns
df = monday_client.read_board(
    board_id="your-board-id",
    columns=['name', 'status', 'numbers']
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
