import pytest
from pandas_monday import reader, api, exceptions
from unittest.mock import Mock

# Enhance the test data to cover more cases
TEST_BOARD_DATA = {
    "data": {
        "boards": [
            {
                "name": "Test Board",
                "columns": [
                    {"id": "name", "title": "Name"},
                    {"id": "status", "title": "Status"},
                    {"id": "date", "title": "Date"},
                ],
                "items_page": {
                    "cursor": None,
                    "items": [
                        {
                            "id": "1",
                            "name": "Item 1",
                            "group": {"title": "Group 1"},
                            "column_values": [
                                {"id": "name", "text": "Item 1"},
                                {"id": "status", "text": "Done"},
                                {"id": "date", "text": "2024-03-20"},
                            ],
                            "subitems": [
                                {
                                    "id": "1.1",
                                    "name": "Subitem 1",
                                    "column_values": [
                                        {"id": "name", "text": "Subitem 1"},
                                        {"id": "status", "text": "In Progress"},
                                        {"id": "date", "text": "2024-03-21"},
                                    ],
                                }
                            ],
                        }
                    ],
                },
            }
        ]
    }
}


@pytest.fixture
def mock_executor():
    executor = Mock(spec=api.monday_api)
    executor.execute_query.return_value = TEST_BOARD_DATA
    return executor


def test_read_board_basic(mock_executor):
    result = reader.read_board(mock_executor, "123", columns=["Name", "Status", "Date"])

    mock_executor.execute_query.assert_called()

    assert list(result.columns) == ["Name", "Status", "Date"]
    assert len(result) == 1
    assert result.iloc[0].to_dict() == {
        "Name": "Item 1",
        "Status": "Done",
        "Date": "2024-03-20",
    }


def test_read_board_with_subitems(mock_executor):
    result = reader.read_board(
        mock_executor, "123", columns=["Name", "Status", "Date"], include_subitems=True
    )

    assert len(result) == 2  # Main item + subitem
    assert result.iloc[1].to_dict()["Name"] == "Subitem 1"
    assert result.iloc[1].to_dict()["Status"] == "In Progress"


def test_read_board_with_filter(mock_executor):
    result = reader.read_board(
        mock_executor,
        "123",
        columns=["Name", "Status"],
        filter_criteria={"Status": "Done"},
    )

    assert len(result) == 1
    assert result.iloc[0]["Status"] == "Done"


def test_read_board_invalid_column(mock_executor):
    with pytest.raises(exceptions.monday_pandas_invalid_column_order):
        reader.read_board(mock_executor, "123", columns=["InvalidColumn"])


def test_read_board_not_found(mock_executor):
    mock_executor.execute_query.return_value = {"data": {"boards": []}}

    with pytest.raises(exceptions.monday_pandas_board_not_found_error):
        reader.read_board(mock_executor, "999")


def test_read_board_api_error(mock_executor):
    mock_executor.execute_query.side_effect = exceptions.monday_pandas_api_error(
        "API Error"
    )

    with pytest.raises(exceptions.monday_pandas_api_error):
        reader.read_board(mock_executor, "123")
