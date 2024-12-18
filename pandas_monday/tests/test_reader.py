import pandas as pd
import pytest
from pandas_monday import reader, api, exceptions
from unittest.mock import Mock

COMPREHENSIVE_BOARD = {
    "data": {
        "boards": [
            {
                "name": "Test Board",
                "columns": [
                    {"id": "name", "title": "Name"},
                    {"id": "status", "title": "Status"},
                    {"id": "priority", "title": "Priority"},
                    {"id": "tags", "title": "Tags"},
                    {"id": "number", "title": "Number"},
                ],
                "items_page": {
                    "cursor": None,
                    "items": [
                        {
                            "id": "1",
                            "name": "Project A",
                            "group": {"title": "Group 1"},
                            "column_values": [
                                {"id": "name", "text": "Project A"},
                                {"id": "status", "text": "Done"},
                                {"id": "priority", "text": "High"},
                                {"id": "tags", "text": "urgent, critical"},
                                {"id": "number", "text": "42"},
                            ],
                            "subitems": [
                                {
                                    "id": "1.1",
                                    "name": "Task A1",
                                    "column_values": [
                                        {"id": "name", "text": "Task A1"},
                                        {"id": "status", "text": "Done"},
                                        {"id": "priority", "text": "Medium"},
                                        {"id": "tags", "text": "backend"},
                                        {"id": "number", "text": "10"},
                                    ],
                                },
                                {
                                    "id": "1.2",
                                    "name": "Task A2",
                                    "column_values": [
                                        {"id": "name", "text": "Task A2"},
                                        {"id": "status", "text": "Working"},
                                        {"id": "priority", "text": "Low"},
                                        {"id": "tags", "text": "frontend"},
                                        {"id": "number", "text": "20"},
                                    ],
                                },
                            ],
                        },
                        {
                            "id": "2",
                            "name": "Project B",
                            "group": {"title": "Group 2"},
                            "column_values": [
                                {"id": "name", "text": "Project B"},
                                {"id": "status", "text": "Working"},
                                {"id": "priority", "text": "Medium"},
                                {"id": "tags", "text": "maintenance"},
                                {"id": "number", "text": "17"},
                            ],
                            "subitems": [
                                {
                                    "id": "2.1",
                                    "name": "Task B1",
                                    "column_values": [
                                        {"id": "name", "text": "Task B1"},
                                        {"id": "status", "text": "Done"},
                                        {"id": "priority", "text": "High"},
                                        {"id": "tags", "text": "bugfix"},
                                        {"id": "number", "text": "30"},
                                    ],
                                },
                            ],
                        },
                    ],
                },
            }
        ]
    }
}


@pytest.fixture
def mock_api():
    api_mock = Mock(spec=api.monday_api)
    api_mock._execute_query.return_value = COMPREHENSIVE_BOARD
    return api_mock


def test_basic_board_read(mock_api):
    """Test reading board without subitems"""
    result = reader.read_board(mock_api, "123", columns=["Name", "Status", "Number"])

    expected = pd.DataFrame(
        {
            "Name": ["Project A", "Project B"],
            "Status": ["Done", "Working"],
            "Number": ["42", "17"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_read_with_subitems(mock_api):
    """Test reading board with subitems included"""
    result = reader.read_board(
        mock_api, "123", columns=["Name", "Status", "Priority"], include_subitems=True
    )

    expected = pd.DataFrame(
        {
            "Name": ["Project A", "Task A1", "Task A2", "Project B", "Task B1"],
            "Status": ["Done", "Done", "Working", "Working", "Done"],
            "Priority": ["High", "Medium", "Low", "Medium", "High"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_max_results(mock_api):
    """Test max_results parameter"""
    result = reader.read_board(
        mock_api, "123", columns=["Name", "Status"], max_results=1
    )

    expected = pd.DataFrame(
        {
            "Name": ["Project A"],
            "Status": ["Done"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_column_subset(mock_api):
    """Test selecting specific columns"""
    result = reader.read_board(mock_api, "123", columns=["Name", "Tags"])

    expected = pd.DataFrame(
        {
            "Name": ["Project A", "Project B"],
            "Tags": ["urgent, critical", "maintenance"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_invalid_column(mock_api):
    """Test error handling for invalid columns"""
    with pytest.raises(exceptions.monday_pandas_invalid_column_order):
        reader.read_board(mock_api, "123", columns=["NonexistentColumn"])
