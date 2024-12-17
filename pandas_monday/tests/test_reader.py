import pandas as pd
import pytest
from pandas_monday import reader, api, exceptions
from unittest.mock import Mock

BASIC_BOARD = {
    "data": {
        "boards": [
            {
                "name": "Basic Board",
                "columns": [
                    {"id": "name", "title": "Name"},
                    {"id": "status", "title": "Status"},
                    {"id": "number", "title": "Number"},
                ],
                "items_page": {
                    "cursor": None,
                    "items": [
                        {
                            "id": "1",
                            "name": "Task 1",
                            "group": {"title": "Group A"},
                            "column_values": [
                                {"id": "name", "text": "Task 1"},
                                {"id": "status", "text": "Done"},
                                {"id": "number", "text": "42"},
                            ],
                        },
                        {
                            "id": "2",
                            "name": "Task 2",
                            "group": {"title": "Group B"},
                            "column_values": [
                                {"id": "name", "text": "Task 2"},
                                {"id": "status", "text": "Working"},
                                {"id": "number", "text": "17"},
                            ],
                        },
                    ],
                },
            }
        ]
    }
}

NESTED_BOARD = {
    "data": {
        "boards": [
            {
                "name": "Nested Board",
                "columns": [
                    {"id": "name", "title": "Name"},
                    {"id": "priority", "title": "Priority"},
                ],
                "items_page": {
                    "cursor": None,
                    "items": [
                        {
                            "id": "1",
                            "name": "Parent 1",
                            "group": {"title": "Group A"},
                            "column_values": [
                                {"id": "name", "text": "Parent 1"},
                                {"id": "priority", "text": "High"},
                            ],
                            "subitems": [
                                {
                                    "id": "1.1",
                                    "name": "Child 1",
                                    "column_values": [
                                        {"id": "name", "text": "Child 1"},
                                        {"id": "priority", "text": "Medium"},
                                    ],
                                },
                                {
                                    "id": "1.2",
                                    "name": "Child 2",
                                    "column_values": [
                                        {"id": "name", "text": "Child 2"},
                                        {"id": "priority", "text": "Low"},
                                    ],
                                },
                            ],
                        }
                    ],
                },
            }
        ]
    }
}


@pytest.fixture
def mock_api():
    api_mock = Mock(spec=api.monday_api)
    return api_mock


def test_basic_board_transformation(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    result = reader.read_board(mock_api, "123", columns=["Name", "Status", "Number"])

    # Verify result
    expected = pd.DataFrame(
        {
            "Name": ["Task 1", "Task 2"],
            "Status": ["Done", "Working"],
            "Number": ["42", "17"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_nested_structure_transformation(mock_api):
    mock_api.execute_query.return_value = NESTED_BOARD

    result = reader.read_board(
        mock_api, "123", columns=["Name", "Priority"], include_subitems=True
    )

    expected = pd.DataFrame(
        {
            "Name": ["Parent 1", "Child 1", "Child 2"],
            "Priority": ["High", "Medium", "Low"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_filtered_data_transformation(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    expected = pd.DataFrame(
        {
            "Name": ["Task 1"],
            "Status": ["Done"],
            "Number": ["42"],
        }
    )

    result = reader.read_board(
        mock_api,
        "123",
        columns=["Name", "Status", "Number"],
        filter_criteria={"Status": "Done"},
    )

    pd.testing.assert_frame_equal(result, expected)


def test_max_results_limit(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    expected = pd.DataFrame(
        {
            "Name": ["Task 1"],
            "Status": ["Done"],
            "Number": ["42"],
        }
    )

    result = reader.read_board(
        mock_api, "123", columns=["Name", "Status", "Number"], max_results=1
    )

    pd.testing.assert_frame_equal(result, expected)


def test_progress_bar_disabled(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    # Specify the columns we want to test
    result = reader.read_board(
        mock_api, "123", columns=["Name", "Status", "Number"], progress_bar=False
    )

    expected = pd.DataFrame(
        {
            "Name": ["Task 1", "Task 2"],
            "Status": ["Done", "Working"],
            "Number": ["42", "17"],
        }
    )

    pd.testing.assert_frame_equal(result, expected)


def test_invalid_column_filter(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    with pytest.raises(exceptions.monday_pandas_invalid_column_order):
        reader.read_board(
            mock_api, "123", filter_criteria={"NonexistentColumn": "Value"}
        )


def test_multiple_filter_criteria(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    expected = pd.DataFrame(
        {
            "Name": ["Task 1"],
            "Status": ["Done"],
            "Number": ["42"],
        }
    )

    result = reader.read_board(
        mock_api,
        "123",
        columns=["Name", "Status", "Number"],
        filter_criteria={"Status": "Done", "Number": "42"},
    )

    pd.testing.assert_frame_equal(result, expected)


def test_filter_no_matches(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    result = reader.read_board(
        mock_api,
        "123",
        columns=["Name", "Status", "Number"],
        filter_criteria={"Status": "NonexistentStatus"},
    )

    assert len(result) == 0
    assert list(result.columns) == ["Name", "Status", "Number"]


def test_column_subset(mock_api):
    mock_api.execute_query.return_value = BASIC_BOARD

    expected = pd.DataFrame(
        {
            "Name": ["Task 1", "Task 2"],
            "Status": ["Done", "Working"],
        }
    )

    result = reader.read_board(mock_api, "123", columns=["Name", "Status"])
    pd.testing.assert_frame_equal(result, expected)
