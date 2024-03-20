def get_page(data, search_keyword: str, search_column: str, sort: bool, sort_column: str, page_index: int,
             page_size: int):
    if search_keyword:
        data = [item for item in data if
                any(search_keyword.lower() in value.lower() for value in item.values())]

        if search_column:
            data = [item for item in data if search_keyword.lower()
                    in str(item[search_column]).lower()]

    if (sort is not None) and sort_column:
        data = sorted(data, key=lambda x: (x[sort_column] is None, x[sort_column]), reverse=sort)

    total_result_details = len(data)

    if page_size > 0:
        start_index = (page_index - 1) * page_size
        end_index = start_index + page_size
        data = data[start_index:end_index]

    return {
        "total_result_details": total_result_details,
        "result_details": data,
    }
