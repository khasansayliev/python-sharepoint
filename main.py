elif field_type == "Calculated":
    # SharePoint encodes as "resulttype;#value"
    # e.g. "float;#3.14", "datetime;#2021-02-19 15:24:32", "boolean;#1", "string;#SomeText"
    if ";#" not in value:
        return value
    result_type, result_value = value.split(";#", 1)
    result_type = result_type.lower()

    if result_type == "float":
        return float(result_value)
    elif result_type == "datetime":
        match = self.DATE_PATTERN.search(result_value)
        if match:
            result_value = match.group(0)
        return datetime.strptime(result_value, "%Y-%m-%d %H:%M:%S")
    elif result_type == "boolean":
        return {"1": "Yes", "0": "No"}.get(result_value, "")
    else:
        # string or any unknown result type — return the value as-is
        return result_value
