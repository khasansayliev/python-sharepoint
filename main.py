def _python_type(self, key: str, value: Any) -> Any:
    """Convert SharePoint internal value to a clean Python type."""
    try:
        field_type = self._sp_cols[key]["type"]

        # Numeric fields
        if field_type in ("Number", "Currency"):
            return float(value)

        # DateTime fields
        elif field_type == "DateTime":
            match = self.DATE_PATTERN.search(value)
            if match:
                value = match.group(0)
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

        # Boolean fields
        elif field_type == "Boolean":
            return {"1": "Yes", "0": "No"}.get(value, "")

        # User and UserMulti fields
        elif field_type in ("User", "UserMulti"):
            if self.users and value in self.users["sp"]:
                return self.users["sp"][value]
            elif ";#" in value:
                parts = value.split(";#")
                # Even indices are IDs, odd indices are names
                users = [parts[i] for i in range(1, len(parts), 2) if parts[i]]
                return users if len(users) > 1 else users[0] if users else value
            return value

        # Lookup and LookupMulti fields — strip leading ID from "123;#Value"
        elif field_type in ("Lookup", "LookupMulti"):
            if ";#" not in value:
                return value
            parts = value.split(";#")
            # Even indices are IDs, odd indices are display values
            names = [parts[i] for i in range(1, len(parts), 2) if parts[i]]
            return names if len(names) > 1 else names[0] if names else value

        # MultiChoice fields — ";#Alice;#Bob;#" -> ["Alice", "Bob"]
        elif field_type == "MultiChoice":
            return [v for v in value.split(";#") if v.strip()]

        return value

    except (AttributeError, ValueError):
        return value
