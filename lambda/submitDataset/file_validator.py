
REQUIRED_COLUMNS = {
    "sample": {
        "sample_id",
        "breed",
        "sex",
    },
    "snp": {
        "id",
        "chromosome",
        "coordinate",
        "allelea_top_base",
        "alleleb_top_base",
    },
    "genotype": {
        "id_ref",
        "value",
        "sample_id"
    }
    # Todo: "phenotypes": {
    #     "sample_name"
    # }
}

SNP_ID = ["snp", "genotype"]
SAMPLE_ID = ["sample", "genotype"]

def validate_file(route_type, rows):
    """ Validate that the contents of a given file (rows) match expected format of given route_type"""
    errors = []
    errors.append(validate_req_cols(route_type, rows))
    errors.append(validate_rows(route_type, rows))

    if errors := [e for e in errors if e]:
        return ", ".join(errors)

    return None


def validate_req_cols(route_type, rows):
    """ Validate that the contents of a given file (rows) contain the required columns for the given route_type"""
    if not rows:
        return "File is empty."
    
    req_cols = REQUIRED_COLUMNS.get(route_type, set())
    missing_cols = req_cols - set(rows[0].keys())

    if missing_cols:
        return f"Missing required columns: {', '.join(missing_cols)}"
    
    return None

def validate_rows(route_type, rows):
    """ Validate that the rows have the correct format for the given route_type"""
    errors = []

    if route_type in SNP_ID:
        if any(not (row.get("id") or row.get("id_ref")) for row in rows):
            errors.append("Missing required SNP ID in one or more rows.")
    if route_type in SAMPLE_ID:
        if any(not row.get("sample_id") for row in rows):
            errors.append("Missing required sample_id in one or more rows.")
    
    if route_type == "genotype":
        if any(row.get("value") not in ["AA", "AB", "BB", "NC"] for row in rows):
            errors.append("Invalid genotype value in one or more rows. Expected 'AA', 'AB', 'BB', or 'NC'.")
    
    return ", ".join(errors) if errors else None


if __name__ == "__main__":
    pass

    # # Test validate_rows - should fail (invalid genotype call)
    # genotype_rows = [
    #     {
    #         "id_ref": "rs123",
    #         "sample_id": "S1",
    #         "value": "AS",
    #     }
    # ]

    # print(validate_file("genotype", genotype_rows))

