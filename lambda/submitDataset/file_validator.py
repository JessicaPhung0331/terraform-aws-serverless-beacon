
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

    col_error = validate_req_cols(route_type, rows)
    if col_error:
        return col_error

    return validate_rows(route_type, rows)


def validate_req_cols(route_type, rows):
    """ Validate that the contents of a given file (rows) contain the required columns for the given route_type"""
    if not rows:
        return "File is empty."
    
    req_cols = REQUIRED_COLUMNS.get(route_type, set())
    missing_cols = req_cols - set(rows[0].keys())

    if missing_cols:
        missing_cols = list(missing_cols)
        print(f"Validation errors - missing columns: {missing_cols}")
        return f"Missing required columns: {sorted(missing_cols)}"
    
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
            errors.append("Invalid genotype value in one or more rows (expected 'AA', 'AB', 'BB', or 'NC').")

    if errors:
        print(f"Validation errors - invalid rows: {' '.join(errors)}")
        return f"Validation errors - invalid rows: {' '.join(errors)}"
    
    return None
