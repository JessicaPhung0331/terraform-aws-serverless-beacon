from typing import List, Union

from shared.apiutils import (
    AlphanumericFilter,
    CustomFilter,
    OntologyFilter,
    Operator,
)
from shared.utils import ENV_ATHENA
from shared.variantutils import perform_variant_search
from shared.apiutils import RequestParams, Granularity

from .genotype import Genotype
from .sample import Sample
from .snp import Snp

type_class = {
    "genotype": Genotype,
    "sample": Sample,
    "snp": Snp,
}
type_relations_table_id = {
    "genotype": "sample_id",
    "sample": "sample_id",
    "snp": "id",
}


# given a dict <f>={"operator":X,"value":Y} return appropriate SQL fragment "<operator> <value>"
def _get_comparison_operator(filter: Union[AlphanumericFilter, OntologyFilter]):
    if isinstance(filter.value, int) or isinstance(filter.value, float):
        # infer a numeric comparison
        return "!=" if filter.operator == Operator.NOT else filter.operator
    # infer an alphanumeric comparison
    return "LIKE" if filter.operator == Operator.EQUAL else "NOT LIKE"


def entity_search_conditions(
    filters: List[Union[OntologyFilter, AlphanumericFilter, CustomFilter]],
    id_type: str,
    default_scope: str,
    id_modifier="id",
    with_where=True,
    request: RequestParams | None = None,
):
    # arrays to gradually form the SQL expression
    join_constraints = []
    outer_constraints = []
    # using execution parameters to separately pass to boto3 for it to do SQL sanitization
    join_execution_parameters = []
    outer_execution_parameters = []
    filter_samples = []

    if request is not None and request.query.request_parameters is not None:
        g_variant = request.query.request_parameters

        query_responses = perform_variant_search(
            reference_name=g_variant.reference_name,
            reference_bases=g_variant.reference_bases,
            alternate_bases=g_variant.alternate_bases,
            start=g_variant.start,
            end=g_variant.end,
            requested_granularity=Granularity.RECORD,
            include_datasets=request.query.include_resultset_responses,
            include_samples=True,
        )

        sample_names_relevant_to_variant = set()
        for query_response in query_responses:
            sample_names_relevant_to_variant.update(query_response.sample_names)

        filter_samples = list(sample_names_relevant_to_variant)

        if filter_samples:
            # Everything is joined by the sample id (unique), so we don't need an explicit relations table
            # This is only needed if we are looking for an individual at some point
            values = ", ".join(["(?)" for _ in filter_samples])

            join_constraints.append(
                f"SELECT sample_id FROM (VALUES {values}) AS t(sample_id)")

            join_execution_parameters += filter_samples
        else:
            # if there are no samples relevant to the variant, then we can return a constraint that will yield no results
            outer_constraints.append("1=0")

    for f in filters:
        if isinstance(f, AlphanumericFilter):
            # check to see if the field is in default scope
            # karyotypicSex = "XX" for default scope (Individuals)
            if f.scope is None or f.scope == default_scope:
                filter_id = f.id.strip().replace(" ", "")

                # naive comparison (JSON will not be in our data)
                operator = _get_comparison_operator(f)
                outer_constraints.append("{} {} ?".format(filter_id, operator))
                outer_execution_parameters.append(f"'{str(f.value)}'")
            # otherwise, we have to use the relations table
            # eg: scope = "cohorts", cohortType = "beacon-defined"
            else:
                group = f.scope
                joined_class = type_class[group]
                filter_id = f.id.strip().replace(" ", "")

                operator = _get_comparison_operator(f)
                comparison = "{} {} ?".format(filter_id, operator)
                join_execution_parameters.append(f"'{str(f.value)}'")
                join_constraints.append(
                    f""" SELECT RI.{type_relations_table_id[id_type]} FROM "{ENV_ATHENA.ATHENA_RELATIONS_TABLE}" RI JOIN "{joined_class._table_name}" TN ON RI.{type_relations_table_id[group]}=TN.id WHERE TN.{comparison} """
                )

    # format fragments together to form coherent SQL expression
    join_constraints = " INTERSECT ".join(join_constraints)
    join_constraints = (
        f"{id_modifier} IN ({join_constraints}) " if join_constraints else ""
    )
    total_constraints = (
        [join_constraints] if join_constraints else []
    ) + outer_constraints
    total_constraints = " AND ".join(total_constraints)
    execution_parameters = join_execution_parameters + outer_execution_parameters

    if total_constraints:
        return (
            ("WHERE " if with_where else "") + total_constraints,
            execution_parameters if execution_parameters else None,
        )
    else:
        return "", None
