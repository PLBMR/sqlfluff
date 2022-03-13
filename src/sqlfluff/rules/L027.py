"""Implementation of Rule L027."""
from typing import List, Optional

from sqlfluff.core.dialects.common import AliasInfo, ColumnAliasInfo
from sqlfluff.core.rules.base import LintResult
from sqlfluff.core.parser import BaseSegment
from sqlfluff.rules.L020 import Rule_L020


class Rule_L027(Rule_L020):
    """References should be qualified if select has more than one referenced table/view.

    .. note::
       Except if they're present in a ``USING`` clause.

    **Anti-pattern**

    In this example, the reference ``vee`` has not been declared,
    and the variables ``a`` and ``b`` are potentially ambiguous.

    .. code-block:: sql

        SELECT a, b
        FROM foo
        LEFT JOIN vee ON vee.a = foo.a

    **Best practice**

    Add the references.

    .. code-block:: sql

        SELECT foo.a, vee.b
        FROM foo
        LEFT JOIN vee ON vee.a = foo.a
    """

    def _is_qualified_reference(
        self,
        reference: BaseSegment,
        table_alias_names: List[str],
        col_alias_names: List[str],
        using_cols: List[str],
        dialect_name: Optional[str],
    ) -> bool:
        """Returns true if the reference is qualified, else false."""
        return (
            reference.qualification() != "unqualified"
            # Allow unqualified columns that
            # are actually aliases defined
            # in a different select clause element.
            or reference.raw in col_alias_names
            # Allow columns defined in a USING expression.
            or reference.raw in using_cols
            # Allow unqualified columns that are table names in Redshift.
            # These can occur in references to SUPER data types in Redshift:
            # https://docs.aws.amazon.com/redshift/latest/dg/query-super.html
            or (dialect_name == "redshift" and reference.raw in table_alias_names)
        )

    def _lint_references_and_aliases(
        self,
        table_aliases: List[AliasInfo],
        standalone_aliases: List[str],
        references: List[BaseSegment],
        col_aliases: List[ColumnAliasInfo],
        using_cols: List[str],
        dialect_name: Optional[str],
        parent_select: Optional[BaseSegment],
    ) -> Optional[List[LintResult]]:
        # Do we have more than one? If so, all references should be qualified.
        if len(table_aliases) <= 1:
            return None
        table_alias_names = [table_alias.ref_str for table_alias in table_aliases]
        # A buffer to keep any violations.
        violation_buff = []
        # Check all the references that we have.
        for r in references:
            # Discard column aliases that
            # refer to the current column reference.
            col_alias_names = [
                c.alias_identifier_name
                for c in col_aliases
                if r not in c.column_reference_segments
            ]
            if not self._is_qualified_reference(
                r, table_alias_names, col_alias_names, using_cols, dialect_name
            ):
                violation_buff.append(
                    LintResult(
                        anchor=r,
                        description=f"Unqualified reference {r.raw!r} found in "
                        "select with more than one referenced table/view.",
                    )
                )

        return violation_buff or None
