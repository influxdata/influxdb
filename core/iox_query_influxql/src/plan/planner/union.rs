use arrow::datatypes::Field;
use datafusion::{
    common::DFSchema,
    logical_expr::{
        LogicalPlan, binary::comparison_coercion, expr_rewriter::coerce_plan_expr_for_schema, union,
    },
};
use itertools::izip;

/// Union two [`LogicalPlan`]s, and type-coerce inputs.
///
/// The InfluxQL planner uses the type-coerced UNION plans when constructing the rest
/// of the logical plan. If the union is not type-coerced, then the remaining
/// consumers (e.g. sort, limit, group by, aggregates) may be invalid.
///
/// The upstream [`union`] API no longer provides type coercison at logical plan construction,
/// and instead it's deferred to the analyzer pass. Therefore, we are adding back the type-coercion
/// when constructing the union node.
pub(crate) fn union_and_coerce(
    prev: LogicalPlan,
    next: LogicalPlan,
) -> datafusion::common::Result<LogicalPlan> {
    let union_schema = coerce_union_schema(vec![&prev, &next]);
    let prev = coerce_plan_expr_for_schema(prev, &union_schema)?;
    let next = coerce_plan_expr_for_schema(next, &union_schema)?;
    union(prev, next)
}

/// Infalliable function that tries to get a common schema that is compatible
/// with all inputs of UNION. Fallback to using the first schema provided (leftmost).
///
/// Modified from the type coersion logic in datafusion, which is not public.
/// Reer to <https://github.com/apache/datafusion/blob/846befb6a620d3b8c0c7ff01be7c35c45fb72360/datafusion/optimizer/src/analyzer/type_coercion.rs#L812>
fn coerce_union_schema(inputs: Vec<&LogicalPlan>) -> DFSchema {
    let base_schema = inputs[0].schema();
    let mut union_datatypes = base_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut union_nullabilities = base_schema
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();
    let mut union_field_meta = base_schema
        .fields()
        .iter()
        .map(|f| f.metadata().clone())
        .collect::<Vec<_>>();

    let mut metadata = base_schema.metadata().clone();

    for plan in inputs.iter().skip(1) {
        let plan_schema = plan.schema();
        metadata.extend(plan_schema.metadata().clone());

        if plan_schema.fields().len() != base_schema.fields().len() {
            return (**base_schema).clone();
        }

        // coerce data type and nullablity for each field
        for (union_datatype, union_nullable, union_field_map, plan_field) in izip!(
            union_datatypes.iter_mut(),
            union_nullabilities.iter_mut(),
            union_field_meta.iter_mut(),
            plan_schema.fields().iter()
        ) {
            let Some(coerced_type) = comparison_coercion(union_datatype, plan_field.data_type())
            else {
                return (**base_schema).clone();
            };

            *union_datatype = coerced_type;
            *union_nullable = *union_nullable || plan_field.is_nullable();
            union_field_map.extend(plan_field.metadata().clone());
        }
    }
    let union_qualified_fields = izip!(
        base_schema.iter(),
        union_datatypes.into_iter(),
        union_nullabilities,
        union_field_meta.into_iter()
    )
    .map(|((qualifier, field), datatype, nullable, metadata)| {
        let mut field = Field::new(field.name().clone(), datatype, nullable);
        field.set_metadata(metadata);
        (qualifier.cloned(), field.into())
    })
    .collect::<Vec<_>>();

    let Ok(new_schema) = DFSchema::new_with_metadata(union_qualified_fields, metadata) else {
        return (**base_schema).clone();
    };
    new_schema
}
