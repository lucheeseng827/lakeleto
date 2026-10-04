//! `--flatten`: struct columns read as one top-level column per field — see
//! [`Source::flatten`](crate::source::Source::flatten).
//!
//! Every engine that flattens goes through [`leaves`], so they agree on what a flattened source
//! is: the names (`user.geo.lat`), the order (a struct's fields where the struct was), which values
//! are null (every field of a null struct), and which columns stay whole (lists and maps, which
//! would turn one row into many, and empty structs, which would vanish).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use crate::error::{EngineError, Result};
use crate::source::Flatten;

/// Between a struct's name and its field's in a flattened column name.
pub const SEPARATOR: &str = ".";

/// One column of a flattened schema, and where it comes from.
pub struct Leaf<'a> {
    /// Field names from the top-level column down: `["user", "geo", "lat"]`, or just `["id"]` for
    /// a column that was not a struct.
    pub path: Vec<&'a str>,
    /// For each struct above this column, top-level first, whether it can be null — the structs a
    /// reader has to look at to know this column's value is null.
    pub nullable_parents: Vec<bool>,
    /// The column it becomes: named by the joined path, and nullable when it or any struct above it
    /// is. A column that was not a struct is kept exactly as it was.
    pub field: FieldRef,
}

/// The columns `schema` flattens into, in order. Refuses a schema where two would share a name —
/// a `user.name` column beside a `user` struct with a `name` field — rather than let one shadow the
/// other in every sort, filter and projection that looks a column up by name.
pub fn leaves(schema: &Schema, flatten: Flatten) -> Result<Vec<Leaf<'_>>> {
    let max = flatten.max_levels().unwrap_or(usize::MAX);
    let mut out = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        walk(
            field,
            vec![field.name().as_str()],
            Vec::new(),
            max,
            &mut out,
        );
    }
    let mut seen = HashSet::with_capacity(out.len());
    for leaf in &out {
        if !seen.insert(leaf.field.name().as_str()) {
            return Err(EngineError::Other(format!(
                "flattening this source gives two columns named `{}` — read it with fewer \
                 flatten levels, or without flattening",
                leaf.field.name()
            )));
        }
    }
    Ok(out)
}

fn walk<'a>(
    field: &'a FieldRef,
    path: Vec<&'a str>,
    nullable_parents: Vec<bool>,
    max: usize,
    out: &mut Vec<Leaf<'a>>,
) {
    match field.data_type() {
        DataType::Struct(children) if spreads(children.len(), nullable_parents.len(), max) => {
            let mut parents = nullable_parents;
            parents.push(field.is_nullable());
            for child in children {
                let mut path = path.clone();
                path.push(child.name());
                walk(child, path, parents.clone(), max, out);
            }
        }
        _ if nullable_parents.is_empty() => out.push(Leaf {
            path,
            nullable_parents,
            field: Arc::clone(field),
        }),
        _ => {
            let nullable = field.is_nullable() || nullable_parents.contains(&true);
            let flat = Field::new(path.join(SEPARATOR), field.data_type().clone(), nullable)
                .with_metadata(field.metadata().clone());
            out.push(Leaf {
                path,
                nullable_parents,
                field: Arc::new(flat),
            });
        }
    }
}

/// Whether a struct with `fields` fields, `depth` structs down, is spread into its fields.
fn spreads(fields: usize, depth: usize, max: usize) -> bool {
    fields > 0 && depth < max
}

/// `schema` flattened as `flatten` says: the same `SchemaRef` when it is `None` or there is no
/// struct to spread, so a caller can tell nothing changed by pointer.
pub fn flatten_schema(schema: &SchemaRef, flatten: Option<Flatten>) -> Result<SchemaRef> {
    let Some(flatten) = flatten.filter(|_| has_structs(schema)) else {
        return Ok(Arc::clone(schema));
    };
    let fields: Vec<FieldRef> = leaves(schema, flatten)?
        .into_iter()
        .map(|leaf| leaf.field)
        .collect();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Rows read under `schema`, flattened as `flatten` says, with the flattened schema. Unchanged when
/// there is nothing to flatten.
pub fn flatten_rows(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    flatten: Option<Flatten>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let flat = flatten_schema(&schema, flatten)?;
    let Some(flatten) = flatten.filter(|_| !Arc::ptr_eq(&flat, &schema)) else {
        return Ok((schema, batches));
    };
    let batches = batches
        .iter()
        .map(|b| flatten_batch(b, &flat, flatten))
        .collect::<Result<_>>()?;
    Ok((flat, batches))
}

/// One batch spread into `flat`'s columns. Every column of a null struct comes out null —
/// [`StructArray::flatten`](arrow_array::StructArray::flatten) folds the struct's nulls into its
/// children — so a field never shows a value its struct did not have.
fn flatten_batch(batch: &RecordBatch, flat: &SchemaRef, flatten: Flatten) -> Result<RecordBatch> {
    let max = flatten.max_levels().unwrap_or(usize::MAX);
    let mut columns = Vec::with_capacity(flat.fields().len());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        spread(field, Arc::clone(column), 0, max, &mut columns)?;
    }
    // The row count is given, not inferred from the columns, so a batch with none survives too.
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(flat), columns, &options)
        .map_err(EngineError::arrow)
}

fn spread(
    field: &FieldRef,
    column: ArrayRef,
    depth: usize,
    max: usize,
    out: &mut Vec<ArrayRef>,
) -> Result<()> {
    match field.data_type() {
        DataType::Struct(children) if spreads(children.len(), depth, max) => {
            let Some(structs) = column.as_struct_opt() else {
                return Err(EngineError::Arrow(format!(
                    "column `{}` is declared a struct but holds {}",
                    field.name(),
                    column.data_type()
                )));
            };
            let (_, values) = structs.flatten();
            for (child, values) in children.iter().zip(values) {
                spread(child, values, depth + 1, max, out)?;
            }
        }
        _ => out.push(column),
    }
    Ok(())
}

fn has_structs(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Struct(c) if !c.is_empty()))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow_array::builder::NullBufferBuilder;
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, Float64Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow_schema::Fields;

    use super::*;

    /// A struct array's own validity, one flag per row.
    fn structs(fields: Fields, children: Vec<ArrayRef>, valid: &[bool]) -> StructArray {
        let mut nulls = NullBufferBuilder::new(valid.len());
        for v in valid {
            nulls.append(*v);
        }
        StructArray::new(fields, children, nulls.finish())
    }

    fn geo() -> Fields {
        Fields::from(vec![
            Field::new("lat", DataType::Float64, false),
            Field::new("lon", DataType::Float64, false),
        ])
    }

    fn user() -> Fields {
        Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("geo", DataType::Struct(geo()), true),
        ])
    }

    /// `id`, `user: {name, geo: {lat, lon}}` and a `tags` list, three rows. Row 1's `user` is null
    /// and row 2's `geo` is null — with values left in their children, which Arrow allows and
    /// flattening must not surface.
    fn batch() -> RecordBatch {
        let geos = structs(
            geo(),
            vec![
                Arc::new(Float64Array::from(vec![51.5, 48.8, 40.7])),
                Arc::new(Float64Array::from(vec![-0.1, 2.3, -74.0])),
            ],
            &[true, true, false],
        );
        let users = structs(
            user(),
            vec![
                Arc::new(StringArray::from(vec!["Ada", "Grace", "Linus"])),
                Arc::new(geos),
            ],
            &[true, false, true],
        );
        let tags = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user", DataType::Struct(user()), true),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(users),
                Arc::new(tags),
            ],
        )
        .unwrap()
    }

    fn names(schema: &Schema) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    fn levels(n: usize) -> Option<Flatten> {
        Some(Flatten::Levels(NonZeroUsize::new(n).unwrap()))
    }

    #[test]
    fn every_level_spreads_structs_in_place_and_leaves_lists_whole() {
        let b = batch();
        let (schema, batches) = flatten_rows(b.schema(), vec![b], Some(Flatten::All)).unwrap();
        assert_eq!(
            names(&schema),
            ["id", "user.name", "user.geo.lat", "user.geo.lon", "tags"]
        );
        assert_eq!(
            batches[0].schema(),
            schema,
            "batches carry the schema reported"
        );
        assert!(matches!(schema.field(4).data_type(), DataType::List(_)));
    }

    #[test]
    fn a_depth_stops_spreading_at_that_level() {
        let b = batch();
        let (schema, batches) = flatten_rows(b.schema(), vec![b], levels(1)).unwrap();
        assert_eq!(names(&schema), ["id", "user.name", "user.geo", "tags"]);
        assert!(matches!(schema.field(2).data_type(), DataType::Struct(_)));
        assert_eq!(batches[0].num_columns(), 4);
    }

    #[test]
    fn a_null_struct_nulls_every_field_under_it() {
        let b = batch();
        let (schema, batches) = flatten_rows(b.schema(), vec![b], Some(Flatten::All)).unwrap();
        let col = |name: &str| batches[0].column(schema.index_of(name).unwrap()).clone();
        // Row 1: `user` is null, so its name and coordinates are too — not "Grace", 48.8, 2.3.
        assert!(col("user.name").is_null(1));
        assert!(col("user.geo.lat").is_null(1));
        // Row 2: only `geo` is null.
        assert!(col("user.name").is_valid(2));
        assert!(col("user.geo.lon").is_null(2));
        assert!(col("user.geo.lat").is_valid(0));
        // A field under a nullable struct is reported nullable even when declared otherwise, or
        // the schema would promise something the rows break.
        assert!(schema.field_with_name("user.name").unwrap().is_nullable());
        assert!(!schema.field_with_name("id").unwrap().is_nullable());
    }

    #[test]
    fn a_schema_with_no_structs_is_returned_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let flat = flatten_schema(&schema, Some(Flatten::All)).unwrap();
        assert!(Arc::ptr_eq(&flat, &schema));
        assert!(Arc::ptr_eq(
            &flatten_schema(&schema, None).unwrap(),
            &schema
        ));
    }

    #[test]
    fn an_empty_struct_stays_a_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("meta", DataType::Struct(Fields::empty()), true),
            Field::new("user", DataType::Struct(user()), true),
        ]));
        let flat = flatten_schema(&schema, Some(Flatten::All)).unwrap();
        assert_eq!(
            names(&flat),
            ["meta", "user.name", "user.geo.lat", "user.geo.lon"]
        );
    }

    #[test]
    fn two_columns_with_one_name_are_refused() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user.name", DataType::Utf8, true),
            Field::new("user", DataType::Struct(user()), true),
        ]));
        let err = flatten_schema(&schema, Some(Flatten::All)).unwrap_err();
        assert!(
            err.to_string().contains("two columns named `user.name`"),
            "{err}"
        );
    }

    #[test]
    fn a_sliced_batch_flattens_its_own_rows() {
        let b = batch().slice(1, 2);
        let (schema, batches) = flatten_rows(b.schema(), vec![b], Some(Flatten::All)).unwrap();
        let name = batches[0].column(schema.index_of("user.name").unwrap());
        assert_eq!(name.len(), 2);
        assert!(name.is_null(0), "row 1 of the original, whose user is null");
        assert_eq!(name.as_string::<i32>().value(1), "Linus");
    }

    #[test]
    fn parse_reads_back_what_as_param_writes() {
        for f in [Flatten::All, Flatten::Levels(NonZeroUsize::new(2).unwrap())] {
            assert_eq!(Flatten::parse(&f.as_param()).unwrap(), Some(f));
        }
        assert_eq!(Flatten::parse("").unwrap(), Some(Flatten::All));
        assert_eq!(Flatten::parse("TRUE").unwrap(), Some(Flatten::All));
        assert_eq!(Flatten::parse("0").unwrap(), None);
        assert_eq!(Flatten::parse("none").unwrap(), None);
        assert!(Flatten::parse("-1").is_err());
        assert!(Flatten::parse("deep").is_err());
    }
}
