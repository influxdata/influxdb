#[derive(Debug)]
pub struct Segment {}

// The GroupingStrategy determines which algorithm is used for calculating
// groups.
enum GroupingStrategy {
    // AutoGroup lets the executor determine the most appropriate grouping
    // strategy using heuristics.
    AutoGroup,

    // HashGroup specifies that groupings should be done using a hashmap.
    HashGroup,

    // SortGroup specifies that groupings should be determined by first sorting
    // the data to be grouped by the group-key.
    SortGroup,
}
