use std::str::Chars;

#[derive(Debug, PartialEq, Clone)]
pub struct Point {
    pub series: String,
    pub time: i64,
    pub value: i64,
}

// TODO: have parse return an error for invalid inputs
pub fn parse(input: &str) -> Vec<Point> {
    let mut points = Vec::with_capacity(10000);
    let mut lines= input.lines();

    for line in lines {
        read_line(line, &mut points)
    }

    return points;
}

fn read_line(line: &str, points: &mut Vec<Point>) {
    let mut points = points;
    let mut chars = line.chars();
    let mut series = String::with_capacity(1000);
    while let Some(ch) = chars.next() {
        match ch {
            ' ' => read_fields(&series, &mut chars, &mut points),
            _ => series.push(ch),
        }
    }
}

fn read_fields(measurement_tags: &str, chars: &mut Chars, points: &mut Vec<Point>) {
    let mut chars = chars;
    let mut points = points;
    let mut field_name = String::with_capacity(100);

    let mut point_offset = points.len();

    while let Some(ch) = chars.next() {
        match ch {
            '=' => {
                read_value(&measurement_tags, field_name, &mut chars, &mut points);
                field_name = String::with_capacity(100);
            },
            _ => field_name.push(ch),
        }
    }

    let time = field_name.parse::<i64>().unwrap();

    while point_offset < points.len() {
        points[point_offset].time = time;
        point_offset += 1;
    }
}

fn read_value(measurement_tags: &str, field_name: String, chars: &mut Chars, points: &mut Vec<Point>) {
    let mut value = String::new();

    while let Some(ch) = chars.next() {
        match ch {
            ' ' => {
                let val = value.parse::<i64>().unwrap();
                points.push(Point{series: measurement_tags.to_string() + "\t" + &field_name, value: val, time: 0});
                return;
            },
            _ => value.push(ch),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_single_field() {
        let input = "foo asdf=23 1234";

        let vals = parse(input);
        assert_eq!(vals[0].series, "foo\tasdf");
        assert_eq!(vals[0].time, 1234);
        assert_eq!(vals[0].value, 23);
    }

    #[test]
    fn parse_two_fields() {
        let input = "foo asdf=23 bar=5 1234";

        let vals = parse(input);
        assert_eq!(vals[0].series, "foo\tasdf");
        assert_eq!(vals[0].time, 1234);
        assert_eq!(vals[0].value, 23);

        assert_eq!(vals[1].series, "foo\tbar");
        assert_eq!(vals[1].time, 1234);
        assert_eq!(vals[1].value, 5);
    }
}