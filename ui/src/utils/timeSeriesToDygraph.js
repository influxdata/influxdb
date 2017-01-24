import _ from 'lodash';
import {STROKE_WIDTH} from 'src/shared/constants';
/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

// activeQueryIndex is an optional argument that indicated which query's series
// we want highlighted.
export default function timeSeriesToDygraph(raw = [], activeQueryIndex, isInDataExplorer) {
  // const labels = []; // all of the effective field names (i.e. <measurement>.<field>)
  const fieldToIndex = {}; // see parseSeries
  const dates = {}; // map of date as string to date value to minimize string coercion
  const dygraphSeries = {}; // dygraphSeries is a graph legend label and its corresponding y-axis e.g. {legendLabel1: 'y', legendLabel2: 'y2'};

  /**
   * dateToFieldValue will look like:
   *
   * {
   *   Date1: {
   *     effectiveFieldName_1: ValueForField1AtDate1,
   *     effectiveFieldName_2: ValueForField2AtDate1,
   *     ...
   *   },
   *   Date2: {
   *     effectiveFieldName_1: ValueForField1AtDate2,
   *     effectiveFieldName_2: ValueForField2AtDate2,
   *     ...
   *   }
   * }
   */
  const dateToFieldValue = {};

  const results = raw.reduce((acc, response) => {
    return [...acc, ..._.get(response, 'response.results', [])]
  }, [])

  const serieses = results.reduce((acc, result, index) => {
    return [...acc, ...result.series.map((item) => ({...item, index}))];
  }, [])

  const cells = serieses.reduce((acc, {name, columns, values, index}) => {
    const rows = values.map((values) => ({
      name,
      columns,
      values,
      index,
    }))
    console.log("HI IM Rerws: ", rows)

    rows.forEach(({values: vals, columns: cols, name: n, index: seriesIndex}) => {
      const [time, ...rowValues] = vals
      rowValues.forEach((value, i) => {
        const column = cols[i + 1]
        acc.push({
          label: `${n}.${column}`,
          value,
          time,
          seriesIndex,
        })
      })
    })

    return acc
  }, [])

  console.log("HI IM CELLS: ", cells)

  const labels = cells.reduce((acc, cell) => {
    const existingLabel = acc.find(({label, seriesIndex}) => cell.label === label && cell.seriesIndex === seriesIndex)

    if (!existingLabel) {
      acc.push({
        label: cell.label,
        seriesIndex: cell.seriesIndex,
      })
    }

    return acc
  }, [])

  const sortedLabels = _.sortBy(labels, 'label')

  const timeSeries = cells.reduce((acc, cell) => {
    let existingRowIndex = acc.findIndex(({time}) => cell.time === time)

    if (existingRowIndex === -1) {
      acc.push({
        time: cell.time,
        values: Array(sortedLabels.length).fill(null),
      })

      existingRowIndex = acc.length - 1
    }

      const values = acc[existingRowIndex].values
      const labelIndex = sortedLabels.findIndex(({label}) => label === cell.label)
      values[labelIndex] = cell.value
      acc[existingRowIndex].values = values

    return acc
  }, [])

  const sortedTimeSeries = _.sortBy(timeSeries, 'time')

  const timeSeriesToDygraph = {
    timeSeries: sortedTimeSeries.map(({time, values}) => ([new Date(time), ...values])),
    labels: ["time", ...sortedLabels.map(({label}) => label)],
  }


  // console.log("MY CAT LOVES LABELS: ", labels)
  // console.log("MY CAT HATES SORTED LABELS: ", sortedLabels)
 console.log("sorted term serrrrries: ", JSON.stringify(timeSeriesToDygraph, null, 2))

  return timeSeriesToDygraph;
// timeSeriesToDygraph , {labels: [], timeSeries: []}

  // raw.forEach(({response}, queryIndex) => {
  //   // If a response is an empty result set or a query returned an error
  //   // from InfluxDB, don't try and parse.
  //   if (response.results.length) {
  //     if (isEmpty(response) || hasError(response)) {
  //       return;
  //     }
  //   }
  //
  //   /**
  //    * response looks like:
  //    * {
  //    *   results: [
  //    *     { series: [...] },
  //    *     { series: [...] },
  //    *   ]
  //    * }
  //    */
  //   response.results.forEach(parseResult);
  //
  //   function parseResult(s) {
  //     /*
  //      * s looks like:
  //      * {
  //      *   series: [
  //      *     {
  //      *       name: "<measurement>",
  //      *       columns: ["time", "<field name 1>", "<field name 2>", ...],
  //      *       values: [<time>, <value of field 1>, <value of field 2>, ...],
  //      *     },
  //      * }
  //      */
  //     s.series.forEach(parseSeries);
  //   }
  //
  //   function parseSeries(series) {
  //     /*
  //      * series looks like:
  //      * {
  //      *   name: "<measurement>",
  //      *   columns: ["time", "<field name 1>", "<field name 2>", ...],
  //      *   values: [
  //      *    [<time1>, <value of field 1 @ time1>, <value of field 2 @ time1>, ...],
  //      *    [<time2>, <value of field 1 @ time2>, <value of field 2 @ time2>, ...],
  //      *   ]
  //      * }
  //      */
  //     const measurementName = series.name;
  //     const columns = series.columns;
  //     // Tags are only included in an influxdb response under certain circumstances, e.g.
  //     // when a query is using GROUP BY (<tag key>).
  //     const tags = Object.keys(series.tags || {}).map((key) => {
  //       return `[${key}=${series.tags[key]}]`;
  //     }).sort().join('');
  //
  //     const c = columns.slice(1).sort();
  //     let previousColumnLength = 0;
  //
  //     if (c.length != previousColumnLength) {
  //       previousColumnLength = c.length;
  //     }
  //
  //     c.forEach((fieldName) => {
  //       let effectiveFieldName = `${measurementName}.${fieldName}${tags}`;
  //
  //       // If there are duplicate effectiveFieldNames identify them by their queryIndex
  //       if (effectiveFieldName in dygraphSeries) {
  //         effectiveFieldName = `${effectiveFieldName}-${queryIndex}`;
  //       }
  //
  //       // Given a field name, identify which column in the timeSeries result should hold the field's value
  //       // ex given this timeSeries [Date, 10, 20, 30] field index at 2 would correspond to value 20
  //       fieldToIndex[effectiveFieldName] = c.indexOf(fieldName);
  //       labels.push(effectiveFieldName);
  //
  //       const {light, heavy} = STROKE_WIDTH;
  //
  //       const dygraphSeriesStyles = {
  //         strokeWidth: queryIndex === activeQueryIndex ? heavy : light,
  //       };
  //
  //       if (!isInDataExplorer) {
  //         dygraphSeriesStyles.axis = queryIndex === 0 ? 'y' : 'y2';
  //       }
  //
  //       dygraphSeries[effectiveFieldName] = dygraphSeriesStyles;
  //     });
  //
  //     (series.values || []).forEach(parseRow);
  //
  //
  //
  //     function parseRow(row) {
  //       /**
  //        * row looks like:
  //        *   [<time1>, <value of field 1 @ time1>, <value of field 2 @ time1>, ...]
  //        */
  //       const date = row[0];
  //       const dateString = date.toString();
  //       row.forEach((value, index) => {
  //         if (index === 0) {
  //           // index 0 in a row is always the timestamp
  //           if (!dateToFieldValue[dateString]) {
  //             dateToFieldValue[dateString] = {};
  //             dates[dateString] = date;
  //           }
  //           return;
  //         }
  //
  //         const fieldName = columns[index];
  //         let effectiveFieldName = `${measurementName}.${fieldName}${tags}`;
  //
  //         // If there are duplicate effectiveFieldNames identify them by their queryIndex
  //         if (effectiveFieldName in dateToFieldValue[dateString]) {
  //           effectiveFieldName = `${effectiveFieldName}-${queryIndex}`;
  //         }
  //
  //         dateToFieldValue[dateString][effectiveFieldName] = value;
  //       });
  //     }
  //   }
  // });
  //
  // function buildTimeSeries() {
  //   const allDates = Object.keys(dateToFieldValue);
  //   allDates.sort((a, b) => a - b);
  //   const rowLength = labels.length + 1;
  //   return allDates.map((date) => {
  //     const row = new Array(rowLength);
  //
  //     row.fill(null);
  //     row[0] = new Date(dates[date]);
  //
  //     const fieldsForRow = dateToFieldValue[date];
  //
  //     Object.keys(fieldsForRow).forEach((effectiveFieldName) => {
  //       row[fieldToIndex[effectiveFieldName]] = fieldsForRow[effectiveFieldName];
  //     });
  //
  //     return row;
  //   });
  // }


}

function isEmpty(resp) {
  return !resp.results[0].series;
}

function hasError(resp) {
  return !!resp.results[0].error;
}
