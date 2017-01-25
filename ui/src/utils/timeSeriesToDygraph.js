import _ from 'lodash';
import {STROKE_WIDTH} from 'src/shared/constants';
/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

// activeQueryIndex is an optional argument that indicated which query's series we want highlighted.
export default function timeSeriesToDygraph(raw = [], activeQueryIndex, isInDataExplorer) {
  // collect results from each influx response
  const results = raw.reduce((acc, response, responseIndex) => {
    const responses = _.get(response, 'response.results', [])
    const indexedResponses = responses.map((response) => ({...response, responseIndex}))
    return [...acc, ...indexedResponses]
  }, [])

  // collect each series
  const serieses = results.reduce((acc, {series, responseIndex}, index) => {
    return [...acc, ...series.map((item) => ({...item, responseIndex, index}))];
  }, [])

  // convert series into cells with rows and columns
  const cells = serieses.reduce((acc, {name, columns, values, index, responseIndex}) => {
    const rows = values.map((values) => ({
      name,
      columns,
      values,
      index,
    }))

    rows.forEach(({values: vals, columns: cols, name: n, index: seriesIndex}) => {
      const [time, ...rowValues] = vals
      rowValues.forEach((value, i) => {
        const column = cols[i + 1]
        acc.push({
          label: `${n}.${column}`,
          value,
          time,
          seriesIndex,
          responseIndex,
        })
      })
    })

    return acc
  }, [])

  const labels = cells.reduce((acc, {label, seriesIndex, responseIndex}) => {
    const existingLabel = acc.find(({
      label: findLabel,
      seriesIndex: findSeriesIndex,
    }) => findLabel === label && findSeriesIndex === seriesIndex)

    if (!existingLabel) {
      acc.push({
        label,
        seriesIndex,
        responseIndex,
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
    const labelIndex = sortedLabels.findIndex(({label, seriesIndex}) => label === cell.label && cell.seriesIndex === seriesIndex);
    values[labelIndex] = cell.value
    acc[existingRowIndex].values = values

    return acc
  }, [])

  const sortedTimeSeries = _.sortBy(timeSeries, 'time')

  const {light, heavy} = STROKE_WIDTH;

  const dygraphSeries = sortedLabels.reduce((acc, {label, responseIndex}) => {
    acc[label] = {
      strokeWidth: responseIndex === activeQueryIndex ? heavy : light,
    }

    if (!isInDataExplorer) {
      acc[label].axis = responseIndex === 0 ? 'y' : 'y2'
    }

    return acc
  }, {})

  const timeSeriesToDygraph = {
    timeSeries: sortedTimeSeries.map(({time, values}) => ([new Date(time), ...values])),
    labels: ["time", ...sortedLabels.map(({label}) => label)],
    dygraphSeries,
  }

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
