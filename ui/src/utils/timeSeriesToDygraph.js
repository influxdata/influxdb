/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

export default function timeSeriesToDygraph(raw = []) {
  const labels = ['time']; // all of the effective field names (i.e. <measurement>.<field>)
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

  raw.forEach(({response}, queryIndex) => {
    // If a response is an empty result set or a query returned an error
    // from InfluxDB, don't try and parse.
    if (response.results.length) {
      if (isEmpty(response) || hasError(response)) {
        return;
      }
    }

    /**
     * response looks like:
     * {
     *   results: [
     *     { series: [...] },
     *     { series: [...] },
     *   ]
     * }
     */
    response.results.forEach(parseResult);

    function parseResult(s) {
      /*
       * s looks like:
       * {
       *   series: [
       *     {
       *       name: "<measurement>",
       *       columns: ["time", "<field name 1>", "<field name 2>", ...],
       *       values: [<time>, <value of field 1>, <value of field 2>, ...],
       *     },
       * }
       */
      s.series.forEach(parseSeries);
    }

    function parseSeries(series) {
      /*
       * series looks like:
       * {
       *   name: "<measurement>",
       *   columns: ["time", "<field name 1>", "<field name 2>", ...],
       *   values: [
       *    [<time1>, <value of field 1 @ time1>, <value of field 2 @ time1>, ...],
       *    [<time2>, <value of field 1 @ time2>, <value of field 2 @ time2>, ...],
       *   ]
       * }
       */
      const measurementName = series.name;
      const columns = series.columns;

      // Tags are only included in an influxdb response under certain circumstances, e.g.
      // when a query is using GROUP BY (<tag key>).
      const tags = Object.keys(series.tags || {}).map((key) => {
        return `[${key}=${series.tags[key]}]`;
      }).sort().join('');

      columns.slice(1).forEach((fieldName) => {
        const effectiveFieldName = `${measurementName}.${fieldName}${tags}`;

        // Given a field name, identify which column in the timeSeries result should hold the field's value
        // ex given this timeSeries [Date, 10, 20, 30] field index at 2 would correspond to value 20
        fieldToIndex[effectiveFieldName] = labels.length;
        labels.push(effectiveFieldName);
        dygraphSeries[effectiveFieldName] = {axis: queryIndex === 0 ? 'y' : 'y2'};
      });

      (series.values || []).forEach(parseRow);

      function parseRow(row) {
        /**
         * row looks like:
         *   [<time1>, <value of field 1 @ time1>, <value of field 2 @ time1>, ...]
         */
        const date = row[0];
        const dateString = date.toString();
        row.forEach((value, index) => {
          if (index === 0) {
            // index 0 in a row is always the timestamp
            if (!dateToFieldValue[dateString]) {
              dateToFieldValue[dateString] = {};
              dates[dateString] = date;
            }
            return;
          }

          const fieldName = columns[index];
          const effectiveFieldName = `${measurementName}.${fieldName}${tags}`;
          dateToFieldValue[dateString][effectiveFieldName] = value;
        });
      }
    }
  });

  function buildTimeSeries() {
    const allDates = Object.keys(dateToFieldValue);
    allDates.sort((a, b) => a - b);
    const rowLength = labels.length;
    return allDates.map((date) => {
      const row = new Array(rowLength);

      row.fill(null);
      row[0] = new Date(dates[date]);

      const fieldsForRow = dateToFieldValue[date];
      Object.keys(fieldsForRow).forEach((effectiveFieldName) => {
        row[fieldToIndex[effectiveFieldName]] = fieldsForRow[effectiveFieldName];
      });

      return row;
    });
  }

  return {
    labels,
    timeSeries: buildTimeSeries(),
    dygraphSeries,
  };
}

function isEmpty(resp) {
  return !resp.results[0].series;
}

function hasError(resp) {
  return !!resp.results[0].error;
}

