import _ from 'lodash';
import {STROKE_WIDTH} from 'src/shared/constants';
/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

// activeQueryIndex is an optional argument that indicated which query's series we want highlighted.
export default function timeSeriesToDygraph(raw = [], activeQueryIndex, isInDataExplorer) {
  // collect results from each influx response
  const results = raw.reduce((acc, rawResponse, responseIndex) => {
    const responses = _.get(rawResponse, 'response.results', []);
    const indexedResponses = responses.map((response) => ({...response, responseIndex}));
    return [...acc, ...indexedResponses];
  }, []);

  // collect each series
  const serieses = results.reduce((acc, {series = [], responseIndex}, index) => {
    return [...acc, ...series.map((item) => ({...item, responseIndex, index}))];
  }, []);

  // convert series into cells with rows and columns
  const cells = serieses.reduce((acc, {name, columns, values, index, responseIndex}) => {
    const rows = values.map((vals) => ({
      name,
      columns,
      vals,
      index,
    }));

    rows.forEach(({vals, columns: cols, name: n, index: seriesIndex}) => {
      const [time, ...rowValues] = vals;
      rowValues.forEach((value, i) => {
        const column = cols[i + 1];
        acc.push({
          label: `${n}.${column}`,
          value,
          time,
          seriesIndex,
          responseIndex,
        });
      });
    });

    return acc;
  }, []);

  const labels = cells.reduce((acc, {label, seriesIndex, responseIndex}) => {
    const existingLabel = acc.find(({
      label: findLabel,
      seriesIndex: findSeriesIndex,
    }) => findLabel === label && findSeriesIndex === seriesIndex);

    if (!existingLabel) {
      acc.push({
        label,
        seriesIndex,
        responseIndex,
      });
    }

    return acc;
  }, []);

  const sortedLabels = _.sortBy(labels, 'label');

  const timeSeries = cells.reduce((acc, cell) => {
    let existingRowIndex = acc.findIndex(({time}) => cell.time === time);

    if (existingRowIndex === -1) {
      acc.push({
        time: cell.time,
        values: Array(sortedLabels.length).fill(null),
      });

      existingRowIndex = acc.length - 1;
    }

    const values = acc[existingRowIndex].values;
    const labelIndex = sortedLabels.findIndex(({label, seriesIndex}) => label === cell.label && cell.seriesIndex === seriesIndex);
    values[labelIndex] = cell.value;
    acc[existingRowIndex].values = values;

    return acc;
  }, []);

  const sortedTimeSeries = _.sortBy(timeSeries, 'time');

  const {light, heavy} = STROKE_WIDTH;

  const dygraphSeries = sortedLabels.reduce((acc, {label, responseIndex}) => {
    acc[label] = {
      strokeWidth: responseIndex === activeQueryIndex ? heavy : light,
    };

    if (!isInDataExplorer) {
      acc[label].axis = responseIndex === 0 ? 'y' : 'y2';
    }

    return acc;
  }, {});

  return {
    timeSeries: sortedTimeSeries.map(({time, values}) => ([new Date(time), ...values])),
    labels: ["time", ...sortedLabels.map(({label}) => label)],
    dygraphSeries,
  };
}
