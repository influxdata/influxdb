import _ from 'lodash';
import {STROKE_WIDTH} from 'src/shared/constants';
/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

// activeQueryIndex is an optional argument that indicated which query's series we want highlighted.
export default function timeSeriesToDygraph(raw = [], activeQueryIndex, isInDataExplorer) {
  const d0 = new Date();

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
  const cells = serieses.reduce((acc, {name, columns, values, index, responseIndex, tags = {}}) => {
    const rows = values.map((vals) => ({
      name,
      vals,
      index,
    }));

    columns.shift();

    // tagSet is each tag key and value for a series
    const tagSet = Object.keys(tags).map((tag) => `[${tag}=${tags[tag]}]`).sort().join('');

    rows.forEach(({vals, name: measurement, index: seriesIndex}) => {
      const [time, ...rowValues] = vals;

      rowValues.forEach((value, i) => {
        const field = columns[i];
        acc.push({
          label: `${measurement}.${field}${tagSet}`,
          value,
          time,
          seriesIndex,
          responseIndex,
        });
      });
    });

    return acc;
  }, []);

  // labels are a unique combination of measurement, fields, and tags that indicate a specific series on the graph legend
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
  const memo = {};

  const ts0 = new Date();

  const timeSeries = cells.reduce((acc, cell) => {
    let existingRowIndex = memo[cell.time];

    if (existingRowIndex === undefined) {
      acc.push({
        time: cell.time,
        values: Array(sortedLabels.length).fill(null),
      });

      existingRowIndex = acc.length - 1;
      memo[cell.time] = existingRowIndex;
    }

    const values = acc[existingRowIndex].values;
    const labelIndex = sortedLabels.findIndex(({label, seriesIndex}) => label === cell.label && cell.seriesIndex === seriesIndex);;
    values[labelIndex] = cell.value;
    acc[existingRowIndex].values = values;

    return acc;
  }, []);
  const ts1 = new Date();

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

  const d1 = new Date();

  console.log(`new function took ${d1 - d0}ms to run for ${cells.length} cells.`);
  console.log(`timeSeries took ${ts1 - ts0}ms to run for ${timeSeries.length} records.`);

  return {
    labels: ["time", ...sortedLabels.map(({label}) => label)],
    timeSeries: sortedTimeSeries.map(({time, values}) => ([new Date(time), ...values])),
    dygraphSeries,
  };
}
