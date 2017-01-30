import _ from 'lodash';
import {STROKE_WIDTH} from 'src/shared/constants';
import {map, reduce, forEach} from 'fast.js';

/**
 * Accepts an array of raw influxdb responses and returns a format
 * that Dygraph understands.
 */

const DEFAULT_SIZE = 0;
const cells = {
  label: new Array(DEFAULT_SIZE),
  value: new Array(DEFAULT_SIZE),
  time: new Array(DEFAULT_SIZE),
  seriesIndex: new Array(DEFAULT_SIZE),
  responseIndex: new Array(DEFAULT_SIZE),
};

// activeQueryIndex is an optional argument that indicated which query's series we want highlighted.
export default function timeSeriesToDygraph(raw = [], activeQueryIndex, isInDataExplorer) {
  // collect results from each influx response
  const results = reduce(raw, (acc, rawResponse, responseIndex) => {
    const responses = _.get(rawResponse, 'response.results', []);
    const indexedResponses = map(responses, (response) => ({...response, responseIndex}));
    return [...acc, ...indexedResponses];
  }, []);

  // collect each series
  const serieses = reduce(results, (acc, {series = [], responseIndex}, index) => {
    return [...acc, ...map(series, (item) => ({...item, responseIndex, index}))];
  }, []);

  const size = reduce(serieses, (acc, {columns, values}) => {
    if (columns.length && values.length) {
      return acc + (columns.length - 1) * values.length;
    }
    return acc;
  }, 0);

  // convert series into cells with rows and columns
  let cellIndex = 0;

  forEach(serieses, ({name, columns, values, index, responseIndex, tags = {}}) => {
    const rows = map(values, (vals) => ({
      name,
      vals,
      index,
    }));

    columns.shift();

    // tagSet is each tag key and value for a series
    const tagSet = map(Object.keys(tags), (tag) => `[${tag}=${tags[tag]}]`).sort().join('');

    forEach(rows, ({vals, name: measurement, index: seriesIndex}) => {
      const [time, ...rowValues] = vals;

      forEach(rowValues, (value, i) => {
        const field = columns[i];
        cells.label[cellIndex] = `${measurement}.${field}${tagSet}`;
        cells.value[cellIndex] = value;
        cells.time[cellIndex] = time;
        cells.seriesIndex[cellIndex] = seriesIndex;
        cells.responseIndex[cellIndex] = responseIndex;
        cellIndex++; // eslint-disable-line no-plusplus
      });
    });
  });

  // labels are a unique combination of measurement, fields, and tags that indicate a specific series on the graph legend
  const labelMemo = {};
  const labels = [];
  for (let i = 0; i < size; i++) {
    const label = cells.label[i];
    const seriesIndex = cells.seriesIndex[i];
    const responseIndex = cells.responseIndex[i];
    const memoKey = `${label}-${seriesIndex}`;
    const existingLabel = labelMemo[memoKey];

    if (!existingLabel) {
      labels.push({
        label,
        seriesIndex,
        responseIndex,
      });
      labelMemo[memoKey] = true;
    }
  }

  const sortedLabels = _.sortBy(labels, 'label');
  const tsMemo = {};

  const timeSeries = [];

  for (let i = 0; i < size; i++) {
    const time = cells.time[i];
    const value = cells.value[i];
    const label = cells.label[i];
    const seriesIndex = cells.seriesIndex[i];

    let existingRowIndex = tsMemo[time];

    if (existingRowIndex === undefined) {
      timeSeries.push({
        time,
        values: Array(sortedLabels.length).fill(null), // TODO clone
      });

      existingRowIndex = timeSeries.length - 1;
      tsMemo[time] = existingRowIndex;
    }

    const values = timeSeries[existingRowIndex].values;
    const labelIndex = sortedLabels.findIndex((find) => find.label === label && find.seriesIndex === seriesIndex);
    values[labelIndex] = value;
    timeSeries[existingRowIndex].values = values;
  }

  const sortedTimeSeries = _.sortBy(timeSeries, 'time');

  const {light, heavy} = STROKE_WIDTH;

  const dygraphSeries = reduce(sortedLabels, (acc, {label, responseIndex}) => {
    acc[label] = {
      strokeWidth: responseIndex === activeQueryIndex ? heavy : light,
    };

    if (!isInDataExplorer) {
      acc[label].axis = responseIndex === 0 ? 'y' : 'y2';
    }

    return acc;
  }, {});

  return {
    labels: ["time", ...map(sortedLabels, ({label}) => label)],
    timeSeries: map(sortedTimeSeries, ({time, values}) => ([new Date(time), ...values])),
    dygraphSeries,
  };
}
