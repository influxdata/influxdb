const PADDING_FACTOR = 0.1;

export default function getRange(timeSeries, override, value = null, rangeValue = null) {
  if (override) {
    return override;
  }

  const subtractPadding = (val) => +val - val * PADDING_FACTOR;
  const addPadding = (val) => +val + val * PADDING_FACTOR;

  const pad = (val, side) => {
    if (val === null || val === '') {
      return null;
    }

    if (val < 0) {
      return side === "top" ? subtractPadding(val) : addPadding(val);
    }

    return side === "top" ? addPadding(val) : subtractPadding(val);
  };

  const points = [
    ...timeSeries,
    [null, pad(value)],
    [null, pad(rangeValue, "top")],
  ];

  const range = points.reduce(([min, max], series) => {
    for (let i = 1; i < series.length; i++) {
      const val = series[i];

      if (max === null) {
        max = val;
      }

      if (min === null) {
        min = val;
      }

      if (typeof val === "number") {
        min = Math.min(min, val);
        max = Math.max(max, val);
      }
    }

    return [min, max];
  }, [null, null]);

  // If time series is such that min and max are equal use Dygraph defaults
  if (range[0] === range[1]) {
    return [null, null];
  }

  return range;
}
