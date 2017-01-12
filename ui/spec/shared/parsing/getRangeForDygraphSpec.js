import getRange from 'shared/parsing/getRangeForDygraph';

describe('getRangeForDygraphSpec', () => {
  it('gets the range for one timeSeries', () => {
    const timeSeries = [[new Date(1000), 1], [new Date(2000), 2], [new Date(3000), 3]];

    const actual = getRange(timeSeries);
    const expected = [1, 3];

    expect(actual).to.deep.equal(expected);
  });

  it('does not get range when a range is provided', () => {
    const timeSeries = [[new Date(1000), 1], [new Date(2000), 2], [new Date(3000), 3]];

    const providedRange = [0, 4];
    const actual = getRange(timeSeries, providedRange);

    expect(actual).to.deep.equal(providedRange);
  });

  it('gets the range for multiple timeSeries', () => {
    const timeSeries = [
      [new Date(1000), null, 1],
      [new Date(1000), 100, 1],
      [new Date(2000), null, 2],
      [new Date(3000), 200, 3],
    ];

    const actual = getRange(timeSeries);
    const expected = [1, 200];

    expect(actual).to.deep.equal(expected);
  });

  it('returns a null array of two elements when min and max are equal', () => {
    const timeSeries = [[new Date(1000), 1], [new Date(2000), 1], [new Date(3000), 1]];
    const actual = getRange(timeSeries);
    const expected = [null, null];

    expect(actual).to.deep.equal(expected);
  });
});
