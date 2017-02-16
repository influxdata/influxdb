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

  it('returns a padded range when an additional value is provided that is near or exceeds range of timeSeries data', () => {
    const value0 = -10;
    const value1 = 20;
    const timeSeries = [[new Date(1000), value0], [new Date(2000), 1], [new Date(3000), value1]];
    const unpadded = getRange(timeSeries);

    const actualOne = getRange(timeSeries, undefined, value0);
    const actualTwo = getRange(timeSeries, undefined, value1);

    expect(actualOne[0]).to.be.below(unpadded[0]);
    expect(actualOne[1]).to.equal(unpadded[1]);
    expect(actualTwo[1]).to.be.above(unpadded[1]);
    expect(actualTwo[0]).to.equal(unpadded[0]);
  });

  it('returns an expected range when an additional value is provided that is well within range of timeSeries data', () => {
    const value = 0;
    const timeSeries = [[new Date(1000), -10], [new Date(2000), 1], [new Date(3000), 20]];
    const unpadded = getRange(timeSeries);

    const actual = getRange(timeSeries, undefined, value);

    expect(actual[0]).to.equal(unpadded[0]);
    expect(actual[1]).to.equal(unpadded[1]);
  });
});
