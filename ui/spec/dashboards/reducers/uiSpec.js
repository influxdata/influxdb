import reducer from 'src/dashboards/reducers/ui';

import {
  loadDashboards,
} from 'src/dashboards/actions';

const noopAction = () => {
  return {type: 'NOOP'};
}

let state = undefined;

describe('DataExplorer.Reducers.UI', () => {
  it('it sets the default state for UI', () => {
    const actual = reducer(state, noopAction());
    const expected = {
      dashboards: [],
    };

    expect(actual).to.deep.equal(expected);
  });

  it('can load the dashboards', () => {
    const dashboards = [{cell: {}, name: "d1"}]
    const actual = reducer(state, loadDashboards(dashboards));
    const expected = {
      dashboards,
    }
    expect(actual).to.deep.equal(expected);
  });
});
