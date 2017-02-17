import reducer from 'src/dashboards/reducers/ui'

import {
  loadDashboards,
  setDashboard,
} from 'src/dashboards/actions';

const noopAction = () => {
  return {type: 'NOOP'};
}

let state = undefined;

describe('DataExplorer.Reducers.UI', () => {
  it('can load the dashboards', () => {
    const dashboard = {id: 2, cells: [], name: "d2"}
    const dashboards = [
      {id: 1, cells: [], name: "d1"},
      dashboard,
    ]

    const actual = reducer(state, loadDashboards(dashboards, dashboard.id))
    const expected = {
      dashboards,
      dashboard,
    }

    expect(actual).to.deep.equal(expected)
  });

  it('can set a dashboard', () => {
    const d1 = {id: 2, cells: [], name: "d2"}
    const d2 = {id: 2, cells: [], name: "d2"}
    const dashboards = [
      d1,
      d2,
    ]

    const loadedState = reducer(state, loadDashboards(dashboards, d1.id))
    const actual = reducer(loadedState, setDashboard(d2.id))
    const expected = {
      dashboards,
      dashboard: d2,
    }

    expect(actual).to.deep.equal(expected)
  });
});
