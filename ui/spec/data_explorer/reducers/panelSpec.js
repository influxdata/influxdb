import reducer from 'src/data_explorer/reducers/panels';
import {deletePanel} from 'src/data_explorer/actions/view';

const fakeAddPanelAction = (panelID, queryID) => {
  return {
    type: 'CREATE_PANEL',
    payload: {panelID, queryID},
  };
};

describe('Chronograf.Reducers.Panel', () => {
  let state;
  const panelID = 123;
  const queryID = 456;

  beforeEach(() => {
    state = reducer({}, fakeAddPanelAction(panelID, queryID));
  });

  it('can add a panel', () => {
    const actual = state[panelID];
    expect(actual).to.deep.equal({
      id: panelID,
      queryIds: [queryID],
    });
  });

  it('can delete a panel', () => {
    const nextState = reducer(state, deletePanel(panelID));

    const actual = nextState[panelID];
    expect(actual).to.equal(undefined);
  });
});
