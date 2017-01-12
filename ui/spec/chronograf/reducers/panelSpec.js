import reducer from 'src/data_explorer/reducers/panels';
import {deletePanel} from 'src/data_explorer/actions/view';

const fakeAddPanelAction = (panelId, queryId) => {
  return {
    type: 'CREATE_PANEL',
    payload: {panelId, queryId},
  };
};

describe('Chronograf.Reducers.Panel', () => {
  let state;
  const panelId = 123;
  const queryId = 456;

  beforeEach(() => {
    state = reducer({}, fakeAddPanelAction(panelId, queryId));
  });

  it('can add a panel', () => {
    const actual = state[panelId];
    expect(actual).to.deep.equal({
      id: panelId,
      queryIds: [queryId],
    });
  });

  it('can delete a panel', () => {
    const nextState = reducer(state, deletePanel(panelId));

    const actual = nextState[panelId];
    expect(actual).to.equal(undefined);
  });
});
