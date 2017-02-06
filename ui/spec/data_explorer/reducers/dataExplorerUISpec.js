import reducer from 'src/data_explorer/reducers/dataExplorerUI';
import {activatePanel} from 'src/data_explorer/actions/view';

describe('DataExploerer.Reducers.UI', () => {
  it('can set the active panel', () => {
    const activePanel = 123;
    const actual = reducer({}, activatePanel(activePanel));

    expect(actual).to.deep.equal({activePanel});
  });
});
