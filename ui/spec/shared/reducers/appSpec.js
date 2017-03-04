import {ephemeralReducer, persistedReducer} from 'src/shared/reducers/app'
import {setAutoRefresh} from 'src/shared/actions/app'

describe('Shared.Reducers.app.persisted', () => {
  it('should handle SET_AUTOREFRESH', () => {
    const initialState = {autoRefresh: 15000}
    const milliseconds = 0

    const reducedState = persistedReducer(initialState, setAutoRefresh(milliseconds));
    expect(reducedState.autoRefresh).to.equal(0);
  })
})
