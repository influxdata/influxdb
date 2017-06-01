import appReducer from 'shared/reducers/app'
import {
  enablePresentationMode,
  disablePresentationMode,
  // delayEnablePresentationMode,
  setAutoRefresh,
  templateControlBarVisibilityToggled,
} from 'shared/actions/app'

describe('Shared.Reducers.appReducer', () => {
  const initialState = {
    ephemeral: {
      inPresentationMode: false,
    },
    persisted: {
      autoRefresh: 0,
      showTemplateControlBar: false,
    },
  }

  it('should handle ENABLE_PRESENTATION_MODE', () => {
    const reducedState = appReducer(initialState, enablePresentationMode())

    expect(reducedState.ephemeral.inPresentationMode).to.equal(true)
  })

  it('should handle DISABLE_PRESENTATION_MODE', () => {
    Object.assign(initialState, {ephemeral: {inPresentationMode: true}})

    const reducedState = appReducer(initialState, disablePresentationMode())

    expect(reducedState.ephemeral.inPresentationMode).to.equal(false)
  })

  it('should handle SET_AUTOREFRESH', () => {
    const expectedMs = 15000

    const reducedState = appReducer(initialState, setAutoRefresh(expectedMs))

    expect(reducedState.persisted.autoRefresh).to.equal(expectedMs)
  })

  it('should handle TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED', () => {
    const reducedState = appReducer(
      initialState,
      templateControlBarVisibilityToggled()
    )

    const expectedTestState = !reducedState.persisted.showTemplateControlBar

    expect(initialState.persisted.showTemplateControlBar).to.equal(
      expectedTestState
    )
  })
})
