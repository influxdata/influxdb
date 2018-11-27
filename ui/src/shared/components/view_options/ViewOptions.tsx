// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Components
import ViewTypeSelector from 'src/shared/components/ViewTypeSelector'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import OptionsSwitcher from 'src/shared/components/view_options/OptionsSwitcher'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Constants
import {HANDLE_VERTICAL} from 'src/shared/constants'

// Types
import {View, NewView, AppState} from 'src/types/v2'

interface DispatchProps {
  onUpdateType: typeof setType
}

interface StateProps {
  view: View | NewView
}

type Props = DispatchProps & StateProps

class ViewOptions extends PureComponent<Props> {
  public render() {
    return (
      <Threesizer orientation={HANDLE_VERTICAL} divisions={this.divisions} />
    )
  }

  private get divisions() {
    const {view, onUpdateType} = this.props
    return [
      {
        name: 'Visualization Type',
        headerButtons: [],
        render: () => (
          <FancyScrollbar>
            <ViewTypeSelector
              type={view.properties.type}
              onUpdateType={onUpdateType}
            />
          </FancyScrollbar>
        ),
        headerOrientation: HANDLE_VERTICAL,
      },
      {
        name: 'Customize',
        headerButtons: [],
        render: () => (
          <FancyScrollbar>
            <OptionsSwitcher view={view} />
          </FancyScrollbar>
        ),
        headerOrientation: HANDLE_VERTICAL,
      },
    ]
  }
}

const mstp = (state: AppState): StateProps => {
  const {view} = getActiveTimeMachine(state)

  return {view}
}

const mdtp: DispatchProps = {
  onUpdateType: setType,
}
export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(ViewOptions)
