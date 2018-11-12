// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Components
import ViewTypeSelector from 'src/shared/components/ViewTypeSelector'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import OptionsSwitcher from 'src/shared/components/view_options/OptionsSwitcher'

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
        menuOptions: [],
        render: () => (
          <ViewTypeSelector
            type={view.properties.type}
            onUpdateType={onUpdateType}
          />
        ),
        headerOrientation: HANDLE_VERTICAL,
      },
      {
        name: 'Customize',
        headerButtons: [],
        menuOptions: [],
        render: () => <OptionsSwitcher view={view} />,
        headerOrientation: HANDLE_VERTICAL,
      },
    ]
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    timeMachines: {activeTimeMachineID, timeMachines},
  } = state
  const timeMachine = timeMachines[activeTimeMachineID]

  return {
    view: timeMachine.view,
  }
}

const mdtp: DispatchProps = {
  onUpdateType: setType,
}
export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(ViewOptions)
