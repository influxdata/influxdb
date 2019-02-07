// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import TelegrafConfig from 'src/organizations/components/TelegrafConfig'

// Types
import {AppState} from 'src/types/v2'

interface OwnProps {
  visible: boolean
  onDismiss: () => void
}

interface StateProps {
  telegrafConfigName: string
}

type Props = OwnProps & StateProps

@ErrorHandling
export class TelegrafConfigOverlay extends PureComponent<Props> {
  public render() {
    const {visible, onDismiss, telegrafConfigName} = this.props

    return (
      <WizardOverlay
        visible={visible}
        title={`Telegraf Configuration - ${telegrafConfigName}`}
        onDismiss={onDismiss}
      >
        <TelegrafConfig />
      </WizardOverlay>
    )
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigName},
  },
}: AppState): StateProps => {
  return {telegrafConfigName}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TelegrafConfigOverlay)
