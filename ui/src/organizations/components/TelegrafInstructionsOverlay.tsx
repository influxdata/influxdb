// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
import FetchAuthToken from 'src/dataLoaders/components/verifyStep/FetchAuthToken'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState} from 'src/types/v2'
import {Telegraf} from 'src/api'

interface OwnProps {
  visible: boolean
  onDismiss: () => void
  collector?: Telegraf
}

interface DispatchProps {
  notify: typeof notifyAction
}

interface StateProps {
  username: string
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
export class TelegrafInstructionsOverlay extends PureComponent<Props> {
  public render() {
    const {notify, collector, visible, onDismiss, username} = this.props

    return (
      <WizardOverlay
        visible={visible}
        title="Telegraf Setup Instructions"
        onDismiss={onDismiss}
      >
        <FetchAuthToken username={username}>
          {authToken => (
            <TelegrafInstructions
              notify={notify}
              authToken={authToken}
              configID={get(collector, 'id', '')}
            />
          )}
        </FetchAuthToken>
      </WizardOverlay>
    )
  }
}

const mstp = ({me: {name}}: AppState): StateProps => ({
  username: name,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafInstructionsOverlay)
