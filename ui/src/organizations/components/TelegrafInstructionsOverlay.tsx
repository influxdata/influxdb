// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {TOKEN_LABEL} from 'src/labels/constants'

// Types
import {AppState} from 'src/types/v2'
import {Telegraf} from '@influxdata/influx'

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
  telegrafs: AppState['telegrafs']['list']
  tokens: AppState['tokens']['list']
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
export class TelegrafInstructionsOverlay extends PureComponent<Props> {
  public render() {
    const {notify, collector, visible, onDismiss} = this.props

    return (
      <WizardOverlay
        visible={visible}
        title="Telegraf Setup Instructions"
        onDismiss={onDismiss}
      >
        <TelegrafInstructions
          notify={notify}
          token={this.token}
          configID={get(collector, 'id', '')}
        />
      </WizardOverlay>
    )
  }

  private get token(): string {
    const {collector, telegrafs, tokens} = this.props
    const config =
      telegrafs.find(t => get(collector, 'id', '') === t.id) || collector

    if (!config) {
      return 'no config found'
    }

    const labels = get(config, 'labels', [])

    const label = labels.find(l => l.name === TOKEN_LABEL)
    const auth = tokens.find(t => t.id === get(label, 'properties.tokenID'))

    if (!label || !auth) {
      return 'unknown token'
    }

    return auth.token
  }
}

const mstp = ({me: {name}, telegrafs, tokens}: AppState): StateProps => ({
  username: name,
  telegrafs: telegrafs.list,
  tokens: tokens.list,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafInstructionsOverlay)
