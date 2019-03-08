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
  telegrafs: Telegraf[]
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
    const {collector, telegrafs} = this.props
    const config = telegrafs.find(t => get(collector, 'id', '') === t.id)

    if (!config) {
      return ''
    }

    const labels = get(config, 'labels', [])

    const token = labels.find(l => l.name === 'token')

    if (!token) {
      return ''
    }

    return token.description
  }
}

const mstp = ({me: {name}, telegrafs}: AppState): StateProps => ({
  username: name,
  telegrafs: telegrafs.list,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafInstructionsOverlay)
