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
import {Telegraf, TelegrafPluginOutputInfluxDBV2} from 'src/api'

interface OwnProps {
  visible: boolean
  onDismiss: () => void
  collector?: Telegraf
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = DispatchProps & OwnProps

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
          authToken={this.authToken}
          configID={get(collector, 'id', '')}
        />
      </WizardOverlay>
    )
  }

  private get authToken(): string {
    const {collector} = this.props

    if (!collector) {
      return ''
    }

    const plugin = collector.plugins.find(
      p => p.name === TelegrafPluginOutputInfluxDBV2.NameEnum.InfluxdbV2
    )

    return get(plugin, 'config.token', '')
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(TelegrafInstructionsOverlay)
