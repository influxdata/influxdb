// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafConfig from 'src/organizations/components/TelegrafConfig'
import {ComponentColor, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// APIs
import {client} from 'src/utils/api'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState} from 'src/types/v2'

interface OwnProps {
  visible: boolean
  onDismiss: () => void
}

interface StateProps {
  telegrafConfigName: string
  telegrafConfigID
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
export class TelegrafConfigOverlay extends PureComponent<Props> {
  public render() {
    const {visible, onDismiss, telegrafConfigName} = this.props

    return (
      <Overlay visible={visible}>
        <Overlay.Container maxWidth={1200}>
          <Overlay.Heading
            title={`Telegraf Configuration - ${telegrafConfigName}`}
            onDismiss={onDismiss}
          />

          <Overlay.Body>
            <div className="config-overlay">
              <TelegrafConfig />
            </div>
          </Overlay.Body>
          <Overlay.Footer>
            <Button
              color={ComponentColor.Secondary}
              text={'Download Config'}
              onClick={this.handleDownloadConfig}
            />
          </Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleDownloadConfig = async () => {
    try {
      const config = await client.telegrafConfigs.getTOML(
        this.props.telegrafConfigID
      )
      downloadTextFile(
        config,
        `${this.props.telegrafConfigName || 'config'}.toml`
      )
    } catch (error) {
      this.props.notify(getTelegrafConfigFailed())
    }
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigName, telegrafConfigID},
  },
}: AppState): StateProps => {
  return {telegrafConfigName, telegrafConfigID}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafConfigOverlay)
