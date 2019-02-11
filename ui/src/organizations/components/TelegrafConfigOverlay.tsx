// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OverlayBody from 'src/clockface/components/overlays/OverlayBody'
import OverlayContainer from 'src/clockface/components/overlays/OverlayContainer'
import OverlayTechnology from 'src/clockface/components/overlays/OverlayTechnology'
import OverlayHeading from 'src/clockface/components/overlays/OverlayHeading'
import TelegrafConfig from 'src/organizations/components/TelegrafConfig'
import {ComponentSize, ComponentColor, Button} from '@influxdata/clockface'
import {OverlayFooter} from 'src/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// APIs
import {client} from 'src/utils/api'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Styles
import 'src/organizations/components/TelegrafConfigOverlay.scss'

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
      <OverlayTechnology visible={visible}>
        <OverlayContainer maxWidth={1200}>
          <OverlayHeading
            title={`Telegraf Configuration - ${telegrafConfigName}`}
            onDismiss={onDismiss}
          />

          <OverlayBody>
            <div className="config-overlay">
              <TelegrafConfig />
            </div>
          </OverlayBody>
          <OverlayFooter>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text={'Download Config'}
              onClick={this.handleDownloadConfig}
            />
          </OverlayFooter>
        </OverlayContainer>
      </OverlayTechnology>
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
