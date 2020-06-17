// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafConfig from 'src/telegrafs/components/TelegrafConfig'
import {
  ComponentColor,
  Button,
  RemoteDataState,
  SpinnerContainer,
  TechnoSpinner,
  Overlay,
  ComponentStatus,
} from '@influxdata/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {getByID} from 'src/resources/selectors'

// Types
import {AppState, Telegraf, ResourceType} from 'src/types'

interface OwnProps {
  onClose: () => void
}

interface StateProps {
  telegraf: Telegraf
  status: RemoteDataState
  telegrafConfig: string
  configStatus: RemoteDataState
}

type Props = OwnProps & StateProps

@ErrorHandling
class TelegrafConfigOverlay extends PureComponent<Props> {
  public render() {
    return <>{this.overlay}</>
  }

  private get overlay(): JSX.Element {
    const {telegraf, status} = this.props

    return (
      <Overlay.Container maxWidth={1200}>
        <Overlay.Header
          title={`Telegraf Configuration - ${_.get(telegraf, 'name', '')}`}
          onDismiss={this.handleDismiss}
        />
        <Overlay.Body>
          <SpinnerContainer
            loading={status}
            spinnerComponent={<TechnoSpinner />}
          >
            <div className="config-overlay">
              <TelegrafConfig />
            </div>
          </SpinnerContainer>
        </Overlay.Body>
        <Overlay.Footer>
          <Button
            color={ComponentColor.Secondary}
            text="Download Config"
            onClick={this.handleDownloadConfig}
            status={this.buttonStatus}
          />
        </Overlay.Footer>
      </Overlay.Container>
    )
  }
  private get buttonStatus(): ComponentStatus {
    const {configStatus} = this.props
    if (configStatus === RemoteDataState.Done) {
      return ComponentStatus.Default
    }
    return ComponentStatus.Disabled
  }

  private handleDismiss = () => {
    this.props.onClose()
  }

  private handleDownloadConfig = () => {
    const {
      telegrafConfig,
      telegraf: {name},
    } = this.props
    downloadTextFile(telegrafConfig, name || 'telegraf', '.conf')
  }
}

const mstp = (state: AppState): StateProps => {
  const {overlays, resources} = state
  const {status, currentConfig} = resources.telegrafs
  const {id} = overlays.params

  return {
    telegraf: getByID<Telegraf>(state, ResourceType.Telegrafs, id),
    status,
    telegrafConfig: currentConfig.item,
    configStatus: currentConfig.status,
  }
}

export default connect<StateProps, {}, {}>(mstp, null)(TelegrafConfigOverlay)
