// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafConfig from 'src/telegrafs/components/TelegrafConfig'
import TelegrafEditorFooter from 'src/dataLoaders/components/TelegrafEditorFooter'
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
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Actions
import {reset} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {AppState, Telegraf} from 'src/types'

interface OwnProps {
  onClose: () => void
}

interface StateProps {
  telegraf: Telegraf
  status: RemoteDataState
  telegrafConfig: string
  configStatus: RemoteDataState
}

interface DispatchProps {
  resetEditor: typeof reset
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class TelegrafConfigOverlay extends PureComponent<Props> {
  public render() {
    return <>{this.overlay}</>
  }

  private get overlay(): JSX.Element {
    const {telegraf, status} = this.props

    let title = 'Telegraf Configuration'
    let footer = (
      <Overlay.Footer>
        <Button
          color={ComponentColor.Secondary}
          text="Download Config"
          onClick={this.handleDownloadConfig}
          status={this.buttonStatus}
        />
      </Overlay.Footer>
    )

    if (!isFlagEnabled('telegrafEditor')) {
      title += ' - ' + _.get(telegraf, 'name', '')
    } else {
      footer = (
        <Overlay.Footer>
          <TelegrafEditorFooter onDismiss={this.handleDismiss} />
        </Overlay.Footer>
      )
    }

    return (
      <Overlay.Container maxWidth={1200}>
        <Overlay.Header title={title} onDismiss={this.handleDismiss} />
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
        {footer}
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
    if (isFlagEnabled('telegrafEditor')) {
      this.props.resetEditor()
    }
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

const mstp = ({telegrafs, overlays}: AppState): StateProps => {
  const id = overlays.params.id

  return {
    telegraf: telegrafs.list.find(t => {
      return t.id === id
    }),
    status: telegrafs.status,
    telegrafConfig: telegrafs.currentConfig.item,
    configStatus: telegrafs.currentConfig.status,
  }
}

const mdtp: DispatchProps = {
  resetEditor: reset,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafConfigOverlay)
