// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TelegrafConfig from 'src/telegrafs/components/TelegrafConfig'
import {ComponentColor, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'

// Actions
import {downloadTelegrafConfig} from 'src/telegrafs/actions'

// Types
import {AppState} from 'src/types'

interface StateProps {
  telegrafConfigName: string
  telegrafConfigID: string
}

interface DispatchProps {
  onDownloadTelegrafConfig: typeof downloadTelegrafConfig
}

type Props = StateProps & DispatchProps

@ErrorHandling
class TelegrafConfigOverlay extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {telegrafConfigName} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={1200}>
          <Overlay.Heading
            title={`Telegraf Configuration - ${telegrafConfigName}`}
            onDismiss={this.handleDismiss}
          />

          <Overlay.Body>
            <div className="config-overlay">
              <TelegrafConfig />
            </div>
          </Overlay.Body>
          <Overlay.Footer>
            <Button
              color={ComponentColor.Secondary}
              text="Download Config"
              onClick={this.handleDownloadConfig}
            />
          </Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/telegrafs`)
  }

  private handleDownloadConfig = async () => {
    const {
      onDownloadTelegrafConfig,
      telegrafConfigName,
      telegrafConfigID,
    } = this.props
    onDownloadTelegrafConfig(telegrafConfigID, telegrafConfigName)
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
  onDownloadTelegrafConfig: downloadTelegrafConfig,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(TelegrafConfigOverlay))
