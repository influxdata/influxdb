// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Overlay} from '@influxdata/clockface'
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Constants
import {TOKEN_LABEL} from 'src/labels/constants'

// Types
import {AppState} from 'src/types'
import {Telegraf} from '@influxdata/influx'

interface StateProps {
  username: string
  telegrafs: AppState['telegrafs']['list']
  tokens: AppState['tokens']['list']
  collectors: Telegraf[]
}

@ErrorHandling
export class TelegrafInstructionsOverlay extends PureComponent<
  StateProps & WithRouterProps
> {
  public render() {
    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={700}>
          <Overlay.Header
            title="Telegraf Setup Instructions"
            onDismiss={this.handleDismiss}
          />
          <Overlay.Body>
            <GetResources resources={[ResourceType.Authorizations]}>
              <TelegrafInstructions
                token={this.token}
                configID={get(this.collector, 'id', '')}
              />
            </GetResources>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get token(): string {
    const {telegrafs, tokens} = this.props
    const config =
      telegrafs.find(t => get(this.collector, 'id', '') === t.id) ||
      this.collector

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

  private get collector() {
    const {
      params: {id},
      collectors,
    } = this.props
    return collectors.find(c => c.id === id)
  }

  private handleDismiss = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    this.setState({
      collectorID: null,
    })

    router.push(`/orgs/${orgID}/load-data/telegrafs/`)
  }
}

const mstp = ({me: {name}, telegrafs, tokens}: AppState): StateProps => ({
  username: name,
  telegrafs: telegrafs.list,
  tokens: tokens.list,
  collectors: telegrafs.list,
})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter<StateProps>(TelegrafInstructionsOverlay))
