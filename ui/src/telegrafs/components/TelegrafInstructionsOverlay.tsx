// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Overlay} from '@influxdata/clockface'
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
import GetResources from 'src/resources/components/GetResources'

// Types
import {Telegraf, AppState, ResourceType} from 'src/types'

// Selectors
import {getAll, getToken} from 'src/resources/selectors'
import {clearDataLoaders} from 'src/dataLoaders/actions/dataLoaders'

interface StateProps {
  username: string
  token: string
  collectors: Telegraf[]
}

interface DispatchProps {
  onClearDataLoaders: typeof clearDataLoaders
}

type Props = StateProps &
  DispatchProps &
  RouteComponentProps<{orgID: string; id: string}>

@ErrorHandling
export class TelegrafInstructionsOverlay extends PureComponent<Props> {
  public render() {
    const {token} = this.props
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
                token={token}
                configID={get(this.collector, 'id', '')}
              />
            </GetResources>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get collector() {
    const {
      match: {
        params: {id},
      },
      collectors,
    } = this.props
    return collectors.find(c => c.id === id)
  }

  private handleDismiss = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
      onClearDataLoaders,
    } = this.props
    this.setState({
      collectorID: null,
    })
    onClearDataLoaders()
    history.push(`/orgs/${orgID}/load-data/telegrafs/`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    me: {name},
  } = state

  const token = getToken(state)
  const telegrafs = getAll<Telegraf>(state, ResourceType.Telegrafs)

  return {
    username: name,
    token,
    collectors: telegrafs,
  }
}

const mdtp: DispatchProps = {
  onClearDataLoaders: clearDataLoaders,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(TelegrafInstructionsOverlay))
