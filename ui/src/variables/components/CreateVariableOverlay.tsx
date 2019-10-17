// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Overlay} from '@influxdata/clockface'
import VariableFormContext from 'src/variables/components/VariableFormContext'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

type Props = WithRouterProps

class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={1000}>
          <Overlay.Header
            title="Create Variable"
            onDismiss={this.handleHideOverlay}
          />
          <Overlay.Body>
            <GetResources resource={ResourceType.Variables}>
              <VariableFormContext onHideOverlay={this.handleHideOverlay} />
            </GetResources>
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleHideOverlay = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/settings/variables`)
  }
}

export {CreateVariableOverlay}
export default withRouter<{}>(CreateVariableOverlay)
