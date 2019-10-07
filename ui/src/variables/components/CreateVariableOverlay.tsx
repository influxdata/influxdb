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
      <GetResources resource={ResourceType.Variables}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={1000}>
            <Overlay.Header
              title="Create Variable"
              onDismiss={this.handleHideOverlay}
            />
            <Overlay.Body>
              <VariableFormContext onHideOverlay={this.handleHideOverlay} />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </GetResources>
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
