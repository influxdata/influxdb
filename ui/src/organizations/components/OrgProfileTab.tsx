// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps, withRouter} from 'react-router'

import _ from 'lodash'

// Components
import {
  Form,
  Button,
  ComponentSize,
  Panel,
  IconFont,
  FlexBox,
  AlignItems,
  FlexDirection,
  Gradients,
  InfluxColors,
  JustifyContent,
} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ButtonType} from 'src/clockface'

type Props = WithRouterProps

@ErrorHandling
class OrgProfileTab extends PureComponent<Props> {
  public render() {
    return (
      <Panel size={ComponentSize.Small} backgroundColor={InfluxColors.Onyx}>
        <Panel.Header title="Organization Profile" />
        <Panel.Body>
          <Form onSubmit={this.handleShowEditOverlay}>
            <Panel
              gradient={Gradients.DocScott}
              size={ComponentSize.ExtraSmall}
            >
              <Panel.Header title="Danger Zone!" />
              <Panel.Body>
                <FlexBox
                  stretchToFitWidth={true}
                  alignItems={AlignItems.Center}
                  direction={FlexDirection.Row}
                  justifyContent={JustifyContent.SpaceBetween}
                >
                  <div>
                    <h5 style={{marginBottom: '0'}}>Rename Organization</h5>
                    <p style={{marginTop: '2px'}}>
                      This action can have wide-reaching unintended
                      consequences.
                    </p>
                  </div>
                  <Button
                    text="Rename"
                    icon={IconFont.Pencil}
                    type={ButtonType.Submit}
                  />
                </FlexBox>
              </Panel.Body>
            </Panel>
          </Form>
        </Panel.Body>
      </Panel>
    )
  }

  private handleShowEditOverlay = () => {
    const {
      params: {orgID},
      router,
    } = this.props

    router.push(`/orgs/${orgID}/profile/rename`)
  }
}

export default withRouter<{}>(OrgProfileTab)
