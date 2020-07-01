// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {RouteComponentProps, withRouter} from 'react-router-dom'

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
  Grid,
  Columns,
} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import CodeSnippet from 'src/shared/components/CodeSnippet'

import {getOrg} from 'src/organizations/selectors'
import {
  copyToClipboardSuccess,
  copyToClipboardFailed,
} from 'src/shared/copy/notifications'

// Types
import {ButtonType} from 'src/clockface'
import {AppState, Organization} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

interface StateProps {
  me: MeState
  org: Organization
}

type Props = StateProps & RouteComponentProps<{orgID: string}>

@ErrorHandling
class OrgProfileTab extends PureComponent<Props> {
  public render() {
    return (
      <>
        <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Six}>
          <Panel backgroundColor={InfluxColors.Onyx}>
            <Panel.Header size={ComponentSize.Small}>
              <h4>Organization Profile</h4>
            </Panel.Header>
            <Panel.Body size={ComponentSize.Small}>
              <Form onSubmit={this.handleShowEditOverlay}>
                <Panel gradient={Gradients.DocScott}>
                  <Panel.Header size={ComponentSize.ExtraSmall}>
                    <h5>Danger Zone!</h5>
                  </Panel.Header>
                  <Panel.Body size={ComponentSize.ExtraSmall}>
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
        </Grid.Column>
        <Grid.Column widthXS={Columns.Twelve} widthSM={Columns.Six}>
          <Panel>
            <Panel.Header size={ComponentSize.ExtraSmall}>
              <h4>Common Ids</h4>
            </Panel.Header>
            <Panel.Body>
              <CodeSnippet
                copyText={this.props.me.id}
                label="My User Id"
                onCopyText={this.generateCopyText('User Id')}
              />
              <CodeSnippet
                copyText={this.props.org.id}
                label="Organization Id"
                onCopyText={this.generateCopyText('Organization Id')}
              />
            </Panel.Body>
          </Panel>
        </Grid.Column>
      </>
    )
  }

  private handleShowEditOverlay = () => {
    const {
      match: {
        params: {orgID},
      },
      history,
    } = this.props

    history.push(`/orgs/${orgID}/settings/about/rename`)
  }

  private generateCopyText = title => (text, copySucceeded) => {
    if (copySucceeded) {
      return copyToClipboardSuccess(text, title)
    } else {
      return copyToClipboardFailed(text, title)
    }
  }
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state),
    me: state.me,
  }
}

export default connect<StateProps>(mstp, null)(withRouter(OrgProfileTab))
