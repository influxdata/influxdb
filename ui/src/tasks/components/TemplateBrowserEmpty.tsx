// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {
  EmptyState,
  ComponentSize,
  Button,
  IconFont,
} from '@influxdata/clockface'

// Types
import {AppState, Organization} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: Organization
}

type Props = StateProps & WithRouterProps

class TemplateBrowserEmpty extends PureComponent<Props> {
  public render() {
    return (
      <div className="import-template-overlay--empty">
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            Looks like you don't have any <b>Templates</b> yet, why not import
            one?
          </EmptyState.Text>
          <Button
            size={ComponentSize.Medium}
            text="Import One Here"
            icon={IconFont.CogThick}
            onClick={this.handleButtonClick}
          />
        </EmptyState>
      </div>
    )
  }

  private handleButtonClick = (): void => {
    const {router, org} = this.props

    router.push(`/orgs/${org.id}/tasks/import`)
  }
}

const mstp = (state: AppState): StateProps => ({
  org: getOrg(state),
})

export default connect<StateProps, {}>(
  mstp,
  null
)(withRouter(TemplateBrowserEmpty))
