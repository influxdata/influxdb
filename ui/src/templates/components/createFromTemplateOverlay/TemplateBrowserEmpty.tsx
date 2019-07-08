// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
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

interface StateProps {
  org: Organization
}

type Props = StateProps & WithRouterProps

class TemplateBrowserEmpty extends PureComponent<Props> {
  public render() {
    return (
      <div className="import-template-overlay--empty">
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text="Looks like you don't have any Templates  yet, why not import one?"
            highlightWords={['Templates']}
          />
          <Button
            size={ComponentSize.Medium}
            text="Go to Templates Settings"
            icon={IconFont.CogThick}
            onClick={this.handleButtonClick}
          />
        </EmptyState>
      </div>
    )
  }

  private handleButtonClick = (): void => {
    const {router, org} = this.props

    router.push(`/orgs/${org.id}/templates`)
  }
}

const mstp = ({orgs: {org}}: AppState): StateProps => ({
  org,
})

export default connect<StateProps, {}>(
  mstp,
  null
)(withRouter(TemplateBrowserEmpty))
