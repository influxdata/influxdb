// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button, EmptyState} from '@influxdata/clockface'

// Types
import {IconFont, ComponentSize, ComponentColor} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class DashboardEmpty extends Component<WithRouterProps> {
  public render() {
    return (
      <div className="dashboard-empty">
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            This Dashboard doesn't have any <b>Cells</b>, why not add one?
          </EmptyState.Text>
          <Button
            text="Add Cell"
            size={ComponentSize.Medium}
            icon={IconFont.AddCell}
            color={ComponentColor.Primary}
            onClick={this.handleAdd}
            testID="add-cell--button"
          />
        </EmptyState>
      </div>
    )
  }

  private handleAdd = () => {
    // TODO(alex): change this to using state from redux
    // and not the location.pathname to decouple routing
    // from presentation
    const {router, location} = this.props
    router.push(`${location.pathname}/cells/new`)
  }
}

export default withRouter<{}>(DashboardEmpty)
