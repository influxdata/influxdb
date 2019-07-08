// Libraries
import React, {Component} from 'react'

// Components
import {Button, EmptyState} from '@influxdata/clockface'

// Types
import {IconFont, ComponentSize, ComponentColor} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onAddCell: () => void
}

@ErrorHandling
class DashboardEmpty extends Component<Props> {
  public render() {
    const {onAddCell} = this.props

    return (
      <div className="dashboard-empty">
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text="This Dashboard doesn't have any Cells , why not add
          one?"
            highlightWords={['Cells']}
          />
          <Button
            text="Add Cell"
            size={ComponentSize.Medium}
            icon={IconFont.AddCell}
            color={ComponentColor.Primary}
            onClick={onAddCell}
            testID="add-cell--button"
          />
        </EmptyState>
      </div>
    )
  }
}

export default DashboardEmpty
