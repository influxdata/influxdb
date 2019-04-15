// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Panel, EmptyState, ComponentSize} from '@influxdata/clockface'
import {TemplateSummary, ITemplate} from '@influxdata/influx'

interface Props {
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: ITemplate
  variables: string[]
  cells: string[]
}

class DashboardTemplateDetails extends PureComponent<Props> {
  public render() {
    const {selectedTemplateSummary, variables, cells} = this.props

    if (!selectedTemplateSummary) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text text="Select a Template from the left" />
        </EmptyState>
      )
    }

    return (
      <Panel
        className="import-template-overlay--details"
        testID="template-panel"
      >
        <Panel.Header title={_.get(selectedTemplateSummary, 'meta.name')} />
        <Panel.Body>
          <div className="import-template-overlay--columns">
            <div className="import-template-overlay--variables-column">
              <h5>Variables:</h5>
              {variables.map(variable => (
                <p key={variable}>{variable}</p>
              ))}
            </div>
            <div className="import-template-overlay--cells-column">
              <h5>Cells:</h5>
              {cells.map(cell => (
                <p key={cell}>{cell}</p>
              ))}
            </div>
          </div>
        </Panel.Body>
      </Panel>
    )
  }
}

export default DashboardTemplateDetails
