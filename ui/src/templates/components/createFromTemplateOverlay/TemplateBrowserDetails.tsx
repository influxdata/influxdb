// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  Panel,
  EmptyState,
  ComponentSize,
  Grid,
  Columns,
  DapperScrollbars,
} from '@influxdata/clockface'
import {TemplateSummary, Template} from 'src/types'

interface Props {
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: Template
  variables: string[]
  cells: string[]
}

class TemplateBrowserDetails extends PureComponent<Props> {
  public render() {
    return (
      <DapperScrollbars
        className="import-template-overlay--details"
        autoSize={false}
      >
        <Panel
          testID="template-panel"
          className="import-template-overlay--panel"
        >
          <Panel.Body size={ComponentSize.Medium}>
            {this.panelContents}
          </Panel.Body>
        </Panel>
      </DapperScrollbars>
    )
  }

  private get panelContents(): JSX.Element {
    const {selectedTemplateSummary} = this.props

    if (!selectedTemplateSummary) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text>Select a Template from the left</EmptyState.Text>
        </EmptyState>
      )
    }

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthSM={Columns.Twelve}>
            {this.templateName}
            {this.templateDescription}
          </Grid.Column>
          {this.props.variables && (
            <Grid.Column widthSM={Columns.Six}>
              <h5 className="import-template-overlay--heading">Variables</h5>
              {this.variablesList}
            </Grid.Column>
          )}
          {this.props.cells && (
            <Grid.Column widthSM={Columns.Six}>
              <h5 className="import-template-overlay--heading">Cells</h5>
              {this.cellsList}
            </Grid.Column>
          )}
        </Grid.Row>
      </Grid>
    )
  }

  private get variablesList(): JSX.Element | JSX.Element[] {
    const {variables} = this.props

    if (!variables.length) {
      return (
        <p className="import-template-overlay--included missing">
          No included variables
        </p>
      )
    }

    return variables.map((variable, i) => (
      <p
        className="import-templates-overlay--included"
        key={`${i} ${variable}`}
      >
        {variable}
      </p>
    ))
  }

  private get cellsList(): JSX.Element | JSX.Element[] {
    const {cells} = this.props

    if (!cells.length) {
      return (
        <p className="import-template-overlay--included missing">
          No included cells
        </p>
      )
    }

    return cells.map((cell, i) => (
      <p className="import-templates-overlay--included" key={`${i} ${cell}`}>
        {cell}
      </p>
    ))
  }

  private get templateDescription(): JSX.Element {
    const {selectedTemplateSummary} = this.props
    const description = _.get(selectedTemplateSummary, 'meta.description')

    if (description) {
      return (
        <p className="import-template-overlay--description">{description}</p>
      )
    }

    return (
      <p className="import-template-overlay--description missing">
        No description
      </p>
    )
  }

  private get templateName(): JSX.Element {
    const {selectedTemplateSummary} = this.props
    const name = _.get(selectedTemplateSummary, 'meta.name')

    const templateName = name || 'Untitled'
    const className = name
      ? 'import-template-overlay--name'
      : 'import-template-overlay--name missing'

    return <h3 className={className}>{templateName}</h3>
  }
}

export default TemplateBrowserDetails
