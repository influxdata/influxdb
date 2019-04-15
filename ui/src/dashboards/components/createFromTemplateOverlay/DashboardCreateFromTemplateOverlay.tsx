// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Button, ComponentColor, ComponentStatus} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import DashboardTemplateBrowser from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplateBrowser'
import DashboardTemplatesEmpty from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplatesEmpty'

// Actions
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions'
import {getTemplateByID} from 'src/templates/actions'

// Types
import {
  TemplateSummary,
  ITemplate,
  TemplateType,
  IDashboardTemplateIncluded,
} from '@influxdata/influx'
import {AppState, RemoteDataState, DashboardTemplate} from 'src/types'

interface StateProps {
  templates: TemplateSummary[]
  templateStatus: RemoteDataState
}

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
}

interface State {
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: ITemplate
  variables: string[]
  cells: string[]
}

type Props = DispatchProps & StateProps

class DashboardImportFromTemplateOverlay extends PureComponent<
  Props & WithRouterProps,
  State
> {
  constructor(props) {
    super(props)
    this.state = {
      selectedTemplateSummary: null,
      selectedTemplate: null,
      variables: [],
      cells: [],
    }
  }

  render() {
    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={900}>
          <Overlay.Heading
            title="Create Dashboard from a Template"
            onDismiss={this.onDismiss}
          />
          <Overlay.Body>{this.overlayBody}</Overlay.Body>
          <Overlay.Footer>
            <Button
              text="Cancel"
              onClick={this.onDismiss}
              key="cancel-button"
            />
            <Button
              text="Create Dashboard"
              onClick={this.onSubmit}
              key="submit-button"
              testID="create-dashboard-button"
              color={ComponentColor.Primary}
              status={this.submitStatus}
            />
          </Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get overlayBody(): JSX.Element {
    const {
      selectedTemplateSummary,
      cells,
      variables,
      selectedTemplate,
    } = this.state
    const {templates} = this.props

    if (!templates.length) {
      return <DashboardTemplatesEmpty />
    }

    return (
      <DashboardTemplateBrowser
        templates={templates}
        cells={cells}
        variables={variables}
        selectedTemplate={selectedTemplate}
        selectedTemplateSummary={selectedTemplateSummary}
        onSelectTemplate={this.handleSelectTemplate}
      />
    )
  }

  private get submitStatus(): ComponentStatus {
    const {selectedTemplate} = this.state

    return selectedTemplate ? ComponentStatus.Default : ComponentStatus.Disabled
  }

  private getVariablesForTemplate(template: ITemplate): string[] {
    const variables = []
    const included = template.content.included as IDashboardTemplateIncluded[]
    included.forEach(data => {
      if (data.type === TemplateType.Variable) {
        variables.push(data.attributes.name)
      }
    })

    return variables
  }

  private getCellsForTemplate(template: ITemplate): string[] {
    const cells = []
    const included = template.content.included as IDashboardTemplateIncluded[]
    included.forEach(data => {
      if (data.type === TemplateType.View) {
        cells.push(data.attributes.name)
      }
    })

    return cells
  }

  private handleSelectTemplate = (
    selectedTemplateSummary: TemplateSummary
  ) => async (): Promise<void> => {
    const selectedTemplate = await getTemplateByID(selectedTemplateSummary.id)
    this.setState({
      selectedTemplateSummary,
      selectedTemplate,
      variables: this.getVariablesForTemplate(selectedTemplate),
      cells: this.getCellsForTemplate(selectedTemplate),
    })
  }

  private onDismiss = () => {
    const {router} = this.props
    router.goBack()
  }

  private onSubmit = async (): Promise<void> => {
    const {createDashboardFromTemplate} = this.props

    await createDashboardFromTemplate(this.state
      .selectedTemplate as DashboardTemplate)
    this.onDismiss()
  }
}

const mstp = ({templates: {items, status}}: AppState): StateProps => ({
  templates: items,
  templateStatus: status,
})

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
}

export default connect<StateProps>(
  mstp,
  mdtp
)(withRouter(DashboardImportFromTemplateOverlay))
