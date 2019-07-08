// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {
  Button,
  ComponentColor,
  ComponentStatus,
  Overlay,
} from '@influxdata/clockface'
import TemplateBrowser from 'src/templates/components/createFromTemplateOverlay/TemplateBrowser'
import TemplateBrowserEmpty from 'src/templates/components/createFromTemplateOverlay/TemplateBrowserEmpty'

// Actions
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions'
import {getTemplateByID} from 'src/templates/actions'

// Types
import {
  TemplateSummary,
  Template,
  TemplateType,
  DashboardTemplateIncluded,
  AppState,
  RemoteDataState,
  DashboardTemplate,
} from 'src/types'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

interface StateProps {
  templates: TemplateSummary[]
  templateStatus: RemoteDataState
}

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
}

interface State {
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: Template
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
      <GetResources resource={ResourceTypes.Templates}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={900}>
            <Overlay.Header
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
                color={ComponentColor.Success}
                status={this.submitStatus}
              />
            </Overlay.Footer>
          </Overlay.Container>
        </Overlay>
      </GetResources>
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
      return <TemplateBrowserEmpty />
    }

    return (
      <TemplateBrowser
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

  private getVariablesForTemplate(template: Template): string[] {
    const variables = []
    const included = template.content.included as DashboardTemplateIncluded[]
    included.forEach(data => {
      if (data.type === TemplateType.Variable) {
        variables.push(data.attributes.name)
      }
    })

    return variables
  }

  private getCellsForTemplate(template: Template): string[] {
    const cells = []
    const included = template.content.included as DashboardTemplateIncluded[]
    included.forEach(data => {
      if (data.type === TemplateType.View) {
        cells.push(data.attributes.name)
      }
    })

    return cells
  }

  private handleSelectTemplate = async (
    selectedTemplateSummary: TemplateSummary
  ): Promise<void> => {
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
    const dashboardTemplate = this.state.selectedTemplate as DashboardTemplate

    await createDashboardFromTemplate(dashboardTemplate)
    this.onDismiss()
  }
}

const mstp = ({templates: {items, status}}: AppState): StateProps => {
  const filteredTemplates = items.filter(
    t => !t.meta.type || t.meta.type === TemplateType.Dashboard
  )

  const templates = _.sortBy(filteredTemplates, item =>
    item.meta.name.toLocaleLowerCase()
  )

  return {
    templates,
    templateStatus: status,
  }
}

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
}

export default connect<StateProps>(
  mstp,
  mdtp
)(withRouter(DashboardImportFromTemplateOverlay))
