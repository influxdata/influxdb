// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import {sortBy} from 'lodash'

// Components
import {
  Button,
  ComponentColor,
  ComponentStatus,
  Overlay,
} from '@influxdata/clockface'
import TemplateBrowser from 'src/templates/components/createFromTemplateOverlay/TemplateBrowser'
import TemplateBrowserEmpty from 'src/templates/components/createFromTemplateOverlay/TemplateBrowserEmpty'
import GetResources from 'src/resources/components/GetResources'

// Actions
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions/thunks'
import {getTemplateByID} from 'src/templates/actions/thunks'

// Constants
import {influxdbTemplateList} from 'src/templates/constants/defaultTemplates'

// Types
import {
  TemplateSummary,
  Template,
  TemplateType,
  DashboardTemplateIncluded,
  AppState,
  RemoteDataState,
  DashboardTemplate,
  ResourceType,
} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors/getAll'

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
  Props & RouteComponentProps<{orgID: string}>,
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
      <GetResources resources={[ResourceType.Templates]}>
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
    const {id} = selectedTemplateSummary
    let selectedTemplate

    if (!id.includes('influxdb-template')) {
      selectedTemplate = await getTemplateByID(id)
    } else {
      selectedTemplate = selectedTemplateSummary
    }

    this.setState({
      selectedTemplateSummary,
      selectedTemplate,
      variables: this.getVariablesForTemplate(selectedTemplate),
      cells: this.getCellsForTemplate(selectedTemplate),
    })
  }

  private onDismiss = () => {
    const {history} = this.props
    history.goBack()
  }

  private onSubmit = () => {
    const {createDashboardFromTemplate} = this.props
    const dashboardTemplate = this.state.selectedTemplate as DashboardTemplate

    createDashboardFromTemplate(dashboardTemplate)
    this.onDismiss()
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    resources: {
      templates: {status},
    },
  } = state
  const items = getAll(state, ResourceType.Templates)
  const filteredTemplates = items.filter(
    t => !t.meta.type || t.meta.type === TemplateType.Dashboard
  )

  const templates = sortBy(filteredTemplates, item =>
    item.meta.name.toLocaleLowerCase()
  )

  return {
    templates: [...templates, ...(influxdbTemplateList as any)],
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
