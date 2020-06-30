// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
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
import TemplateBrowserEmpty from 'src/tasks/components/TemplateBrowserEmpty'
import GetResources from 'src/resources/components/GetResources'

// Actions
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/tasks/actions/thunks'
import {getTemplateByID} from 'src/templates/actions/thunks'

// Types
import {
  TemplateSummary,
  Template,
  TemplateType,
  AppState,
  RemoteDataState,
  TaskTemplate,
  ResourceType,
} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors/getAll'

interface StateProps {
  templates: TemplateSummary[]
  templateStatus: RemoteDataState
}

interface DispatchProps {
  createTaskFromTemplate: typeof createTaskFromTemplateAction
}

interface State {
  selectedTemplateSummary: TemplateSummary
  selectedTemplate: Template
}

type Props = DispatchProps & StateProps

class TaskImportFromTemplateOverlay extends PureComponent<
  Props & WithRouterProps,
  State
> {
  constructor(props) {
    super(props)
    this.state = {
      selectedTemplateSummary: null,
      selectedTemplate: null,
    }
  }

  render() {
    return (
      <Overlay visible={true}>
        <GetResources resources={[ResourceType.Templates]}>
          <Overlay.Container maxWidth={900}>
            <Overlay.Header
              title="Create Task from a Template"
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
                text="Create Task"
                onClick={this.onSubmit}
                key="submit-button"
                testID="create-task-button"
                color={ComponentColor.Success}
                status={this.submitStatus}
              />
            </Overlay.Footer>
          </Overlay.Container>
        </GetResources>
      </Overlay>
    )
  }

  private get overlayBody(): JSX.Element {
    const {selectedTemplateSummary, selectedTemplate} = this.state
    const {templates} = this.props

    if (!templates.length) {
      return <TemplateBrowserEmpty />
    }

    return (
      <TemplateBrowser
        templates={templates}
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

  private handleSelectTemplate = async (
    selectedTemplateSummary: TemplateSummary
  ): Promise<void> => {
    const selectedTemplate = await getTemplateByID(selectedTemplateSummary.id)

    this.setState({
      selectedTemplateSummary,
      selectedTemplate,
    })
  }

  private onDismiss = () => {
    const {router} = this.props
    router.goBack()
  }

  private onSubmit = () => {
    const {createTaskFromTemplate} = this.props
    const taskTemplate = this.state.selectedTemplate as TaskTemplate

    createTaskFromTemplate(taskTemplate)
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
    t => !t.meta.type || t.meta.type === TemplateType.Task
  )

  const templates = sortBy(filteredTemplates, item =>
    item.meta.name.toLocaleLowerCase()
  )

  return {
    templates,
    templateStatus: status,
  }
}

const mdtp: DispatchProps = {
  createTaskFromTemplate: createTaskFromTemplateAction,
}

export default connect<StateProps>(
  mstp,
  mdtp
)(withRouter(TaskImportFromTemplateOverlay))
