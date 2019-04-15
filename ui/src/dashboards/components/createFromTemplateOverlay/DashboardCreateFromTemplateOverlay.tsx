// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Button, ComponentColor, ComponentStatus} from '@influxdata/clockface'
import {Overlay, ResponsiveGridSizer} from 'src/clockface'
import {
  TemplateSummary,
  ITemplate,
  Organization,
  TemplateType,
  IDashboardTemplateIncluded,
} from '@influxdata/influx'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import DashboardTemplateDetails from 'src/dashboards/components/createFromTemplateOverlay/DashboardTemplateDetails'

// Actions
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions'
import {getTemplateByID} from 'src/templates/actions'

// Types
import {AppState, RemoteDataState, DashboardTemplate} from 'src/types'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

interface StateProps {
  templates: TemplateSummary[]
  templateStatus: RemoteDataState
  orgs: Organization[]
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
    const {
      selectedTemplateSummary,
      cells,
      variables,
      selectedTemplate,
    } = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={900}>
          <Overlay.Heading
            title="Create Dashboard from a Template"
            onDismiss={this.onDismiss}
          />
          <Overlay.Body>
            <div className="import-template-overlay">
              <GetResources resource={ResourceTypes.Templates}>
                <ResponsiveGridSizer columns={2}>
                  {this.templates}
                </ResponsiveGridSizer>
              </GetResources>
              <DashboardTemplateDetails
                cells={cells}
                variables={variables}
                selectedTemplateSummary={selectedTemplateSummary}
                selectedTemplate={selectedTemplate}
              />
            </div>
          </Overlay.Body>
          <Overlay.Footer>{this.buttons}</Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get templates(): JSX.Element[] {
    const {templates} = this.props
    const {selectedTemplateSummary} = this.state

    return templates.map(t => {
      return (
        <CardSelectCard
          key={t.id}
          id={t.id}
          onClick={this.selectTemplate(t)}
          checked={_.get(selectedTemplateSummary, 'id', '') === t.id}
          label={t.meta.name}
          hideImage={true}
          testID={`card-select-${t.meta.name}`}
        />
      )
    })
  }

  private get buttons(): JSX.Element[] {
    const {selectedTemplate} = this.state

    const submitStatus = selectedTemplate
      ? ComponentStatus.Default
      : ComponentStatus.Disabled

    return [
      <Button text="Cancel" onClick={this.onDismiss} key="cancel-button" />,
      <Button
        text="Create Dashboard"
        onClick={this.onSubmit}
        key="submit-button"
        testID="create-dashboard-button"
        color={ComponentColor.Primary}
        status={submitStatus}
      />,
    ]
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

  private selectTemplate = (
    selectedTemplateSummary: TemplateSummary
  ) => async (): Promise<void> => {
    this.setState({selectedTemplateSummary})
    const selectedTemplate = await getTemplateByID(selectedTemplateSummary.id)
    this.setState({
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
    const {
      createDashboardFromTemplate,
      params: {orgID},
    } = this.props

    await createDashboardFromTemplate(
      this.state.selectedTemplate as DashboardTemplate,
      orgID
    )
    this.onDismiss()
  }
}

const mstp = ({templates: {items, status}, orgs}: AppState): StateProps => ({
  templates: items,
  templateStatus: status,
  orgs: orgs.items,
})

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
}

export default connect<StateProps>(
  mstp,
  mdtp
)(withRouter(DashboardImportFromTemplateOverlay))
