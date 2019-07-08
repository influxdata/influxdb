// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {IconFont, ComponentColor} from '@influxdata/clockface'
import {ResourceList, Context} from 'src/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
} from 'src/dashboards/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Types
import {ILabel} from '@influxdata/influx'
import {AppState, Dashboard} from 'src/types'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants'
import {resetViews} from 'src/dashboards/actions/views'

interface PassedProps {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
}

interface DispatchProps {
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
  onCreateLabel: typeof createLabelAsync
  onResetViews: typeof resetViews
}

type Props = PassedProps & DispatchProps & StateProps & WithRouterProps

class DashboardCard extends PureComponent<Props> {
  public render() {
    const {dashboard, onFilterChange, labels} = this.props
    const {id} = dashboard

    return (
      <ResourceList.Card
        key={`dashboard-id--${id}`}
        testID="dashboard-card"
        name={() => (
          <ResourceList.EditableName
            onUpdate={this.handleUpdateDashboard}
            onClick={this.handleClickDashboard}
            name={dashboard.name}
            noNameString={DEFAULT_DASHBOARD_NAME}
            parentTestID="dashboard-card--name"
            buttonTestID="dashboard-card--name-button"
            inputTestID="dashboard-card--input"
          />
        )}
        description={() => (
          <ResourceList.Description
            onUpdate={this.handleUpdateDescription}
            description={dashboard.description}
            placeholder={`Describe ${dashboard.name}`}
          />
        )}
        labels={() => (
          <InlineLabels
            selectedLabels={dashboard.labels}
            labels={labels}
            onFilterChange={onFilterChange}
            onAddLabel={this.handleAddLabel}
            onRemoveLabel={this.handleRemoveLabel}
            onCreateLabel={this.handleCreateLabel}
          />
        )}
        updatedAt={dashboard.meta.updatedAt}
        contextMenu={() => this.contextMenu}
      />
    )
  }

  private handleUpdateDashboard = async (name: string) => {
    await this.props.onUpdateDashboard({...this.props.dashboard, name})
  }

  private get contextMenu(): JSX.Element {
    const {dashboard, onDeleteDashboard, onCloneDashboard} = this.props

    return (
      <Context>
        <Context.Menu icon={IconFont.CogThick}>
          <Context.Item
            label="Export"
            action={this.handleExport}
            value={dashboard}
          />
        </Context.Menu>
        <Context.Menu
          icon={IconFont.Duplicate}
          color={ComponentColor.Secondary}
        >
          <Context.Item
            label="Clone"
            action={onCloneDashboard}
            value={dashboard}
          />
        </Context.Menu>
        <Context.Menu
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
          testID="context-delete-menu"
        >
          <Context.Item
            label="Delete"
            action={onDeleteDashboard}
            value={dashboard}
            testID="context-delete-dashboard"
          />
        </Context.Menu>
      </Context>
    )
  }

  private handleClickDashboard = () => {
    const {
      router,
      dashboard,
      onResetViews,
      params: {orgID},
    } = this.props

    onResetViews()

    router.push(`/orgs/${orgID}/dashboards/${dashboard.id}`)
  }

  private handleUpdateDescription = (description: string): void => {
    const {onUpdateDashboard} = this.props
    const dashboard = {...this.props.dashboard, description}

    onUpdateDashboard(dashboard)
  }

  private handleAddLabel = (label: ILabel): void => {
    const {dashboard, onAddDashboardLabels} = this.props

    onAddDashboardLabels(dashboard.id, [label])
  }

  private handleRemoveLabel = (label: ILabel): void => {
    const {dashboard, onRemoveDashboardLabels} = this.props

    onRemoveDashboardLabels(dashboard.id, [label])
  }

  private handleCreateLabel = async (label: ILabel): Promise<void> => {
    try {
      await this.props.onCreateLabel(label.name, label.properties)

      // notify success
    } catch (err) {
      console.error(err)
      // notify of fail
      throw err
    }
  }

  private handleExport = () => {
    const {
      router,
      dashboard,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/dashboards/${dashboard.id}/export`)
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list),
  }
}

const mdtp: DispatchProps = {
  onCreateLabel: createLabelAsync,
  onAddDashboardLabels: addDashboardLabelsAsync,
  onRemoveDashboardLabels: removeDashboardLabelsAsync,
  onResetViews: resetViews,
}

export default connect<StateProps, DispatchProps, PassedProps>(
  mstp,
  mdtp
)(withRouter(DashboardCard))
