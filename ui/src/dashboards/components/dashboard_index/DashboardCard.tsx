// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {IconFont, ComponentColor, ResourceCard} from '@influxdata/clockface'
import {Context} from 'src/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {
  addDashboardLabelAsync,
  removeDashboardLabelAsync,
} from 'src/dashboards/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Types
import {AppState, Dashboard, Label} from 'src/types'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants'
import {resetViews} from 'src/dashboards/actions/views'

// Utilities
import {relativeTimestampFormatter} from 'src/shared/utils/relativeTimestampFormatter'

interface PassedProps {
  dashboard: Dashboard
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: Label[]
}

interface DispatchProps {
  onAddDashboardLabel: typeof addDashboardLabelAsync
  onRemoveDashboardLabel: typeof removeDashboardLabelAsync
  onCreateLabel: typeof createLabelAsync
  onResetViews: typeof resetViews
}

type Props = PassedProps & DispatchProps & StateProps & WithRouterProps

class DashboardCard extends PureComponent<Props> {
  public render() {
    const {dashboard, onFilterChange, labels} = this.props
    const {id} = dashboard

    return (
      <ResourceCard
        key={`dashboard-id--${id}`}
        testID="dashboard-card"
        name={
          <ResourceCard.EditableName
            onUpdate={this.handleUpdateDashboard}
            onClick={this.handleClickDashboard}
            name={dashboard.name}
            noNameString={DEFAULT_DASHBOARD_NAME}
            testID="dashboard-card--name"
            buttonTestID="dashboard-card--name-button"
            inputTestID="dashboard-card--input"
          />
        }
        description={
          <ResourceCard.EditableDescription
            onUpdate={this.handleUpdateDescription}
            description={dashboard.description}
            placeholder={`Describe ${dashboard.name}`}
          />
        }
        labels={
          <InlineLabels
            selectedLabels={dashboard.labels}
            labels={labels}
            onFilterChange={onFilterChange}
            onAddLabel={this.handleAddLabel}
            onRemoveLabel={this.handleRemoveLabel}
            onCreateLabel={this.handleCreateLabel}
          />
        }
        metaData={[
          <>
            {relativeTimestampFormatter(
              dashboard.meta.updatedAt,
              'Last modified '
            )}
          </>,
        ]}
        contextMenu={this.contextMenu}
      />
    )
  }

  private handleUpdateDashboard = (name: string) => {
    this.props.onUpdateDashboard({...this.props.dashboard, name})
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
      onResetViews,
      router,
      dashboard,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/dashboards/${dashboard.id}`)

    onResetViews()
  }

  private handleUpdateDescription = (description: string) => {
    const {onUpdateDashboard} = this.props
    const dashboard = {...this.props.dashboard, description}

    onUpdateDashboard(dashboard)
  }

  private handleAddLabel = (label: Label) => {
    const {dashboard, onAddDashboardLabel} = this.props

    onAddDashboardLabel(dashboard.id, label)
  }

  private handleRemoveLabel = (label: Label) => {
    const {dashboard, onRemoveDashboardLabel} = this.props

    onRemoveDashboardLabel(dashboard.id, label)
  }

  private handleCreateLabel = async (label: Label) => {
    await this.props.onCreateLabel(label.name, label.properties) // eslint-disable-line
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
  onAddDashboardLabel: addDashboardLabelAsync,
  onRemoveDashboardLabel: removeDashboardLabelAsync,
  onResetViews: resetViews,
}

export default connect<StateProps, DispatchProps, PassedProps>(
  mstp,
  mdtp
)(withRouter(DashboardCard))
