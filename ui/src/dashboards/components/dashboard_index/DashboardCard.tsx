// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {IconFont, ComponentColor} from '@influxdata/clockface'
import {ResourceList, Context} from 'src/clockface'
import FeatureFlag from 'src/shared/components/FeatureFlag'
import FetchLabels from 'src/shared/components/FetchLabels'
import ResourceLabels from 'src/shared/components/inline_label_editor/ResourceLabels'

// Actions
import {
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
  createDashboardLabelAsync,
} from 'src/dashboards/actions/v2'

// Types
import {Dashboard, Organization} from 'src/types/v2'
import {Label} from 'src/types/v2/labels'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants'

interface DispatchProps {
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
  onCreateDashboardLabel: typeof createDashboardLabelAsync
}

interface PassedProps {
  dashboard: Dashboard
  orgs: Organization[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  showOwnerColumn: boolean
  onFilterChange: (searchTerm: string) => void
}

type Props = DispatchProps & PassedProps

class DashboardCard extends PureComponent<Props> {
  public render() {
    const {dashboard} = this.props
    const {id} = dashboard

    return (
      <ResourceList.Card
        key={`dashboard-id--${id}`}
        testID="resource-card"
        name={() => (
          <ResourceList.Name
            onUpdate={this.handleUpdateDashboard}
            name={dashboard.name}
            hrefValue={`/dashboards/${dashboard.id}`}
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
        labels={() => this.labels}
        updatedAt={dashboard.meta.updatedAt}
        owner={this.ownerOrg}
        contextMenu={() => this.contextMenu}
      />
    )
  }

  private handleUpdateDashboard = (name: string) => {
    this.props.onUpdateDashboard({...this.props.dashboard, name})
  }

  private get contextMenu(): JSX.Element {
    const {
      dashboard,
      onDeleteDashboard,
      onExportDashboard,
      onCloneDashboard,
    } = this.props

    return (
      <Context>
        <FeatureFlag>
          <Context.Menu icon={IconFont.CogThick}>
            <Context.Item
              label="Export"
              action={onExportDashboard}
              value={dashboard}
            />
          </Context.Menu>
        </FeatureFlag>
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

  private handleRemoveLabel = (label: Label): void => {
    const {onRemoveDashboardLabels, dashboard} = this.props

    onRemoveDashboardLabels(dashboard.id, [label])
  }

  private handleAddLabel = (label: Label): void => {
    const {onAddDashboardLabels, dashboard} = this.props

    onAddDashboardLabels(dashboard.id, [label])
  }

  private get labels(): JSX.Element {
    const {dashboard, onFilterChange} = this.props

    return (
      <FetchLabels>
        {labels => (
          <ResourceLabels
            labels={labels}
            selectedLabels={dashboard.labels}
            onRemoveLabel={this.handleRemoveLabel}
            onAddLabel={this.handleAddLabel}
            onFilterChange={onFilterChange}
          />
        )}
      </FetchLabels>
    )
  }

  private get ownerOrg(): Organization {
    const {dashboard, orgs} = this.props
    return orgs.find(o => o.id === dashboard.orgID)
  }

  private handleUpdateDescription = (description: string): void => {
    const {onUpdateDashboard} = this.props
    const dashboard = {...this.props.dashboard, description}

    onUpdateDashboard(dashboard)
  }
}

const mdtp: DispatchProps = {
  onAddDashboardLabels: addDashboardLabelsAsync,
  onRemoveDashboardLabels: removeDashboardLabelsAsync,
  onCreateDashboardLabel: createDashboardLabelAsync,
}

export default connect<{}, DispatchProps, PassedProps>(
  null,
  mdtp
)(DashboardCard)
