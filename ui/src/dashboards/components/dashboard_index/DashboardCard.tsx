// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {IconFont, ComponentColor} from '@influxdata/clockface'
import {ResourceList, Context} from 'src/clockface'
import FeatureFlag from 'src/shared/components/FeatureFlag'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// Actions
import {createLabelAJAX} from 'src/labels/api'
import {
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
} from 'src/dashboards/actions/v2'

// Types
import {Organization} from 'src/types/v2'
import {IDashboard, ILabel} from '@influxdata/influx'
import {AppState} from 'src/types/v2'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants'

interface PassedProps {
  dashboard: IDashboard
  orgs: Organization[]
  onDeleteDashboard: (dashboard: IDashboard) => void
  onCloneDashboard: (dashboard: IDashboard) => void
  onUpdateDashboard: (dashboard: IDashboard) => void
  showOwnerColumn: boolean
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
}

interface DispatchProps {
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
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
        owner={this.ownerOrg}
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
        <FeatureFlag>
          <Context.Menu icon={IconFont.CogThick}>
            <Context.Item
              label="Export"
              action={this.handleExport}
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

  private get ownerOrg(): Organization {
    const {dashboard, orgs, showOwnerColumn} = this.props

    if (showOwnerColumn) {
      return orgs.find(o => o.id === dashboard.orgID)
    }
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

  private handleCreateLabel = async (label: ILabel): Promise<ILabel> => {
    try {
      const newLabel = await createLabelAJAX(label)
      // notify success
      return newLabel
    } catch (err) {
      console.log(err)
      // notify of fail
      throw err
    }
  }

  private handleExport = () => {
    const {
      router,
      dashboard,
      location: {pathname},
    } = this.props

    router.push(`${pathname}/${dashboard.id}/export`)
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: labels.list,
  }
}

const mdtp: DispatchProps = {
  onAddDashboardLabels: addDashboardLabelsAsync,
  onRemoveDashboardLabels: removeDashboardLabelsAsync,
}

export default connect<StateProps, DispatchProps, PassedProps>(
  mstp,
  mdtp
)(withRouter(DashboardCard))
