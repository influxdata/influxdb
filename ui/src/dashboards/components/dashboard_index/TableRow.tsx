// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  ComponentSpacer,
  IndexList,
} from 'src/clockface'
import DefaultToggle from 'src/dashboards/components/DashboardDefaultToggle'

// Types
import {Dashboard} from 'src/types/v2'
import {Alignment} from 'src/clockface'

interface Props {
  dashboard: Dashboard
  defaultDashboardLink: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onSetDefaultDashboard: (dashboardLink: string) => void
}

export default class DashboardsIndexTableRow extends PureComponent<Props> {
  public render() {
    const {dashboard, onSetDefaultDashboard, defaultDashboardLink} = this.props
    const {id, name, links} = dashboard

    return (
      <IndexList.Row key={`dashboard-id--${id}`} disabled={false}>
        <IndexList.Cell>
          <Link to={`/dashboards/${id}`}>{name}</Link>
        </IndexList.Cell>
        <IndexList.Cell>You</IndexList.Cell>
        <IndexList.Cell>12h Ago</IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Center}>
          <DefaultToggle
            dashboardLink={links.self}
            defaultDashboardLink={defaultDashboardLink}
            onChangeDefault={onSetDefaultDashboard}
          />
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              text="Export"
              icon={IconFont.Export}
              onClick={this.handleExport}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              text="Clone"
              icon={IconFont.Duplicate}
              onClick={this.handleClone}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Danger}
              text="Delete"
              onClick={this.handleDelete}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleExport = () => {
    const {onExportDashboard, dashboard} = this.props
    onExportDashboard(dashboard)
  }

  private handleClone = () => {
    const {onCloneDashboard, dashboard} = this.props
    onCloneDashboard(dashboard)
  }

  private handleDelete = () => {
    const {onDeleteDashboard, dashboard} = this.props
    onDeleteDashboard(dashboard)
  }
}
