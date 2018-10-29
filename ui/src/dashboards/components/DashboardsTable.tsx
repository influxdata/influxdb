// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {Link, withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Types
import {Alignment} from 'src/clockface'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  ComponentSpacer,
  EmptyState,
} from 'src/clockface'
import DefaultToggle from 'src/dashboards/components/DashboardDefaultToggle'

import {Dashboard} from 'src/types/v2'

interface Props {
  searchTerm: string
  dashboards: Dashboard[]
  defaultDashboardLink: string
  onDeleteDashboard: (dashboard: Dashboard) => () => void
  onCreateDashboard: () => void
  onCloneDashboard: (
    dashboard: Dashboard
  ) => (event: MouseEvent<HTMLButtonElement>) => void
  onExportDashboard: (dashboard: Dashboard) => () => void
  onSetDefaultDashboard: (dashboardLink: string) => void
}

class DashboardsTable extends PureComponent<Props & WithRouterProps> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="50%" />
          <IndexList.HeaderCell columnName="Owner" width="10%" />
          <IndexList.HeaderCell columnName="Modified" width="10%" />
          <IndexList.HeaderCell
            columnName="Default"
            width="10%"
            alignment={Alignment.Center}
          />
          <IndexList.HeaderCell
            columnName=""
            width="20%"
            alignment={Alignment.Right}
          />
        </IndexList.Header>
        <IndexList.Body emptyState={this.emptyState} columnCount={5}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {
      onSetDefaultDashboard,
      defaultDashboardLink,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
    } = this.props

    return this.props.dashboards.map(d => (
      <IndexList.Row key={`dashboard-id--${d.id}`} disabled={false}>
        <IndexList.Cell>
          <Link to={`/dashboards/${d.id}?${this.sourceParam}`}>{d.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell>You</IndexList.Cell>
        <IndexList.Cell>12h Ago</IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Center}>
          <DefaultToggle
            dashboardLink={d.links.self}
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
              onClick={onExportDashboard(d)}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              text="Clone"
              icon={IconFont.Duplicate}
              onClick={onCloneDashboard(d)}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Danger}
              text="Delete"
              onClick={onDeleteDashboard(d)}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    ))
  }

  private get sourceParam(): string {
    const {query} = this.props.location

    if (!query.sourceID) {
      return ''
    }

    return `sourceID=${query.sourceID}`
  }

  private get emptyState(): JSX.Element {
    const {onCreateDashboard, searchTerm} = this.props

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text text="No dashboards match your search term" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Looks like you don’t have any dashboards" />
        <Button
          text="Create a Dashboard"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
          onClick={onCreateDashboard}
        />
      </EmptyState>
    )
  }
}

export default withRouter(DashboardsTable)
