// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {Link, withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Types
import {Alignment} from 'src/clockface'
import {
  IndexListColumn,
  IndexListRow,
} from 'src/shared/components/index_views/IndexListTypes'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  ComponentSpacer,
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
      <IndexList
        columns={this.columns}
        rows={this.rows}
        emptyState={this.emptyState}
      />
    )
  }

  private get columns(): IndexListColumn[] {
    return [
      {
        key: 'dashboards--name',
        title: 'Name',
        size: 500,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'dashboards--owner',
        title: 'Owner',
        size: 100,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'dashboards--modified',
        title: 'Last Modified',
        size: 90,
        showOnHover: false,
        align: Alignment.Left,
      },
      {
        key: 'dashboards--default',
        title: 'Default',
        size: 90,
        showOnHover: false,
        align: Alignment.Center,
      },
      {
        key: 'dashboards--actions',
        title: '',
        size: 200,
        showOnHover: true,
        align: Alignment.Right,
      },
    ]
  }

  private get rows(): IndexListRow[] {
    const {
      onSetDefaultDashboard,
      defaultDashboardLink,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
    } = this.props

    return this.props.dashboards.map(d => ({
      disabled: false,
      columns: [
        {
          key: 'dashboards--name',
          contents: (
            <Link to={`/dashboards/${d.id}?${this.sourceParam}`}>{d.name}</Link>
          ),
        },
        {
          key: 'dashboards--owner',
          contents: 'You',
        },
        {
          key: 'dashboards--modified',
          contents: '12h ago',
        },
        {
          key: 'dashboards--default',
          contents: (
            <DefaultToggle
              dashboardLink={d.links.self}
              defaultDashboardLink={defaultDashboardLink}
              onChangeDefault={onSetDefaultDashboard}
            />
          ),
        },
        {
          key: 'dashboards--actions',
          contents: (
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
          ),
        },
      ],
    }))
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
      return <h4>No dashboards match your search term</h4>
    }

    return (
      <>
        <h4>Looks like you don’t have any dashboards</h4>
        <br />
        <Button
          text="Create a Dashboard"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
          onClick={onCreateDashboard}
        />
      </>
    )
  }
}

export default withRouter(DashboardsTable)
