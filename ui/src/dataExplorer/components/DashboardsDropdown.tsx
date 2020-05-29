import React, {PureComponent} from 'react'

// utils
import _ from 'lodash'

// components
import {Dropdown, DropdownItemType} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// types
import {Dashboard} from 'src/types'
import {DashboardTemplate} from 'src/dashboards/constants'

interface Props {
  onSelect: (selectedIDs: string[], value: Dashboard) => void
  selectedIDs: string[]
  dashboards: Dashboard[]
  newDashboardName: string
}

@ErrorHandling
class DashboardsDropdown extends PureComponent<Props> {
  public render() {
    return (
      <Dropdown
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            testID="save-as-dashboard-cell--dropdown"
          >
            {this.dropdownLabel}
          </Dropdown.Button>
        )}
        menu={() => (
          <Dropdown.Menu testID="save-as-dashboard-cell--dropdown-menu">
            {this.dropdownItems}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private handleSelectDashboard = (dashboard: Dashboard): void => {
    const {onSelect, selectedIDs} = this.props

    let updatedSelection: string[]

    if (selectedIDs.includes(dashboard.id)) {
      updatedSelection = selectedIDs.filter(id => id !== dashboard.id)
    } else {
      updatedSelection = [...selectedIDs, dashboard.id]
    }

    onSelect(updatedSelection, dashboard)
  }

  private get dropdownLabel(): string {
    const {dashboards, selectedIDs, newDashboardName} = this.props

    if (!selectedIDs.length) {
      return 'Choose at least 1 dashboard'
    }

    const dashboardsWithNew = [
      ...dashboards,
      {...DashboardTemplate, name: newDashboardName},
    ]

    return dashboardsWithNew
      .filter(d => selectedIDs.includes(d.id))
      .map(d => d.name)
      .join(', ')
  }

  private get dropdownItems(): JSX.Element[] {
    const {dashboards, selectedIDs} = this.props
    const dashboardItems = dashboards.map(d => {
      return (
        <Dropdown.Item
          id={d.id}
          key={d.id}
          value={d}
          type={DropdownItemType.Checkbox}
          onClick={this.handleSelectDashboard}
          selected={selectedIDs.includes(d.id)}
        >
          {d.name}
        </Dropdown.Item>
      )
    })

    return [this.newDashboardItem, this.dividerItem, ...dashboardItems]
  }

  private get newDashboardItem(): JSX.Element {
    const {selectedIDs} = this.props

    return (
      <Dropdown.Item
        id={DashboardTemplate.id}
        key={DashboardTemplate.id}
        value={DashboardTemplate}
        type={DropdownItemType.Checkbox}
        onClick={this.handleSelectDashboard}
        selected={selectedIDs.includes(DashboardTemplate.id)}
        testID="save-as-dashboard-cell--create-new-dash"
      >
        {DashboardTemplate.name}
      </Dropdown.Item>
    )
  }

  private get dividerItem(): JSX.Element {
    return <Dropdown.Divider id="divider" key="existing-dashboards" />
  }
}
export default DashboardsDropdown
