import React, {PureComponent} from 'react'

// utils
import _ from 'lodash'

// components
import {MultiSelectDropdown} from 'src/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// types
import {Dashboard} from 'src/types/v2'
import {DashboardTemplate} from 'src/dashboards/constants'

interface Props {
  onSelect: (selectedIDs: string[], value: Dashboard) => void
  selectedIDs: string[]
  dashboards: Dashboard[]
}

@ErrorHandling
class DashboardsDropdown extends PureComponent<Props> {
  public render() {
    const {selectedIDs, onSelect} = this.props
    return (
      <MultiSelectDropdown
        selectedIDs={selectedIDs}
        onChange={onSelect}
        emptyText={'Choose at least 1 dashboard'}
      >
        {this.dropdownItems}
      </MultiSelectDropdown>
    )
  }

  private get dropdownItems(): JSX.Element[] {
    const {dashboards} = this.props
    const dashboardItems = dashboards.map(d => {
      return (
        <MultiSelectDropdown.Item id={d.id} key={d.id} value={d}>
          {d.name}
        </MultiSelectDropdown.Item>
      )
    })

    return [this.newDashboardItem, this.dividerItem, ...dashboardItems]
  }

  private get newDashboardItem(): JSX.Element {
    return (
      <MultiSelectDropdown.Item
        id={DashboardTemplate.id}
        key={DashboardTemplate.id}
        value={DashboardTemplate}
      >
        {DashboardTemplate.name}
      </MultiSelectDropdown.Item>
    )
  }

  private get dividerItem(): JSX.Element {
    return <MultiSelectDropdown.Divider key={0} />
  }
}
export default DashboardsDropdown
