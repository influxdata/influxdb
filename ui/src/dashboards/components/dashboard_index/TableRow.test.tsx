// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import TableRow from 'src/dashboards/components/dashboard_index/TableRow'
import {Label} from 'src/clockface'

// Dummy Data
import {dashboardWithLabels, dashboard, orgs} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    dashboard,
    orgs,
    onDeleteDashboard: jest.fn(),
    onCloneDashboard: jest.fn(),
    onExportDashboard: jest.fn(),
    onUpdateDashboard: jest.fn(),
    onEditLabels: jest.fn(),
    ...override,
  }

  return shallow(<TableRow {...props} />)
}

describe('Dashboard index row', () => {
  it('renders with no labels', () => {
    const wrapper = setup()

    expect(wrapper.exists()).toBe(true)
  })

  describe('if there are labels', () => {
    it('renders correctly', () => {
      const wrapper = setup({dashboard: dashboardWithLabels})

      const labelContainer = wrapper.find(Label.Container)
      const labels = wrapper.find(Label)

      expect(labelContainer.exists()).toBe(true)
      expect(labels.length).toBe(dashboardWithLabels.labels.length)
    })
  })
})
