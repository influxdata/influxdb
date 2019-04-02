// Libraries
import React from 'react'
import {render} from 'react-testing-library'
import {IconFont} from 'src/clockface'
import _ from 'lodash'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

// Constants
const DASHBOARDS_NAV_ITEM = 'Dashboards'
const TASKS_NAV_ITEM = 'Tasks'

function Nav(props) {
  const {pathname} = props

  return (
    <NavMenu>
      <NavMenu.Item
        title={DASHBOARDS_NAV_ITEM}
        path="/orgs/036ef4599dddb000/dashboards"
        icon={IconFont.Dashboards}
        active={getNavItemActivation(['dashboards'], pathname)}
      />
      <NavMenu.Item
        title={TASKS_NAV_ITEM}
        path="/orgs/036ef4599dddb000/tasks"
        icon={IconFont.Calendar}
        active={getNavItemActivation(['tasks'], pathname)}
      />
    </NavMenu>
  )
}

const setup = (override?) => {
  const props = {
    pathname: '/',
    ...override,
  }

  return render(<Nav {...props} />)
}

describe('Nav', () => {
  it('only highlights one nav item', () => {
    const pathname = '/orgs/036ef4599dddb000/dashboards'
    const {getByTestId} = setup({pathname})

    const dashItem = getByTestId(`nav-menu--item ${DASHBOARDS_NAV_ITEM}`)
    const tasksItem = getByTestId(`nav-menu--item ${TASKS_NAV_ITEM}`)

    expect(dashItem.className).toContain('active')
    expect(tasksItem.className).not.toContain('active')
  })
})
