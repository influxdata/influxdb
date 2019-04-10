// Libraries
import React from 'react'
import {render} from 'react-testing-library'
import _ from 'lodash'

// Components
import {NavMenu, Icon, IconFont} from '@influxdata/clockface'

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
        titleLink={className => (
          <a href="#" className={className}>
            {DASHBOARDS_NAV_ITEM}
          </a>
        )}
        iconLink={className => (
          <a href="#" className={className}>
            <Icon glyph={IconFont.Dashboards} />
          </a>
        )}
        active={getNavItemActivation(['dashboards'], pathname)}
        testID={`nav-menu--item ${DASHBOARDS_NAV_ITEM}`}
      />
      <NavMenu.Item
        titleLink={className => (
          <a href="#" className={className}>
            {TASKS_NAV_ITEM}
          </a>
        )}
        iconLink={className => (
          <a href="#" className={className}>
            <Icon glyph={IconFont.Calendar} />
          </a>
        )}
        active={getNavItemActivation(['tasks'], pathname)}
        testID={`nav-menu--item ${TASKS_NAV_ITEM}`}
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
