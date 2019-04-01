// Libraries
import React from 'react'
import {render} from 'react-testing-library'
import {IconFont} from 'src/clockface'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'

function Nav(props) {
  const {pathname} = props
  return (
    <>
      <NavMenu.Item
        title="Dashboards"
        link="/orgs/036ef4599dddb000/dashboards"
        icon={IconFont.Dashboards}
        location={pathname}
        highlightPaths={['dashboards']}
      />
      <NavMenu.Item
        title="Tasks"
        link="/orgs/036ef4599dddb000/tasks"
        icon={IconFont.Calendar}
        location={pathname}
        highlightPaths={['tasks']}
      />
    </>
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

    const dashItem = getByTestId(`nav-menu-item ${IconFont.Dashboards}`)
    const tasksItem = getByTestId(`nav-menu-item ${IconFont.Calendar}`)

    expect(dashItem.className).toContain('active')
    expect(tasksItem.className).not.toContain('active')
  })
})
