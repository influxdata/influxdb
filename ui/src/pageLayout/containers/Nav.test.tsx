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
        link="/dashboards"
        icon={IconFont.Dashboards}
        location={pathname}
        highlightPaths={['dashboards']}
      />
      <NavMenu.Item
        title="Organizations"
        link="/organizations"
        icon={IconFont.UsersDuo}
        location={pathname}
        highlightPaths={['organizations']}
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
    const pathname = '/organizations/036ef4599dddb000/dashboards'
    const {getByTestId} = setup({pathname})

    const dashItem = getByTestId(`nav-menu-item ${IconFont.Dashboards}`)
    const orgItem = getByTestId(`nav-menu-item ${IconFont.UsersDuo}`)

    expect(dashItem.className).not.toContain('active')
    expect(orgItem.className).toContain('active')
  })
})
