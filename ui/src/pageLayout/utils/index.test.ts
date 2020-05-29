// Constants
import {HOMEPAGE_PATHNAME} from 'src/shared/constants'

// Utils
import {getNavItemActivation} from 'src/pageLayout/utils'

describe('pageLayout.utils.getNavItemActivation', () => {
  it('matches the homepage when the location pathname has fewer than 3 parts', () => {
    let pathname = ''
    expect(
      getNavItemActivation([HOMEPAGE_PATHNAME, 'account'], pathname)
    ).toEqual(true)

    pathname = '/orgs'
    expect(
      getNavItemActivation([HOMEPAGE_PATHNAME, 'account'], pathname)
    ).toEqual(true)

    pathname = '/orgs/3491cbaef55b4559'
    expect(
      getNavItemActivation([HOMEPAGE_PATHNAME, 'account'], pathname)
    ).toEqual(true)

    pathname = '/orgs/3491cbaef55b4559/some-path'
    expect(
      getNavItemActivation([HOMEPAGE_PATHNAME, 'account'], pathname)
    ).toEqual(false)

    pathname = '/orgs/3491cbaef55b4559'
    expect(getNavItemActivation(['not-homepage', 'account'], pathname)).toEqual(
      false
    )

    pathname = '/new/meme/reviews'
    expect(
      getNavItemActivation([HOMEPAGE_PATHNAME, 'account'], pathname)
    ).toEqual(false)
  })

  it('matches the given name strictly when it appears after the third slash in the pathname', () => {
    let given = 'data-explorer'
    const base = '/orgs/3491cbaef55b4559'

    expect(getNavItemActivation([given], '/orgs/1/data-explorer')).toEqual(true)
    expect(getNavItemActivation([given], '/new/meme/reviews')).toEqual(false)

    given = 'dashboards'
    expect(
      getNavItemActivation([given, 'another-name'], `${base}/${given}`)
    ).toEqual(true)

    given = 'tasks'
    expect(
      getNavItemActivation([given], `${base}/${given}/longer-path`)
    ).toEqual(true)

    given = 'alerting'
    expect(
      getNavItemActivation([given], `/different/basepath/${base}/${given}`)
    ).toEqual(true)

    given = 'alert-history'
    expect(getNavItemActivation([given], `/${given}`)).toEqual(false)

    given = 'load-data'
    expect(getNavItemActivation([given], `/two-slashes/${given}`)).toEqual(
      false
    )

    given = 'buckets'
    expect(getNavItemActivation([given], `${base}${given}/${given}`)).toEqual(
      true
    )

    given = 'telegrafs'
    expect(getNavItemActivation([given], `${base}/${given}${given}`)).toEqual(
      false
    )

    given = 'scrapers'
    expect(
      getNavItemActivation([given], `${base}/${given}${given}${given}`)
    ).toEqual(false)

    given = 'tokens'
    expect(getNavItemActivation([given], `${base}////////${given}`)).toEqual(
      true
    )
  })
})
