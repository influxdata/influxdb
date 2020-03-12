import {getOrgIDFromUrl} from 'src/shared/utils/getOrgIDFromUrl'

const baseUrl = 'http://influx.application/'
const orgID = 'f440096db5cdb0b1'
const pathname = `/orgs/${orgID}`
const location = {
  ...window.location,
  href: `${baseUrl}${pathname}`,
  pathname,
}
Object.defineProperty(window, 'location', {
  writable: true,
  value: location,
})

describe('getting an orgId', () => {
  it('gets the org id from the simplest url', () => {
    expect(getOrgIDFromUrl()).toEqual(orgID)
  })

  it("returns -1 if it can't find the org id", () => {
    const newPathname = '/'
    window.location.pathname = newPathname
    window.location.href = `${baseUrl}${pathname}`
    expect(getOrgIDFromUrl()).toEqual('-1')
  })

  it('handles paths with more stuff after the org id', () => {
    let newPathname = `/orgs/${orgID}/dashboard/1333333333337`
    window.location.pathname = newPathname
    window.location.href = `${baseUrl}${pathname}`
    expect(getOrgIDFromUrl()).toEqual(orgID)

    newPathname = `/orgs/${orgID}/data-explorer`
    window.location.pathname = newPathname
    window.location.href = `${baseUrl}${pathname}`
    expect(getOrgIDFromUrl()).toEqual(orgID)
  })
})
