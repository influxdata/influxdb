// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {NavMenu} from '@influxdata/clockface'
import {Organization} from '@influxdata/influx'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'

interface Props {
  orgs: Organization[]
  isShowingOrganizations: boolean
  showOrganizationsView: () => void
  closeOrganizationsView: () => void
}

class AccountNavSubItem extends PureComponent<Props> {
  render() {
    const {orgs, isShowingOrganizations, showOrganizationsView} = this.props

    if (isShowingOrganizations) {
      return (
        <SortingHat list={orgs} sortKey="name">
          {this.orgs}
        </SortingHat>
      )
    }

    return (
      <>
        <CloudExclude key="feature-flag">
          {orgs.length > 1 && (
            <NavMenu.SubItem
              titleLink={className => (
                <div onClick={showOrganizationsView} className={className}>
                  Switch Organizations
                </div>
              )}
              active={false}
              key="switch-orgs"
            />
          )}

          <NavMenu.SubItem
            titleLink={className => (
              <Link to="/orgs/new" className={className}>
                Create Organization
              </Link>
            )}
            active={false}
          />
        </CloudExclude>

        <NavMenu.SubItem
          titleLink={className => (
            <Link to="/logout" className={className}>
              Logout
            </Link>
          )}
          active={false}
          key="logout"
        />
      </>
    )
  }

  private orgs = (orgs: Organization[]): JSX.Element => {
    const {closeOrganizationsView} = this.props

    return (
      <>
        {orgs.reduce(
          (acc, org) => {
            acc.push(
              <NavMenu.SubItem
                titleLink={className => (
                  <a
                    className={className}
                    href={`/orgs/${org.id}`}
                    style={{display: 'block'}}
                  >
                    {org.name}
                  </a>
                )}
                key={org.id}
                active={false}
              />
            )
            return acc
          },

          [
            <NavMenu.SubItem
              titleLink={className => (
                <div className={className} onClick={closeOrganizationsView}>
                  {'< Back'}
                </div>
              )}
              active={false}
              key="back-button"
              className="back-button"
            />,
          ]
        )}
      </>
    )
  }
}

export default AccountNavSubItem
