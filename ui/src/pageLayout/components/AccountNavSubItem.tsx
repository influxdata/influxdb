// Libraries
import React, {SFC} from 'react'

// Components
import NavMenu from 'src/pageLayout/components/NavMenu'
import {Organization} from '@influxdata/influx'
import {NavMenuType} from 'src/clockface'
import CloudFeatureFlag from 'src/shared/components/CloudFeatureFlag'

interface Props {
  orgs: Organization[]
  showOrganizations: boolean
  toggleOrganizationsView: () => void
}

const AccountNavSubItem: SFC<Props> = ({
  orgs,
  showOrganizations,
  toggleOrganizationsView,
}) => {
  if (showOrganizations) {
    return (
      <>
        {orgs.reduce(
          (acc, org) => {
            acc.push(
              <NavMenu.SubItem
                title={org.name}
                path={`/orgs/${org.id}`}
                key={org.id}
                type={NavMenuType.RouterLink}
                active={false}
              />
            )
            return acc
          },
          [
            <NavMenu.SubItem
              title="< Back"
              onClick={toggleOrganizationsView}
              type={NavMenuType.ShowDropdown}
              active={false}
              key="back-button"
              className="back-button"
            />,
          ]
        )}
      </>
    )
  }

  return (
    <>
      <CloudFeatureFlag key="feature-flag">
        {orgs.length > 1 && (
          <NavMenu.SubItem
            title="Switch Organizations"
            onClick={toggleOrganizationsView}
            type={NavMenuType.ShowDropdown}
            active={false}
            key="switch-orgs"
          />
        )}
      </CloudFeatureFlag>

      <NavMenu.SubItem
        title="Logout"
        path="/logout"
        active={false}
        key="logout"
      />
    </>
  )
}

export default AccountNavSubItem
