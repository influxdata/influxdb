// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import OrgLink from 'src/side_nav/components/OrgLink'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

// Actions
import {meChangeOrganizationAsync} from 'src/shared/actions/auth'

// Constants
import {SUPERADMIN_ROLE} from 'src/auth/Authorized'

// Types
import {Me} from 'src/types'
import {Links, ExternalLink} from 'src/types/auth'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OrgID {
  organization: string
}

interface Props {
  me: Me
  links: Links
  logoutLink: string
  meChangeOrg: (meLink: string, orgID: OrgID) => void
}

@ErrorHandling
class UserNavBlock extends PureComponent<Props> {
  public render() {
    const {logoutLink, me, links, meChangeOrg} = this.props

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user-outline" />
          {this.isSuperAdmin && (
            <span className="sidebar--icon sidebar--icon__superadmin icon crown2" />
          )}
        </div>
        <div className="sidebar-menu sidebar-menu--user-nav">
          {!!this.customLinks && (
            <div className="sidebar-menu--section sidebar-menu--section__custom-links">
              Custom Links
            </div>
          )}
          {!!this.customLinks &&
            this.customLinks.map((link, i) => (
              <a
                key={i}
                className="sidebar-menu--item sidebar-menu--item__link-name"
                href={link.url}
                target="_blank"
              >
                {link.name}
              </a>
            ))}
          <div className="sidebar-menu--section sidebar-menu--section__switch-orgs">
            Switch Organizations
          </div>
          <FancyScrollbar
            className="sidebar-menu--scrollbar"
            autoHeight={true}
            maxHeight={100}
            autoHide={false}
          >
            {me.roles.map((r, i) => (
              <OrgLink
                onMeChangeOrg={meChangeOrg}
                meLink={links.me}
                key={i}
                me={me}
                role={r}
              />
            ))}
          </FancyScrollbar>
          <div className="sidebar-menu--section sidebar-menu--section__account">
            Account
          </div>
          <div className="sidebar-menu--provider">
            <div>
              {me.scheme} / {me.provider}
            </div>
          </div>
          <a
            className="sidebar-menu--item sidebar-menu--item__logout"
            href={logoutLink}
          >
            Log out
          </a>
          <div className="sidebar-menu--heading sidebar--no-hover">
            {me.name}
          </div>
          <div className="sidebar-menu--triangle" />
        </div>
      </div>
    )
  }

  private get customLinks(): ExternalLink[] {
    return this.props.links.external.custom
  }

  private get isSuperAdmin(): boolean {
    return this.props.me.role === SUPERADMIN_ROLE
  }
}

const mdtp = {
  meChangeOrg: meChangeOrganizationAsync,
}

export default connect(null, mdtp)(UserNavBlock)
