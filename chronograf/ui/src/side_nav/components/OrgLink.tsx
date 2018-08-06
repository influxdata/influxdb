// Libraries
import classnames from 'classnames'
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Types
import {Me, Role} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OrgID {
  organization: string
}

interface Props {
  me: Me
  role: Role
  meLink: string
  onMeChangeOrg: (meLink: string, orgID: OrgID) => void
}

@ErrorHandling
class OrgLink extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {role} = this.props

    return (
      <span className={this.className} onClick={this.handleChangeOrganization}>
        {this.orgName} <strong>({role.name})</strong>
      </span>
    )
  }

  private get orgName(): string {
    const {me, role} = this.props
    const org = me.organizations.find(o => o.id === role.organization)

    if (!org) {
      return ''
    }

    return org.name
  }

  private get isCurrentOrg(): boolean {
    const {me, role} = this.props
    return me.currentOrganization.id === role.organization
  }

  private get className(): string {
    return classnames('sidebar-menu--item', {
      active: this.isCurrentOrg,
    })
  }

  private handleChangeOrganization = async () => {
    const {router, meLink, onMeChangeOrg, role} = this.props
    try {
      await onMeChangeOrg(meLink, {organization: role.organization})
      router.push('')
    } catch (error) {
      console.error(error)
    }
  }
}

export default withRouter<Props>(OrgLink)
