// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'
import {WithRouterProps, withRouter} from 'react-router-dom'

// Components
import {IconFont, Icon} from '@influxdata/clockface'

interface ComponentProps {
  orgName: string
  orgID: string
  selected: boolean
  onDismiss: () => void
}

type Props = ComponentProps & WithRouterProps

const OrgSwitcherItem: FC<Props> = ({
  orgName,
  selected,
  onDismiss,
  orgID,
  router,
}) => {
  const orgSwitcherItemClass = classnames('org-switcher--item', {
    'org-switcher--item__selected': selected,
  })

  const orgSwitcherIcon = selected ? IconFont.Checkmark : IconFont.CaretRight

  const handleClick = (): void => {
    onDismiss()
    router.push(`orgs/${orgID}`)
  }

  const currentOrgIndicator = selected ? <em>Current</em> : null

  return (
    <li className={orgSwitcherItemClass} onClick={handleClick}>
      <div className="org-switcher--item-circle">
        <Icon glyph={orgSwitcherIcon} className="org-switcher--item-icon" />
      </div>
      <span className="org-switcher--item-label">
        {orgName}
        {currentOrgIndicator}
      </span>
    </li>
  )
}

export default withRouter(OrgSwitcherItem)
