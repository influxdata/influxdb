// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Overlay, Sort} from '@influxdata/clockface'
import OrgSwitcherItem from 'src/pageLayout/components/OrgSwitcherItem'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

interface ComponentProps {
  onClose: () => void
}

interface StateProps {
  orgs: Organization[]
  currentOrg: Organization
}

type Props = ComponentProps & StateProps

const OrgSwitcherOverlay: FC<Props> = ({orgs, onClose, currentOrg}) => {
  return (
    <Overlay.Container maxWidth={500}>
      <Overlay.Header
        title="Switch Organizations"
        onDismiss={onClose}
        testID="switch-overlay--header"
      />
      <Overlay.Body>
        <p className="org-switcher--prompt">Choose an organization</p>
        <SortingHat list={orgs} sortKey="name" direction={Sort.Ascending}>
          {sortedOrgs => (
            <div className="org-switcher--list">
              {sortedOrgs.map(org => (
                <OrgSwitcherItem
                  key={org.id}
                  orgID={org.id}
                  orgName={org.name}
                  selected={org.id === currentOrg.id}
                  onDismiss={onClose}
                />
              ))}
            </div>
          )}
        </SortingHat>
      </Overlay.Body>
    </Overlay.Container>
  )
}

const mstp = (state: AppState) => {
  const orgs = getAll<Organization>(state, ResourceType.Orgs)
  const currentOrg = getOrg(state)

  return {
    orgs,
    currentOrg,
  }
}

export default connect<StateProps, {}, ComponentProps>(
  mstp,
  null
)(OrgSwitcherOverlay)
