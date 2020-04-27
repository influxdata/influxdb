// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {get, sortBy} from 'lodash'

// Utils
import {getAll} from 'src/resources/selectors'

// Actions
import {getDemoDataBucketMembership as getDemoDataBucketMembershipAction} from 'src/cloud/actions/demodata'

// Components
import {
  IconFont,
  ComponentColor,
  Dropdown,
  DropdownItemType,
} from '@influxdata/clockface'

// Types
import {AppState, Bucket, ResourceType} from 'src/types'

interface StateProps {
  ownBuckets: Bucket[]
  demoDataBuckets: Bucket[]
}

interface DispatchProps {
  getDemoDataBucketMembership: typeof getDemoDataBucketMembershipAction
}

type Props = DispatchProps & StateProps

const DemoDataDropdown: FC<Props> = ({
  ownBuckets,
  demoDataBuckets,
  getDemoDataBucketMembership,
}) => {
  if (!demoDataBuckets.length) {
    return null
  }

  const ownBucketNames = ownBuckets.map(o => o.name.toLocaleLowerCase())

  const sortedBuckets = sortBy(demoDataBuckets, d => {
    return d.name.toLocaleLowerCase()
  })

  const dropdownItems = sortedBuckets.map(b => {
    if (ownBucketNames.includes(b.name.toLocaleLowerCase())) {
      return (
        <Dropdown.Item
          testID={`dropdown-item--demodata-${b.name}`}
          id={b.id}
          key={b.id}
          value={b}
          onClick={() => {}}
          style={{fontStyle: 'italic'}}
          selected={true}
          type={DropdownItemType.Dot}
        >
          {b.name}
        </Dropdown.Item>
      )
    }

    return (
      <Dropdown.Item
        testID={`dropdown-item--demodata-${b.name}`}
        id={b.id}
        key={b.id}
        value={b}
        onClick={getDemoDataBucketMembership}
        selected={false}
        type={DropdownItemType.Dot}
      >
        {b.name}
      </Dropdown.Item>
    )
  })

  return (
    <Dropdown
      testID="dropdown--demodata"
      style={{width: '200px', marginRight: '8px'}}
      button={(active, onClick) => (
        <Dropdown.Button
          active={active}
          onClick={onClick}
          icon={IconFont.Plus}
          color={ComponentColor.Secondary}
          testID="dropdown-button--demodata"
        >
          Add Demo Data
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu onCollapse={onCollapse}>{dropdownItems}</Dropdown.Menu>
      )}
    />
  )
}

const mstp = (state: AppState): StateProps => ({
  ownBuckets: getAll<Bucket>(state, ResourceType.Buckets),
  demoDataBuckets: get(state, 'cloud.demoData.buckets', []) as Bucket[],
})

const mdtp: DispatchProps = {
  getDemoDataBucketMembership: getDemoDataBucketMembershipAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(DemoDataDropdown)
