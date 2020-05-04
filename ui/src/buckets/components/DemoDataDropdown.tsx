// Libraries
import React, {FC, useEffect} from 'react'
import {connect} from 'react-redux'
import {get, sortBy} from 'lodash'

// Actions
import {
  getDemoDataBucketMembership as getDemoDataBucketMembershipAction,
  getDemoDataBuckets as getDemoDataBucketsAction,
} from 'src/cloud/actions/demodata'

// Components
import {ComponentColor, Dropdown, Icon, IconFont} from '@influxdata/clockface'

// Types
import {AppState, Bucket, ResourceType} from 'src/types'

interface StateProps {
  ownBucketsByID: {[id: string]: Bucket}
  demoDataBuckets: Bucket[]
}

interface DispatchProps {
  getDemoDataBucketMembership: typeof getDemoDataBucketMembershipAction
  getDemoDataBuckets: typeof getDemoDataBucketsAction
}

type Props = DispatchProps & StateProps

const DemoDataDropdown: FC<Props> = ({
  ownBucketsByID,
  demoDataBuckets,
  getDemoDataBucketMembership,
  getDemoDataBuckets,
}) => {
  useEffect(() => {
    getDemoDataBuckets()
  }, [])

  if (!demoDataBuckets.length) {
    return null
  }

  const sortedBuckets = sortBy(demoDataBuckets, d => {
    return d.name.toLocaleLowerCase()
  })

  const dropdownItems = sortedBuckets.map(b => {
    if (ownBucketsByID[b.id]) {
      return (
        <Dropdown.Item
          testID={`dropdown-item--demodata-${b.name}`}
          className="demodata-dropdown--item__added"
          id={b.id}
          key={b.id}
          value={b}
          selected={true}
        >
          <div className="demodata-dropdown--item-contents">
            <Icon
              glyph={IconFont.Checkmark}
              className="demodata-dropdown--item-icon"
            />
            {b.name}
          </div>
        </Dropdown.Item>
      )
    }

    return (
      <Dropdown.Item
        testID={`dropdown-item--demodata-${b.name}`}
        className="demodata-dropdown--item"
        id={b.id}
        key={b.id}
        value={b}
        onClick={getDemoDataBucketMembership}
        selected={false}
      >
        <div className="demodata-dropdown--item-contents">
          <Icon
            glyph={IconFont.Checkmark}
            className="demodata-dropdown--item-icon"
          />
          {b.name}
        </div>
      </Dropdown.Item>
    )
  })

  return (
    <Dropdown
      testID="dropdown--demodata"
      style={{width: '220px'}}
      className="demodata-dropdown"
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
  ownBucketsByID: state.resources[ResourceType.Buckets].byID,
  demoDataBuckets: get(state, 'cloud.demoData.buckets', []) as Bucket[],
})

const mdtp: DispatchProps = {
  getDemoDataBucketMembership: getDemoDataBucketMembershipAction,
  getDemoDataBuckets: getDemoDataBucketsAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(DemoDataDropdown)
