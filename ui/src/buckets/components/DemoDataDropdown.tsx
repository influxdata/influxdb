// Libraries
import React, {FC} from 'react'
import _ from 'lodash'

// Components
import {IconFont, ComponentColor, Dropdown} from '@influxdata/clockface'

// Types
import {Bucket} from 'src/types'
import {getDemoDataBucketMembership} from 'src/cloud/actions/demodata'

interface Props {
  buckets: Bucket[]
  getMembership: typeof getDemoDataBucketMembership
}

const DemoDataDropdown: FC<Props> = ({buckets, getMembership}) => {
  const demoDataItems = buckets.map(b => (
    <Dropdown.Item
      testID={`dropdown-item--demodata-${b.name}`}
      id={b.id}
      key={b.id}
      value={b}
      onClick={getMembership}
    >
      {b.name}
    </Dropdown.Item>
  ))

  return (
    <Dropdown
      testID="dropdown--demodata"
      style={{width: '160px', marginRight: '8px'}}
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
        <Dropdown.Menu onCollapse={onCollapse}>{demoDataItems}</Dropdown.Menu>
      )}
    />
  )
}

export default DemoDataDropdown
