// Libraries
import React, {FC} from 'react'
import _ from 'lodash'

// Components
import {Dropdown} from '@influxdata/clockface'
import {IconFont, ComponentColor} from '@influxdata/clockface'

// Types
import {Bucket} from 'src/types'

interface Props {
  buckets: Bucket[]
}

const DemoDataDropdown: FC<Props> = ({buckets}) => {
  const demoDataItems = buckets.map(b => (
    <Dropdown.Item
      testID="dropdown-item demodata--1"
      id={b.name}
      key={b.name}
      value={b.name}
      onClick={v => console.log(v)}
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
