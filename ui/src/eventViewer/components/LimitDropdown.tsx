// Libraries
import React, {FC} from 'react'
import {Dropdown} from '@influxdata/clockface'

// Types
import {EventViewerChildProps} from 'src/eventViewer/types'

const LIMIT_CHOICES = [50, 100, 500, 1000, 2000]

const LimitDropdown: FC<EventViewerChildProps> = ({state, dispatch}) => {
  const button = (active, onClick) => (
    <Dropdown.Button active={active} onClick={onClick}>
      {state.limit} Results Per Fetch
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>
      {LIMIT_CHOICES.map(limit => (
        <Dropdown.Item
          key={limit}
          id={String(limit)}
          value={limit}
          onClick={limit => dispatch({type: 'LIMIT_CHANGED', limit})}
        >
          {limit} Results Per Fetch
        </Dropdown.Item>
      ))}
    </Dropdown.Menu>
  )

  return <Dropdown widthPixels={165} button={button} menu={menu} />
}

export default LimitDropdown
