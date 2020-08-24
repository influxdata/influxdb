// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

// Contexts
import {PipeContext} from 'src/notebooks/context/pipe'

// Constants
import {FUNCTIONS, QueryFn} from 'src/timeMachine/constants/queryBuilder'

// Utils
import {event} from 'src/cloud/utils/reporting'

const AggregateFunctionSelector: FC = () => {
  const {data, update} = useContext(PipeContext)
  const selectedFunction = data?.aggregateFunction || FUNCTIONS[0]

  const updateSelectedFunction = useCallback(
    (aggregateFunction: QueryFn): void => {
      event(
        `Updating the Aggregate function in the Flow Query Builder to ${aggregateFunction.name}`
      )
      update({aggregateFunction})
    },
    [update]
  )

  const menuItems = (
    <>
      {FUNCTIONS.map(func => (
        <Dropdown.Item
          key={func.name}
          value={func}
          onClick={updateSelectedFunction}
          selected={func.name === selectedFunction.name}
          title={func.name}
          wrapText={true}
        >
          {func.name}
        </Dropdown.Item>
      ))}
    </>
  )

  const button = (active, onClick) => (
    <Dropdown.Button onClick={onClick} active={active}>
      {selectedFunction.name}
    </Dropdown.Button>
  )

  const menu = onCollapse => (
    <Dropdown.Menu onCollapse={onCollapse}>{menuItems}</Dropdown.Menu>
  )

  return <Dropdown button={button} menu={menu} style={{width: '130px'}} />
}

export default AggregateFunctionSelector
