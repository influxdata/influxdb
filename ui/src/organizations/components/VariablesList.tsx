// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import VariableRow from 'src/organizations/components/VariableRow'

// Types
import {Macro} from '@influxdata/influx'

interface Props {
  variables: Macro[]
  emptyState: JSX.Element
}

class VariablesList extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {emptyState, variables} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="60%" />
            <IndexList.HeaderCell columnName="Type" width="40%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {variables.map(variable => (
              <VariableRow key={variable.id} variable={variable} />
            ))}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }
}

export default VariablesList
