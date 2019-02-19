// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import VariableRow from 'src/organizations/components/VariableRow'

// Types
import {Variable} from '@influxdata/influx'

interface Props {
  variables: Variable[]
  emptyState: JSX.Element
  onDeleteVariable: (variable: Variable) => void
}

class VariableList extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {emptyState, variables, onDeleteVariable} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="60%" />
            <IndexList.HeaderCell columnName="Type" width="40%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {variables.map(variable => (
              <VariableRow
                key={`variable-${variable.id}`}
                variable={variable}
                onDeleteVariable={onDeleteVariable}
              />
            ))}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }
}

export default VariableList
