// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import VariableForm from 'src/variables/components/VariableForm'

// Utils
import {createVariable} from 'src/variables/actions'
import {extractVariablesList} from 'src/variables/selectors'

// Types
import {AppState} from 'src/types'
import {getActiveQuery} from 'src/timeMachine/selectors'
import {IVariable as Variable} from '@influxdata/influx'

interface OwnProps {
  onHideOverlay: () => void
}

interface DispatchProps {
  onCreateVariable: typeof createVariable
}

interface StateProps {
  initialScript?: string
  variables: Variable[]
}

type Props = StateProps & DispatchProps & OwnProps

class SaveAsVariable extends PureComponent<Props & WithRouterProps> {
  render() {
    const {
      onHideOverlay,
      onCreateVariable,
      initialScript,
      variables,
    } = this.props

    return (
      <VariableForm
        onHideOverlay={onHideOverlay}
        onCreateVariable={onCreateVariable}
        initialScript={initialScript}
        variables={variables}
      />
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const variables = extractVariablesList(state)

  return {
    initialScript: activeQuery.text,
    variables,
  }
}

const mdtp = {
  onCreateVariable: createVariable,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(SaveAsVariable))
