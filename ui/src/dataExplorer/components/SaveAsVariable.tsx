// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import VariableForm from 'src/organizations/components/VariableForm'

// Utils
import {getActiveOrg} from 'src/organizations/selectors'
import {createVariable} from 'src/variables/actions'

// Types
import {AppState} from 'src/types'
import {Variable} from '@influxdata/influx'
import {getActiveQuery} from 'src/timeMachine/selectors'

interface OwnProps {
  onHideOverlay: () => void
}

interface DispatchProps {
  onCreateVariable: (variable: Variable) => void
}

interface StateProps {
  initialScript?: string
  orgID: string
}

type Props = StateProps & DispatchProps & OwnProps

class SaveAsVariable extends PureComponent<Props> {
  render() {
    const {orgID, onHideOverlay, onCreateVariable, initialScript} = this.props

    return (
      <VariableForm
        orgID={orgID}
        onHideOverlay={onHideOverlay}
        onCreateVariable={onCreateVariable}
        initialScript={initialScript}
      />
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state)
  const activeOrgID = getActiveOrg(state).id

  return {
    orgID: activeOrgID,
    initialScript: activeQuery.text,
  }
}

const mdtp = {
  onCreateVariable: createVariable,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(SaveAsVariable)
