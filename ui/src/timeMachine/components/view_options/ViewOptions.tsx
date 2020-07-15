// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {setType} from 'src/timeMachine/actions'

// Components
import OptionsSwitcher from 'src/timeMachine/components/view_options/OptionsSwitcher'
import {Grid, DapperScrollbars} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class ViewOptions extends PureComponent<Props> {
  public render() {
    return (
      <div className="view-options">
        <DapperScrollbars
          autoHide={false}
          style={{width: '100%', height: '100%'}}
        >
          <div className="view-options--container">
            <Grid>
              <Grid.Row>
                <OptionsSwitcher view={this.props.view} />
              </Grid.Row>
            </Grid>
          </div>
        </DapperScrollbars>
      </div>
    )
  }
}

const mstp = (state: AppState) => {
  const {view} = getActiveTimeMachine(state)

  return {view}
}

const mdtp = {
  onUpdateType: setType,
}

const connector = connect(mstp, mdtp)

export default connector(ViewOptions)
