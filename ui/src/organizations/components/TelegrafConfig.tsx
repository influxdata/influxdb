// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import FetchTelegrafConfig from 'src/organizations/components/FetchTelegrafConfig'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState} from 'src/types'

interface OwnProps {}

interface DispatchProps {
  notify: typeof notifyAction
}

interface StateProps {
  telegrafConfigID: string
}

type Props = StateProps & DispatchProps & OwnProps

@ErrorHandling
export class TelegrafConfig extends PureComponent<Props> {
  public render() {
    const options = {
      tabIndex: 1,
      mode: 'toml',
      readonly: true,
      lineNumbers: true,
      autoRefresh: true,
      theme: 'time-machine',
      completeSingle: false,
    }

    return (
      <FetchTelegrafConfig
        telegrafConfigID={this.props.telegrafConfigID}
        notify={this.props.notify}
      >
        {telegrafConfig => (
          <ReactCodeMirror
            autoFocus={true}
            autoCursor={true}
            value={telegrafConfig}
            options={options}
            onBeforeChange={this.onBeforeChange}
            onTouchStart={this.onTouchStart}
          />
        )}
      </FetchTelegrafConfig>
    )
  }

  private onBeforeChange = () => {}
  private onTouchStart = () => {}
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigID},
  },
}: AppState): StateProps => ({
  telegrafConfigID,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafConfig)
