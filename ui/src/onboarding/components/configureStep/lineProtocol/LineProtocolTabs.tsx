// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

import PrecisionDropdown from 'src/onboarding/components/configureStep/lineProtocol/PrecisionDropdown'
import TabSelector from 'src/onboarding/components/configureStep/lineProtocol/TabSelector'
import TabBody from 'src/onboarding/components/configureStep/lineProtocol/TabBody'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

// Components
import {Grid, Columns} from 'src/clockface'

// Actions
import {
  setLineProtocolBody,
  setActiveLPTab,
  writeLineProtocolAction,
  setPrecision,
} from 'src/onboarding/actions/dataLoaders'

import {AppState} from 'src/types/v2/index'

// Styles
import 'src/clockface/components/auto_input/AutoInput.scss'
import {WritePrecision} from 'src/api'

interface OwnProps {
  tabs: LineProtocolTab[]
  bucket: string
  org: string
  handleSubmit?: () => void
}

type Props = OwnProps & DispatchProps & StateProps

interface DispatchProps {
  setLineProtocolBody: typeof setLineProtocolBody
  setActiveLPTab: typeof setActiveLPTab
  writeLineProtocolAction: typeof writeLineProtocolAction
  setPrecision: typeof setPrecision
}

interface StateProps {
  lineProtocolBody: string
  activeLPTab: LineProtocolTab
  precision: WritePrecision
}

interface State {
  urlInput: string
}

export class LineProtocolTabs extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      urlInput: '',
    }
  }

  public render() {
    const {
      setPrecision,
      precision,
      activeLPTab,
      tabs,
      setLineProtocolBody,
      lineProtocolBody,
      handleSubmit,
    } = this.props

    const {urlInput} = this.state

    return (
      <div>
        <TabSelector
          activeLPTab={activeLPTab}
          tabs={tabs}
          onClick={this.handleTabClick}
        />

        <Grid>
          <Grid.Row>
            <Grid.Column
              widthXS={Columns.Twelve}
              widthMD={Columns.Ten}
              offsetMD={Columns.One}
              widthLG={Columns.Eight}
              offsetLG={Columns.Two}
            >
              <div className="onboarding--admin-user-form">
                <div className={'wizard-step--lp-body'}>
                  <TabBody
                    onURLChange={this.handleURLChange}
                    activeLPTab={activeLPTab}
                    precision={precision}
                    urlInput={urlInput}
                    lineProtocolBody={lineProtocolBody}
                    setLineProtocolBody={setLineProtocolBody}
                    handleSubmit={handleSubmit}
                  />
                </div>

                <PrecisionDropdown
                  setPrecision={setPrecision}
                  precision={precision}
                />
              </div>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </div>
    )
  }

  private handleTabClick = (tab: LineProtocolTab) => {
    const {setActiveLPTab, setLineProtocolBody} = this.props

    setLineProtocolBody('')
    setActiveLPTab(tab)
  }

  private handleURLChange = (urlInput: string) => {
    this.setState({urlInput})
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lineProtocolBody, activeLPTab, precision},
  },
}: AppState) => {
  return {lineProtocolBody, activeLPTab, precision}
}

const mdtp: DispatchProps = {
  setLineProtocolBody,
  setActiveLPTab,
  writeLineProtocolAction,
  setPrecision,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocolTabs)
