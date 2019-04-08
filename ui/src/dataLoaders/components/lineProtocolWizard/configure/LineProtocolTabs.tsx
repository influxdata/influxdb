// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Grid, Columns} from '@influxdata/clockface'
import PrecisionDropdown from 'src/dataLoaders/components/lineProtocolWizard/configure/PrecisionDropdown'
import TabSelector from 'src/dataLoaders/components/lineProtocolWizard/configure/TabSelector'
import TabBody from 'src/dataLoaders/components/lineProtocolWizard/configure/TabBody'

// Types
import {AppState, LineProtocolTab} from 'src/types'
import {WritePrecision} from '@influxdata/influx'

// Actions
import {
  setLineProtocolBody,
  setActiveLPTab,
  writeLineProtocolAction,
  setPrecision,
} from 'src/dataLoaders/actions/dataLoaders'

// Styles
import 'src/clockface/components/auto_input/AutoInput.scss'

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
                <div className="wizard-step--lp-body">
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
