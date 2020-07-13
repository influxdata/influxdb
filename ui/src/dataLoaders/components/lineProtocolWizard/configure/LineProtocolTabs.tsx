// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import PrecisionDropdown from 'src/dataLoaders/components/lineProtocolWizard/configure/PrecisionDropdown'
import TabSelector from 'src/dataLoaders/components/lineProtocolWizard/configure/TabSelector'
import TabBody from 'src/dataLoaders/components/lineProtocolWizard/configure/TabBody'

// Types
import {AppState, LineProtocolTab} from 'src/types'

// Actions
import {
  setLineProtocolBody,
  setActiveLPTab,
  setPrecision,
} from 'src/dataLoaders/actions/dataLoaders'

interface OwnProps {
  tabs: LineProtocolTab[]
  bucket: string
  org: string
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

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
    } = this.props

    const {urlInput} = this.state

    return (
      <>
        <div className="line-protocol--header">
          <TabSelector
            activeLPTab={activeLPTab}
            tabs={tabs}
            onClick={this.handleTabClick}
          />
          <PrecisionDropdown
            setPrecision={setPrecision}
            precision={precision}
          />
        </div>
        <TabBody
          onURLChange={this.handleURLChange}
          activeLPTab={activeLPTab}
          precision={precision}
          urlInput={urlInput}
          lineProtocolBody={lineProtocolBody}
          setLineProtocolBody={setLineProtocolBody}
        />
      </>
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

const mdtp = {
  setLineProtocolBody,
  setActiveLPTab,
  setPrecision,
}

const connector = connect(mstp, mdtp)

export default connector(LineProtocolTabs)
