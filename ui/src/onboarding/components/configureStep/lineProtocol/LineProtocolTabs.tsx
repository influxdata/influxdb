// Libraries
import React, {PureComponent} from 'react'

// Components
import {Radio, ButtonShape} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

interface State {
  activeTab: LineProtocolTab
}

interface Props {
  tabs: LineProtocolTab[]
}

class LineProtocolTabs extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {activeTab: this.props.tabs[0]}
  }
  public render() {
    return (
      <>
        {this.tabSelector}
        {this.tabBody}
      </>
    )
  }

  private handleTabClick = (tab: LineProtocolTab) => () => {
    this.setState({activeTab: tab})
  }

  private get tabSelector(): JSX.Element {
    const {tabs} = this.props
    const {activeTab} = this.state
    return (
      <Radio shape={ButtonShape.Default}>
        {tabs.map(t => (
          <Radio.Button
            key={t}
            id={t}
            titleText={t}
            value={t}
            active={activeTab === t}
            onClick={this.handleTabClick(t)}
          >
            {t}
          </Radio.Button>
        ))}
      </Radio>
    )
  }

  private get tabBody(): JSX.Element {
    const {activeTab} = this.state
    if (activeTab === LineProtocolTab.UploadFile) {
      return <DragAndDrop submitText="Upload File" />
    } else {
      return null
    }
  }
}

export default LineProtocolTabs
