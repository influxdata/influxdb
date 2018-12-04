// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input, InputType, Radio, ButtonShape, Form} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import TextArea from 'src/clockface/components/inputs/TextArea'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

// Actions
import {
  setLineProtocolText,
  setActiveLPTab,
} from 'src/onboarding/actions/dataLoaders'

// Styles
import 'src/clockface/components/auto_input/AutoInput.scss'

interface OwnProps {
  tabs: LineProtocolTab[]
}

type Props = OwnProps & DispatchProps & StateProps

interface DispatchProps {
  setLineProtocolText: typeof setLineProtocolText
  setActiveLPTab: typeof setActiveLPTab
}

interface StateProps {
  lineProtocolText: string
  activeLPTab: LineProtocolTab
}

const lineProtocolTabsStyle = {
  height: '280px',
  width: '750px',
  marginTop: '30px',
}

export class LineProtocolTabs extends PureComponent<Props> {
  public render() {
    return (
      <>
        {this.tabSelector}
        <div style={lineProtocolTabsStyle}>{this.tabBody}</div>
      </>
    )
  }

  private handleTabClick = (tab: LineProtocolTab) => () => {
    const {setActiveLPTab, setLineProtocolText} = this.props
    setLineProtocolText('')
    setActiveLPTab(tab)
  }

  private get tabSelector(): JSX.Element {
    const {tabs, activeLPTab} = this.props
    return (
      <Radio shape={ButtonShape.Default}>
        {tabs.map(t => (
          <Radio.Button
            key={t}
            id={t}
            titleText={t}
            value={t}
            active={activeLPTab === t}
            onClick={this.handleTabClick(t)}
          >
            {t}
          </Radio.Button>
        ))}
      </Radio>
    )
  }

  private get tabBody(): JSX.Element {
    const {setLineProtocolText, lineProtocolText, activeLPTab} = this.props

    if (activeLPTab === LineProtocolTab.UploadFile) {
      return (
        <DragAndDrop
          submitText="Upload File"
          handleSubmit={setLineProtocolText}
        />
      )
    }
    if (activeLPTab === LineProtocolTab.EnterManually) {
      return (
        <TextArea
          value={lineProtocolText}
          placeholder="Write text here"
          handleSubmitText={setLineProtocolText}
        />
      )
    }
    if (activeLPTab === LineProtocolTab.EnterURL) {
      return (
        <Form className="onboarding--admin-user-form">
          <Form.Element label="File URL:">
            <Input
              titleText="File URL:"
              type={InputType.Text}
              placeholder="http://..."
              widthPixels={700}
              value={lineProtocolText}
              onChange={this.handleChange}
              autoFocus={true}
            />
          </Form.Element>
        </Form>
      )
    }
    return
  }
  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {setLineProtocolText} = this.props
    setLineProtocolText(e.target.value)
  }
}

const mstp = ({
  onboarding: {
    dataLoaders: {lineProtocolText, activeLPTab},
  },
}): StateProps => {
  return {lineProtocolText, activeLPTab}
}

const mdtp: DispatchProps = {
  setLineProtocolText,
  setActiveLPTab,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocolTabs)
