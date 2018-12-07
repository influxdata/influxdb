// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Input,
  InputType,
  Radio,
  ButtonShape,
  Form,
  Button,
  ComponentSize,
  ComponentColor,
} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import TextArea from 'src/clockface/components/inputs/TextArea'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

// Actions
import {
  setLineProtocolText,
  setActiveLPTab,
  writeLineProtocolAction,
} from 'src/onboarding/actions/dataLoaders'

import {AppState} from 'src/types/v2/index'

// Styles
import 'src/clockface/components/auto_input/AutoInput.scss'

interface OwnProps {
  tabs: LineProtocolTab[]
  bucket: string
  org: string
}

type Props = OwnProps & DispatchProps & StateProps

interface DispatchProps {
  setLineProtocolText: typeof setLineProtocolText
  setActiveLPTab: typeof setActiveLPTab
  writeLineProtocolAction: typeof writeLineProtocolAction
}

interface StateProps {
  lineProtocolText: string
  activeLPTab: LineProtocolTab
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
    return (
      <>
        {this.tabSelector}
        <div className={'wizard-step--lp-body'}>{this.tabBody}</div>
        <div className="wizard-button-bar">{this.submitButton}</div>
      </>
    )
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

  private get submitButton(): JSX.Element {
    const {lineProtocolText} = this.props
    if (lineProtocolText) {
      return (
        <Button
          size={ComponentSize.Medium}
          color={ComponentColor.Primary}
          text={'submit line protocol'}
          onClick={this.handleSubmitLineProtocol}
        />
      )
    }
    return null
  }

  private handleTabClick = (tab: LineProtocolTab) => () => {
    const {setActiveLPTab, setLineProtocolText} = this.props
    setLineProtocolText('')
    setActiveLPTab(tab)
  }

  private get tabBody(): JSX.Element {
    const {setLineProtocolText, lineProtocolText, activeLPTab} = this.props
    const {urlInput} = this.state

    if (activeLPTab === LineProtocolTab.UploadFile) {
      return (
        <DragAndDrop
          submitText="Upload File"
          handleSubmit={setLineProtocolText}
          submitOnDrop={true}
          submitOnUpload={true}
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
              value={urlInput}
              onChange={this.handleURLChange}
              autoFocus={true}
            />
          </Form.Element>
        </Form>
      )
    }
    return
  }

  private handleURLChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({urlInput: e.target.value})
  }

  private handleSubmitLineProtocol = async (): Promise<void> => {
    const {bucket, org, writeLineProtocolAction, lineProtocolText} = this.props
    writeLineProtocolAction(org, bucket, lineProtocolText)
  }
}

const mstp = ({
  onboarding: {
    dataLoaders: {lineProtocolText, activeLPTab},
  },
}: AppState) => {
  return {lineProtocolText, activeLPTab}
}

const mdtp: DispatchProps = {
  setLineProtocolText,
  setActiveLPTab,
  writeLineProtocolAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(LineProtocolTabs)
