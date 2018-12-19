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
import PrecisionDropdown from 'src/onboarding/components/configureStep/lineProtocol/PrecisionDropdown'

// Types
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

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
    const {setPrecision, precision} = this.props
    return (
      <>
        {this.tabSelector}
        <div className={'wizard-step--lp-body'}>{this.tabBody}</div>
        <PrecisionDropdown setPrecision={setPrecision} precision={precision} />
        <div className="wizard--button-bar">{this.submitButton}</div>
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
    const {lineProtocolBody} = this.props
    if (lineProtocolBody) {
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
    const {setActiveLPTab, setLineProtocolBody, activeLPTab} = this.props
    if (tab !== activeLPTab) {
      setLineProtocolBody('')
      setActiveLPTab(tab)
    }
  }

  private get tabBody(): JSX.Element {
    const {setLineProtocolBody, lineProtocolBody, activeLPTab} = this.props
    const {urlInput} = this.state
    switch (activeLPTab) {
      case LineProtocolTab.UploadFile:
        return (
          <DragAndDrop
            submitText="Upload File"
            handleSubmit={setLineProtocolBody}
            submitOnDrop={true}
            submitOnUpload={true}
          />
        )
      case LineProtocolTab.EnterManually:
        return (
          <TextArea
            value={lineProtocolBody}
            placeholder="Write text here"
            handleSubmitText={setLineProtocolBody}
          />
        )
      case LineProtocolTab.EnterURL:
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
  }

  private handleURLChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({urlInput: e.target.value})
  }

  private handleSubmitLineProtocol = async (): Promise<void> => {
    const {
      bucket,
      org,
      writeLineProtocolAction,
      lineProtocolBody,
      precision,
    } = this.props
    writeLineProtocolAction(org, bucket, lineProtocolBody, precision)
  }
}

const mstp = ({
  onboarding: {
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
