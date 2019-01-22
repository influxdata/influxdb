// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {AppState} from 'src/types/v2/index'
import {Form, Input, InputType, ComponentSize} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import OnboardingButtons from '../../OnboardingButtons'
import {setTelegrafConfigName} from 'src/onboarding/actions/dataLoaders'

interface OwnProps {
  onClickNext: () => void
  onClickPrevious: () => void
}

interface DispatchProps {
  onSetTelegrafConfigName: typeof setTelegrafConfigName
}

interface StateProps {
  telegrafConfigName: string
}

type Props = OwnProps & DispatchProps & StateProps

class TelegrafPluginInstructions extends PureComponent<Props> {
  public render() {
    const {onClickPrevious, onClickNext, telegrafConfigName} = this.props
    return (
      <Form onSubmit={onClickNext}>
        <div className="wizard-step--scroll-area">
          <FancyScrollbar autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">
                Telegraf Configuration Information
              </h3>
              <h5 className="wizard-step--sub-title">
                Telegraf is a plugin based data collection agent. Click on the
                plugin names to the left in order to configure the selected
                plugins. For more information about Telegraf Plugins, see
                documentation.
              </h5>
              <Form.Element label="Telegraf Configuration Name">
                <Input
                  type={InputType.Text}
                  value={telegrafConfigName}
                  onChange={this.handleNameInput}
                  titleText="Telegraf Configuration Name"
                  size={ComponentSize.Medium}
                  autoFocus={true}
                />
              </Form.Element>
            </div>
          </FancyScrollbar>
        </div>
        <OnboardingButtons onClickBack={onClickPrevious} />
      </Form>
    )
  }
  private handleNameInput = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onSetTelegrafConfigName(e.target.value)
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafConfigName},
  },
}: AppState): StateProps => {
  return {
    telegrafConfigName,
  }
}

const mdtp: DispatchProps = {
  onSetTelegrafConfigName: setTelegrafConfigName,
}
export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafPluginInstructions)
