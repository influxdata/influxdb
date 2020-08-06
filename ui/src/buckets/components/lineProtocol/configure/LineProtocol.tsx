// Libraries
import React, {FC, useContext} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Form,
  Overlay,
  ComponentColor,
  Button,
  ButtonType,
  ComponentStatus,
} from '@influxdata/clockface'
import LineProtocolTabs from 'src/buckets/components/lineProtocol/configure/LineProtocolTabs'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import LineProtocolHelperText from 'src/buckets/components/lineProtocol/LineProtocolHelperText'
import {Context} from 'src/buckets/components/lineProtocol/LineProtocolWizard'

// Actions
import {writeLineProtocolAction} from 'src/buckets/components/lineProtocol/LineProtocol.thunks'

// Types
import {AppState, LineProtocolTab} from 'src/types/index'

// Selectors
import {getOrg} from 'src/organizations/selectors'

type OwnProps = {bucket: string}
type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

const tabs: LineProtocolTab[] = ['Upload File', 'Enter Manually']

const LineProtocol: FC<Props> = ({bucket, org}) => {
  const [{body, precision}] = useContext(Context)

  const handleSubmit = () => () => {
    writeLineProtocolAction(org, bucket, body, precision)
  }

  return (
    <Form onSubmit={handleSubmit}>
      <Overlay.Body style={{textAlign: 'center'}}>
        <LineProtocolTabs tabs={tabs} />
        <LineProtocolHelperText />
      </Overlay.Body>
      <Overlay.Footer>
        <Button
          text="Write Data"
          color={ComponentColor.Primary}
          type={ButtonType.Submit}
          status={ComponentStatus.Disabled}
          testID="write-data--button"
        />
      </Overlay.Footer>
    </Form>
  )
}

const mstp = (state: AppState) => {
  const org = getOrg(state).name

  return {org}
}

const mdtp = {
  writeLineProtocolAction,
}

const connector = connect(mstp, mdtp)

export default connector(LineProtocol)
