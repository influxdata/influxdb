// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'

// Constants
import {CLOUD} from 'src/shared/constants'

// Actions
import {getOrgSettings as getOrgSettingsAction} from 'src/cloud/actions/orgsettings'

interface DispatchProps {
  getOrgSettings: typeof getOrgSettingsAction
}

class OrgSettings extends PureComponent<DispatchProps> {
  public componentDidMount() {
    if (CLOUD) {
      this.props.getOrgSettings()
    }
  }

  public render() {
    return this.props.children
  }
}

const mdtp: DispatchProps = {
  getOrgSettings: getOrgSettingsAction,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(OrgSettings)
