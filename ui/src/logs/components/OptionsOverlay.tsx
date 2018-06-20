import React, {Component} from 'react'
import _ from 'lodash'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'
import {Color} from 'src/logs/components/ColorDropdown'
import SeverityConfig from 'src/logs/components/SeverityConfig'

import {DEFAULT_SEVERITY_LEVELS} from 'src/logs/constants'

interface SeverityItem {
  severity: string
  default: Color
  override?: Color
}

interface Props {
  severityConfigs: SeverityItem[]
  onUpdateConfigs: (updatedConfigs: SeverityItem[]) => void
  onDismissOverlay: () => void
}

interface State {
  workingSeverityConfigs: SeverityItem[]
}

class OptionsOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      workingSeverityConfigs: this.props.severityConfigs,
    }
  }

  public render() {
    const {workingSeverityConfigs} = this.state

    return (
      <Container maxWidth={700}>
        <Heading title="Configure Log Viewer">
          {this.overlayActionButtons}
        </Heading>
        <Body>
          <div className="row">
            <div className="col-sm-6">
              <SeverityConfig
                configs={workingSeverityConfigs}
                onReset={this.handleResetSeverityColors}
                onChangeColor={this.handleChangeSeverityColor}
              />
            </div>
            <div className="col-sm-6">
              <label className="form-label">Order Table Columns</label>
              <p>Column re-ordering goes here</p>
            </div>
          </div>
        </Body>
      </Container>
    )
  }

  private get overlayActionButtons(): JSX.Element {
    const {onDismissOverlay} = this.props

    return (
      <div className="btn-group--right">
        <button className="btn btn-sm btn-default" onClick={onDismissOverlay}>
          Cancel
        </button>
        <button
          className="btn btn-sm btn-success"
          onClick={this.handleSave}
          disabled={this.isSaveDisabled}
        >
          Save
        </button>
      </div>
    )
  }

  private get isSaveDisabled(): boolean {
    const {workingSeverityConfigs} = this.state
    const {severityConfigs} = this.props

    if (_.isEqual(workingSeverityConfigs, severityConfigs)) {
      return true
    }

    return false
  }

  private handleSave = () => {
    const {onUpdateConfigs, onDismissOverlay} = this.props
    const {workingSeverityConfigs} = this.state

    onUpdateConfigs(workingSeverityConfigs)
    onDismissOverlay()
  }

  private handleResetSeverityColors = (): void => {
    this.setState({workingSeverityConfigs: DEFAULT_SEVERITY_LEVELS})
  }

  private handleChangeSeverityColor = (severity: string) => (
    override: Color
  ): void => {
    const workingSeverityConfigs = this.state.workingSeverityConfigs.map(
      config => {
        if (config.severity === severity) {
          return {...config, override}
        }

        return config
      }
    )

    this.setState({workingSeverityConfigs})
  }
}

export default OptionsOverlay
