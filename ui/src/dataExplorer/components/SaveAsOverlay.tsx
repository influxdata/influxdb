import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'
import SaveAsTaskForm from 'src/dataExplorer/components/SaveAsTaskForm'
import SaveAsVariable from 'src/dataExplorer/components/SaveAsVariable'
import {Radio, Overlay} from '@influxdata/clockface'

enum SaveAsOption {
  Dashboard = 'dashboard',
  Task = 'task',
  Variable = 'variable',
}

interface State {
  saveAsOption: SaveAsOption
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & WithRouterProps

class SaveAsOverlay extends PureComponent<Props, State> {
  public state: State = {
    saveAsOption: SaveAsOption.Dashboard,
  }

  render() {
    const {saveAsOption} = this.state
    const {onDismiss} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Header title="Save As" onDismiss={onDismiss} />
          <Overlay.Body>
            <div className="save-as--options">
              <Radio>
                <Radio.Button
                  id="save-as-dashboard"
                  active={saveAsOption === SaveAsOption.Dashboard}
                  value={SaveAsOption.Dashboard}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="cell-radio-button"
                  titleText="Save query as a dashboard cell"
                >
                  Dashboard Cell
                </Radio.Button>
                <Radio.Button
                  id="save-as-task"
                  active={saveAsOption === SaveAsOption.Task}
                  value={SaveAsOption.Task}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="task--radio-button"
                  titleText="Save query as a task"
                >
                  Task
                </Radio.Button>
                <Radio.Button
                  id="save-as-variable"
                  active={saveAsOption === SaveAsOption.Variable}
                  value={SaveAsOption.Variable}
                  onClick={this.handleSetSaveAsOption}
                  data-testid="variable-radio-button"
                  titleText="Save query as a variable"
                >
                  Variable
                </Radio.Button>
              </Radio>
            </div>
            {this.saveAsForm}
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get saveAsForm(): JSX.Element {
    const {saveAsOption} = this.state
    const {onDismiss} = this.props

    if (saveAsOption === SaveAsOption.Dashboard) {
      return <SaveAsCellForm dismiss={onDismiss} />
    } else if (saveAsOption === SaveAsOption.Task) {
      return <SaveAsTaskForm dismiss={onDismiss} />
    } else if (saveAsOption === SaveAsOption.Variable) {
      return <SaveAsVariable onHideOverlay={onDismiss} />
    }
  }

  private handleSetSaveAsOption = (saveAsOption: SaveAsOption) => {
    this.setState({saveAsOption})
  }
}

export default withRouter<OwnProps, {}>(SaveAsOverlay)
