// Libraries
import React, {PureComponent} from 'react'

// Components
import SaveAsCellForm from 'src/dataExplorer/components/SaveAsCellForm'
import {
  Button,
  Radio,
  OverlayTechnology,
  OverlayBody,
  OverlayHeading,
  OverlayContainer,
} from 'src/clockface'

// Styles
import 'src/dataExplorer/components/SaveAsButton.scss'

enum SaveAsOption {
  Dashboard = 'dashboard',
  Task = 'task',
}

interface Props {}

interface State {
  isOverlayVisible: boolean
  saveAsOption: SaveAsOption
}

class SaveAsButton extends PureComponent<Props, State> {
  public state: State = {
    isOverlayVisible: false,
    saveAsOption: SaveAsOption.Dashboard,
  }

  public render() {
    const {isOverlayVisible, saveAsOption} = this.state

    return (
      <>
        <Button text="Save As..." onClick={this.handleShowOverlay} />
        <OverlayTechnology visible={isOverlayVisible}>
          <OverlayContainer>
            <OverlayHeading title="Save As">
              <div className="save-as--close-overlay">
                <span
                  className="icon remove"
                  onClick={this.handleHideOverlay}
                />
              </div>
            </OverlayHeading>
            <OverlayBody>
              <div className="save-as--options">
                <Radio>
                  <Radio.Button
                    active={saveAsOption === SaveAsOption.Dashboard}
                    value={SaveAsOption.Dashboard}
                    onClick={this.handleSetSaveAsOption}
                  >
                    Dashboard Cell
                  </Radio.Button>
                  <Radio.Button
                    active={saveAsOption === SaveAsOption.Task}
                    value={SaveAsOption.Task}
                    onClick={this.handleSetSaveAsOption}
                    disabled={true}
                  >
                    Task
                  </Radio.Button>
                </Radio>
              </div>
              {saveAsOption === SaveAsOption.Dashboard && <SaveAsCellForm />}
            </OverlayBody>
          </OverlayContainer>
        </OverlayTechnology>
      </>
    )
  }

  private handleShowOverlay = () => {
    this.setState({isOverlayVisible: true})
  }

  private handleHideOverlay = () => {
    this.setState({isOverlayVisible: false})
  }

  private handleSetSaveAsOption = (saveAsOption: SaveAsOption) => {
    this.setState({saveAsOption})
  }
}

export default SaveAsButton
