import {useState} from 'react'
import classnames from 'classnames'

type PanelState = 'hidden' | 'small' | 'large'
interface ProgressMap {
  [key: PanelState]: PanelState
}

const PANEL_PROGRESS: ProgressMap = {
  hidden: 'small',
  small: 'large',
  large: 'hidden',
}

function usePanelState() {
  const [panelState, setPanelState] = useState<PanelState>('small')

  const className = classnames('notebook-panel', {
    [`notebook-panel__${panelState}`]: panelState,
  })

  const showChildren = panelState === 'small' || panelState === 'large'

  const toggle = (): void => {
    setPanelState(PANEL_PROGRESS[panelState])
  }

  return [className, showChildren, toggle]
}

export default usePanelState
