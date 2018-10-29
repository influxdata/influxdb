import {Editor, Position} from 'codemirror'

export interface Hints {
  from: Position
  to: Position
  list: Array<Hint | string>
}

// Interface used by showHint.js Codemirror add-on
// When completions aren't simple strings, they should be objects with the following properties:

export interface Hint {
  text: string
  className?: string
  displayText?: string
  from?: Position
  /** Called if a completion is picked. If provided *you* are responsible for applying the completion */
  hint?: (cm: Editor, data: Hints, cur: Hint) => void
  render?: (element: HTMLLIElement, data: Hints, cur: Hint) => void
  to?: Position
}

type HintFunction = (cm: Editor) => Hints

interface AsyncHintFunction {
  (cm: Editor, callback: (hints: Hints) => any): any
  async?: boolean
}

export interface ShowHintOptions {
  completeSingle?: boolean
  hint: HintFunction | AsyncHintFunction
}

export type ShowHint = (cm: Editor, options: ShowHintOptions) => void
