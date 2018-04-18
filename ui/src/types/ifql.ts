// function definitions
export type OnDeleteFuncNode = (funcID: string, expressionID: string) => void
export type OnChangeArg = (inputArg: InputArg) => void
export interface InputArg {
  funcID: string
  expressionID: string
  key: string
  value: string | boolean
  generate?: boolean
}
