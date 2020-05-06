import React, {FC, createElement, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/notebook.context'

const PIPES = {}

export interface TypeRegistration {
  type: string // a unique string that identifies a pipe
  component: JSX.Element // the view component for rendering the interface
  button: string // a human readable string for appending the type
  title: string // the header of the NotebookPanel
  empty: any // the default state for an add
}

export function register(definition: TypeRegistration) {
  if (PIPES.hasOwnProperty(definition.type)) {
    throw new Exception(
      `Pipe of type [${definition.type}] has already been registered`
    )
  }

  PIPES[definition.type] = {
    ...definition,
  }
}

export const AddMorePipes: FC = () => {
  const {addPipe} = useContext(NotebookContext)

  const pipes = Object.entries(PIPES).map(([type, def]) => {
    return (
      <div
        className="ugh-button"
        key={def.type}
        onClick={() => {
          addPipe({
            ...def.empty,
            type,
          })
        }}
      >
        {def.button}
      </div>
    )
  })

  return <>{pipes}</>
}

interface PipeProps {
  idx: number
}

const NotebookPipe: FC<PipeProps> = ({idx}) => {
  const {pipes} = useContext(NotebookContext)

  if (!PIPES.hasOwnProperty(pipes[idx].type)) {
    throw new Error(`NotebookPipe type [${type}] not registered`)
    return null
  }

  return createElement(PIPES[pipes[idx].type].component, {idx})
}

export default NotebookPipe

// NOTE: this loads in all the modules under the current directory
// to make it easier to add new types
const context = require.context('./pipes', true, /index\.(ts|tsx)$/)
context.keys().forEach(key => {
  context(key)
})
