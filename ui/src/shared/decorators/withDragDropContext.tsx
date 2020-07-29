import * as React from 'react'
import HTML5Backend from 'react-dnd-html5-backend'
import {DndProvider} from 'react-dnd'

export default function<TProps extends {}>(
  Component: React.ComponentClass<TProps> | React.StatelessComponent<TProps>
) {
  return (props: TProps) => (
    <DndProvider backend={HTML5Backend}>
      <Component {...props} />
    </DndProvider>
  )
}
