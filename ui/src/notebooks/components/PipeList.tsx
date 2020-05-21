import React, {
  FC,
  useContext,
  useEffect,
  memo,
  useCallback,
  createElement,
} from 'react'
import {PipeContextProps, PipeData} from 'src/notebooks'
import Pipe from 'src/notebooks/components/Pipe'
import {NotebookContext} from 'src/notebooks/context/notebook'
import NotebookPanel from 'src/notebooks/components/panel/NotebookPanel'

const lookup = {}

const NotebookPipe: FC = memo(({index, data, onUpdate}) => {
  if (index in lookup) {
    console.log(
      'rerender',
      lookup[index].data === data,
      lookup[index].onUpdate === onUpdate
    )
    console.log('\t', data, lookup[index].data)
  }
  lookup[index] = {
    data,
    onUpdate,
  }
  const panel: FC<PipeContextProps> = props => {
    const _props = {
      ...props,
      index,
    }

    return createElement(NotebookPanel, _props)
  }

  const _onUpdate = (data: PipeData) => {
    onUpdate(index, data)
  }

  return <Pipe data={data} onUpdate={_onUpdate} Context={panel} />
})

const PipeList: FC = () => {
  const {id, pipes, updatePipe} = useContext(NotebookContext)
  const update = useCallback(updatePipe, [id])

  const _pipes = pipes.map((_, index) => {
    return (
      <NotebookPipe
        key={`pipe-${id}-${index}`}
        index={index}
        data={pipes[index]}
        onUpdate={update}
      />
    )
  })

  return <>{_pipes}</>
}

export default PipeList
