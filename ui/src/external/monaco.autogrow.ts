const MIN_HEIGHT = 100

export const registerAutogrow = editor => {
  let prevHeight = 0

  const updateEditorHeight = () => {
    const editorElement = editor.getDomNode()

    if (!editorElement) {
      return
    }

    const lineHeight = editor.getOption(
      window.monaco.editor.EditorOption.lineHeight
    )
    const lineCount = (editor.getModel() || {}).getLineCount() || 1
    const height = editor.getTopForLineNumber(lineCount + 1) + lineHeight

    if (prevHeight !== Math.max(MIN_HEIGHT, height)) {
      prevHeight = Math.max(MIN_HEIGHT, height)
      editorElement.style.height = `${prevHeight}px`
      editor.layout()
    }
  }

  editor.updateOptions({
    minimap: {
      enabled: false,
    },
    scrollbar: {
      vertical: 'hidden',
      handleMouseWheel: false,
    },
    scrollBeyondLastLine: false,
  })

  editor.onDidChangeModelDecorations(() => {
    updateEditorHeight() // typing
    requestAnimationFrame(updateEditorHeight) // folding
  })
}
