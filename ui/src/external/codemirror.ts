import {
  modeFlux,
  modeTickscript,
  modeInfluxQL,
  modeMarkdown,
} from 'src/shared/constants/codeMirrorModes'
import 'codemirror/addon/hint/show-hint'

/* eslint-disable */

const CodeMirror = require('codemirror')

CodeMirror.defineOption('placeholder', '', function(cm, val, old) {
  const prev = old && old != CodeMirror.Init
  if (val && !prev) {
    cm.on('blur', onBlur)
    cm.on('change', onChange)
    cm.on('swapDoc', onChange)
    onChange(cm)
  } else if (!val && prev) {
    cm.off('blur', onBlur)
    cm.off('change', onChange)
    cm.off('swapDoc', onChange)
    clearPlaceholder(cm)
    const wrapper = cm.getWrapperElement()
    wrapper.className = wrapper.className.replace(' CodeMirror-empty', '')
  }

  if (val && !cm.hasFocus()) onBlur(cm)
})

CodeMirror.defineSimpleMode = function(name, states) {
  CodeMirror.defineMode(name, function(config) {
    return CodeMirror.simpleMode(config, states)
  })
}

CodeMirror.simpleMode = function(config, states) {
  ensureState(states, 'start')
  const states_ = {},
    meta = states.meta || {}
  let hasIndentation = false
  for (const state in states) {
    if (state !== meta && states.hasOwnProperty(state)) {
      const list = (states_[state] = []),
        orig = states[state]
      for (let i = 0; i < orig.length; i++) {
        const data = orig[i]
        list.push(new Rule(data, states))
        if (data.indent || data.dedent) {
          hasIndentation = true
        }
      }
    }
  }
  const mode = {
    startState() {
      return {
        state: 'start',
        pending: null,
        local: null,
        localState: null,
        indent: hasIndentation ? [] : null,
      }
    },
    copyState(state) {
      const s = {
        state: state.state,
        pending: state.pending,
        local: state.local,
        localState: null,
        indent: state.indent && state.indent.slice(0),
        stack: null,
        persistentStates: null,
      }
      if (state.localState) {
        s.localState = CodeMirror.copyState(state.local.mode, state.localState)
      }
      if (state.stack) {
        s.stack = state.stack.slice(0)
      }
      for (let pers = state.persistentStates; pers; pers = pers.next) {
        s.persistentStates = {
          mode: pers.mode,
          spec: pers.spec,
          state:
            pers.state === state.localState
              ? s.localState
              : CodeMirror.copyState(pers.mode, pers.state),
          next: s.persistentStates,
        }
      }
      return s
    },
    token: tokenFunction(states_, config),
    innerMode(state) {
      return (
        state.local && {
          mode: state.local.mode,
          state: state.localState,
        }
      )
    },
    indent: indentFunction(states_, meta),
  }
  if (meta) {
    for (const prop in meta) {
      if (meta.hasOwnProperty(prop)) {
        mode[prop] = meta[prop]
      }
    }
  }
  return mode
}

function ensureState(states, name) {
  if (!states.hasOwnProperty(name)) {
    throw new Error(`Undefined state ${name} in simple mode`)
  }
}

function toRegex(val, caret) {
  if (!val) {
    return /(?:)/
  }
  let flags = ''
  if (val instanceof RegExp) {
    if (val.ignoreCase) {
      flags = 'i'
    }
    val = val.source
  } else {
    val = String(val)
  }
  return new RegExp(`${caret === false ? '' : '^'}(?:${val})`, flags)
}

function asToken(val) {
  if (!val) {
    return null
  }
  if (val.apply) {
    return val
  }
  if (typeof val === 'string') {
    return val.replace(/\./g, ' ')
  }
  const result = []
  for (let i = 0; i < val.length; i++) {
    result.push(val[i] && val[i].replace(/\./g, ' '))
  }
  return result
}

function Rule(data, states) {
  if (data.next || data.push) {
    ensureState(states, data.next || data.push)
  }
  this.regex = toRegex(data.regex, null)
  this.token = asToken(data.token)
  this.data = data
}

function tokenFunction(states, config) {
  return function(stream, state) {
    if (state.pending) {
      const pend = state.pending.shift()
      if (state.pending.length === 0) {
        state.pending = null
      }
      stream.pos += pend.text.length
      return pend.token
    }

    if (state.local) {
      let tok, m
      if (state.local.end && stream.match(state.local.end)) {
        tok = state.local.endToken || null
        state.local = state.localState = null
        return tok
      }

      tok = state.local.mode.token(stream, state.localState)
      if (
        state.local.endScan &&
        (m = state.local.endScan.exec(stream.current()))
      ) {
        stream.pos = stream.start + m.index
      }
      return tok
    }

    const curState = states[state.state]
    for (let i = 0; i < curState.length; i++) {
      const rule = curState[i]
      const matches =
        (!rule.data.sol || stream.sol()) && stream.match(rule.regex)
      if (matches) {
        if (rule.data.next) {
          state.state = rule.data.next
        } else if (rule.data.push) {
          ;(state.stack || (state.stack = [])).push(state.state)
          state.state = rule.data.push
        } else if (rule.data.pop && state.stack && state.stack.length) {
          state.state = state.stack.pop()
        }

        if (rule.data.mode) {
          enterLocalMode(config, state, rule.data.mode, rule.token)
        }
        if (rule.data.indent) {
          state.indent.push(stream.indentation() + config.indentUnit)
        }
        if (rule.data.dedent) {
          state.indent.pop()
        }
        let token = rule.token
        if (token && token.apply) {
          token = token(matches)
        }
        if (matches.length > 2) {
          state.pending = []
          for (let j = 2; j < matches.length; j++) {
            if (matches[j]) {
              state.pending.push({
                text: matches[j],
                token: rule.token[j - 1],
              })
            }
          }
          stream.backUp(
            matches[0].length - (matches[1] ? matches[1].length : 0)
          )
          return token[0]
        } else if (token && token.join) {
          return token[0]
        }
        return token
      }
    }
    stream.next()
    return null
  }
}

function cmp(a, b) {
  if (a === b) {
    return true
  }
  if (!a || typeof a !== 'object' || !b || typeof b !== 'object') {
    return false
  }
  let props = 0
  for (const prop in a) {
    if (a.hasOwnProperty(prop)) {
      if (!b.hasOwnProperty(prop) || !cmp(a[prop], b[prop])) {
        return false
      }
      props += 1
    }
  }
  for (const prop in b) {
    if (b.hasOwnProperty(prop)) {
      props -= 1
    }
  }
  return props === 0
}

function enterLocalMode(config, state, spec, token) {
  let pers
  if (spec.persistent) {
    for (let p = state.persistentStates; p && !pers; p = p.next) {
      if (spec.spec ? cmp(spec.spec, p.spec) : spec.mode === p.mode) {
        pers = p
      }
    }
  }
  const mode = pers
    ? pers.mode
    : spec.mode || CodeMirror.getMode(config, spec.spec)
  const lState = pers ? pers.state : CodeMirror.startState(mode)
  if (spec.persistent && !pers) {
    state.persistentStates = {
      mode,
      spec: spec.spec,
      state: lState,
      next: state.persistentStates,
    }
  }

  state.localState = lState
  state.local = {
    mode,
    end: spec.end && toRegex(spec.end, null),
    endScan: spec.end && spec.forceEnd !== false && toRegex(spec.end, false),
    endToken: token && token.join ? token[token.length - 1] : token,
  }
}

function indexOf(val, arr) {
  for (let i = 0; i < arr.length; i++) {
    if (arr[i] === val) {
      return true
    }
  }
}

function indentFunction(states, meta) {
  return function(state, textAfter, line) {
    if (state.local && state.local.mode.indent) {
      return state.local.mode.indent(state.localState, textAfter, line)
    }
    if (
      state.indent === null ||
      state.local ||
      (meta.dontIndentStates && indexOf(state.state, meta.dontIndentStates))
    ) {
      return CodeMirror.Pass
    }

    let pos = state.indent.length - 1,
      rules = states[state.state]
    scan: for (;;) {
      for (let i = 0; i < rules.length; i++) {
        const rule = rules[i]
        if (rule.data.dedent && rule.data.dedentIfLineStart !== false) {
          const m = rule.regex.exec(textAfter)
          if (m && m[0]) {
            pos -= 1
            if (rule.next || rule.push) {
              rules = states[rule.next || rule.push]
            }
            textAfter = textAfter.slice(m[0].length)
            continue scan
          }
        }
      }
      break
    }
    return pos < 0 ? 0 : state.indent[pos]
  }
}

function clearPlaceholder(cm) {
  if (cm.state.placeholder) {
    cm.state.placeholder.parentNode.removeChild(cm.state.placeholder)
    cm.state.placeholder = null
  }
}
function setPlaceholder(cm) {
  clearPlaceholder(cm)
  const elt = (cm.state.placeholder = document.createElement('pre'))
  elt.style.cssText = 'height: 0; overflow: visible'
  elt.style.direction = cm.getOption('direction')
  elt.className = 'CodeMirror-placeholder'
  let placeHolder = cm.getOption('placeholder')
  if (typeof placeHolder == 'string')
    placeHolder = document.createTextNode(placeHolder)
  elt.appendChild(placeHolder)
  cm.display.lineSpace.insertBefore(elt, cm.display.lineSpace.firstChild)
}

function onBlur(cm) {
  if (isEmpty(cm)) setPlaceholder(cm)
}
function onChange(cm) {
  const wrapper = cm.getWrapperElement(),
    empty = isEmpty(cm)
  wrapper.className =
    wrapper.className.replace(' CodeMirror-empty', '') +
    (empty ? ' CodeMirror-empty' : '')

  if (empty) setPlaceholder(cm)
  else clearPlaceholder(cm)
}

function isEmpty(cm) {
  return cm.lineCount() === 1 && cm.getLine(0) === ''
}

/* eslint-enable */

// Modes
CodeMirror.defineSimpleMode('flux', modeFlux)
CodeMirror.defineSimpleMode('tickscript', modeTickscript)
CodeMirror.defineSimpleMode('influxQL', modeInfluxQL)
CodeMirror.defineSimpleMode('markdown', modeMarkdown)
